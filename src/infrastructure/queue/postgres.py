"""Очередь на Postgres. Реализация ADR-SPEECH-0001.

SQL здесь намеренно написан текстом, а не собран из ORM. Протокол очереди —
предмет отдельного документа, который читают и сверяют с кодом; выражение
`FOR UPDATE SKIP LOCKED`, собранное конструктором запросов, сверить с
документом нельзя, не запустив его.
"""

from __future__ import annotations

import hashlib
import json
import random
import uuid
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Final

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from src.application.dto.calls import (
    JobErrorClass,
    JobKind,
    JobLane,
    JobStatus,
    is_terminal_error,
)
from src.application.ports.queue import (
    ClaimedJob,
    ExpiredJob,
    LaneDepth,
    NewJob,
    QueueSnapshot,
    ReprocessingCallNotFound,
    ReprocessingConflict,
)

#: Пространство ключей advisory-блокировок этого сервиса. Первое число
#: двухаргументной формы `pg_try_advisory_lock` — namespace: без него слот
#: расшифровки мог бы совпасть с чужой блокировкой в той же базе.
ADVISORY_NAMESPACE: Final[int] = 0x5350  # 'SP'

#: Задержки повтора: (база, потолок) в секундах, по классу отказа и этапу.
#:
#: У `ingest` с `not_found` окно шире прочих: запись может ещё не появиться у
#: источника. Оно стало короче после решения о суточном пакете — сбор идёт
#: вечером, когда записи за сутки давно готовы, поэтому отсутствие записи
#: означает скорее её отсутствие, чем незавершённость.
_BACKOFF_INGEST_NOT_FOUND: Final[tuple[int, int]] = (60, 600)
_BACKOFF_DEFAULT: Final[tuple[int, int]] = (30, 900)


def backoff_seconds(kind: JobKind, error_class: JobErrorClass, attempts: int) -> float:
    """Через сколько повторять. Экспонента с джиттером и потолком.

    Джиттер обязателен, а не украшение: без него полторы тысячи заданий,
    поставленных одним суточным прогоном и отказавших на недоступном Whisper,
    повторятся одновременно — и добьют его ровно в тот момент, когда он
    поднимается.
    """
    if kind is JobKind.INGEST and error_class is JobErrorClass.NOT_FOUND:
        base, cap = _BACKOFF_INGEST_NOT_FOUND
    else:
        base, cap = _BACKOFF_DEFAULT
    raw = base * (2 ** max(0, attempts - 1))
    bounded = min(float(raw), float(cap))
    return bounded * random.uniform(0.8, 1.2)  # noqa: S311 - джиттер, не криптография


_ENQUEUE_SQL = text(
    """
    INSERT INTO speech_jobs (
        id, call_id, kind, lane, status, priority, attempts, max_attempts,
        run_after, lease_seconds, payload, created_at
    )
    VALUES (
        gen_random_uuid(), :call_id, :kind, CAST(:lane AS text), 'queued',
        -- Приведение стоит на **обоих** вхождениях параметра, и это не
        -- избыточность. Один и тот же `:lane` попадает и в колонку
        -- `varchar(16)`, и в это сравнение с текстовым литералом; приведя
        -- только одно, Postgres по-прежнему выводит для параметра два разных
        -- типа и отказывается готовить запрос
        -- («inconsistent types deduced for parameter»).
        --
        -- Приоритет по умолчанию вычисляется здесь, а не в Python, чтобы полоса
        -- и приоритет не расходились случайно: они пишутся одной операцией.
        -- Явный приоритет ставит приём — см. `NewJob.priority`.
        COALESCE(
            CAST(:priority AS integer),
            CASE CAST(:lane AS text) WHEN 'daily' THEN 0 ELSE 100 END
        ),
        0, :max_attempts,
        COALESCE(:run_after, now()), :lease_seconds, CAST(:payload AS jsonb), now()
    )
    ON CONFLICT (call_id, kind) DO NOTHING
    RETURNING id
    """
)

#: Сколько запрос готов ждать блокировку строки звонка.
#:
#: Без потолка `FOR UPDATE` ждёт столько, сколько держит другой, и «переобработать»
#: в интерфейсе просто висит — без ответа, без ошибки, без срока.
#:
#: Три секунды: воркер держит строку звонка коротко (долгая фаза этапа идёт
#: **вне** транзакции), и перекрытие измеряется миллисекундами. Секунды хватает
#: с запасом, а больше трёх человек уже считает зависанием и жмёт второй раз.
CALL_LOCK_TIMEOUT_MS: Final[int] = 3000

#: `SET LOCAL` — на текущую транзакцию, и откатывать его не нужно: он кончится
#: вместе с ней. Значение подставляется форматированием, а не параметром,
#: потому что `SET` параметров не принимает; оно наше и целочисленное, чужого
#: здесь не бывает.
_SET_CALL_LOCK_TIMEOUT_SQL = text(f"SET LOCAL lock_timeout = {CALL_LOCK_TIMEOUT_MS}")

_LOCK_CALL_SQL = text("SELECT id FROM speech_calls WHERE id = :call_id FOR UPDATE")

# Workers may already hold a job/result lock when they update the call. Do not
# wait on their jobs while holding the call lock: that would invert lock order.
_LOCK_CALL_JOBS_SQL = text(
    "SELECT id, kind, status FROM speech_jobs WHERE call_id = :call_id "
    "ORDER BY id FOR UPDATE NOWAIT"
)

_DELETE_DOWNSTREAM_SQL = text(
    "DELETE FROM speech_jobs WHERE call_id = :call_id "
    "AND kind = ANY(CAST(:kinds AS text[])) AND status IN ('done', 'failed', 'dead')"
)

_RESET_JOB_SQL = text(
    """
    UPDATE speech_jobs
       SET status = 'queued', lane = CAST(:lane AS text),
           priority = CASE CAST(:lane AS text) WHEN 'daily' THEN 0 ELSE 100 END,
           attempts = 0, max_attempts = :max_attempts,
           run_after = COALESCE(:run_after, now()), lease_seconds = :lease_seconds,
           payload = CAST(:payload AS jsonb),
           locked_by = NULL, locked_at = NULL, started_at = NULL, finished_at = NULL,
           last_error_class = NULL, last_error_at = NULL
     WHERE id = :job_id AND status IN ('done', 'failed', 'dead')
    RETURNING id
    """
)

_CLAIM_SQL = text(
    """
    UPDATE speech_jobs
       SET status = 'running',
           locked_by = :worker_id,
           locked_at = now(),
           started_at = COALESCE(started_at, now()),
           attempts = attempts + 1
     WHERE id = (
           SELECT id
             FROM speech_jobs
            WHERE status = 'queued'
              AND run_after <= now()
              AND lane = ANY(CAST(:lanes AS text[]))
              AND kind = ANY(CAST(:kinds AS text[]))
            ORDER BY priority, created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
     )
    RETURNING id, call_id, kind, lane, attempts, max_attempts, payload, lease_seconds,
              priority
    """
)

_HEARTBEAT_SQL = text(
    """
    UPDATE speech_jobs
       SET locked_at = now()
     WHERE id = :job_id AND locked_by = :worker_id AND status = 'running'
    """
)

_COMPLETE_SQL = text(
    """
    UPDATE speech_jobs
       SET status = 'done', finished_at = now(), locked_by = NULL, locked_at = NULL
     WHERE id = :job_id AND locked_by = :worker_id AND status = 'running'
    """
)

_FAIL_SQL = text(
    """
    UPDATE speech_jobs
       SET status = CASE
               WHEN :terminal THEN 'dead'
               WHEN attempts >= max_attempts THEN 'dead'
               ELSE 'queued'
           END,
           run_after = now() + make_interval(secs => :backoff),
           locked_by = NULL,
           locked_at = NULL,
           last_error_class = :error_class,
           last_error_at = now(),
           finished_at = CASE
               WHEN :terminal OR attempts >= max_attempts THEN now()
               ELSE NULL
           END
     WHERE id = :job_id AND locked_by = :worker_id
    RETURNING status
    """
)

#: Отложить задание: занятость внешнего ресурса — не отказ. Класс ошибки не
#: пишется намеренно (его отсутствие и есть признак «не отказывало»), а
#: потраченная при захвате попытка возвращается — иначе задание, которому не
#: повезло с очередью, стало бы `dead`, ни разу не дойдя до модели.
#:
#: `GREATEST(attempts - 1, 0)` — на случай, если счётчик уже нулевой: отрицательное
#: число попыток не значит ничего и сломало бы сравнение с `max_attempts`.
_DEFER_SQL = text(
    """
    UPDATE speech_jobs
       SET status = 'queued',
           attempts = GREATEST(attempts - 1, 0),
           run_after = now() + make_interval(secs => :delay),
           locked_by = NULL,
           locked_at = NULL
     WHERE id = :job_id AND locked_by = :worker_id AND status = 'running'
    RETURNING id
    """
)

_REQUEUE_EXPIRED_SQL = text(
    """
    UPDATE speech_jobs
       SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
           locked_by = NULL,
           locked_at = NULL,
           last_error_class = 'lease_expired',
           last_error_at = now(),
           run_after = now(),
           finished_at = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END
     WHERE status = 'running'
       AND locked_at < now() - make_interval(secs => lease_seconds)
    RETURNING id, call_id, kind, status
    """
)

_SNAPSHOT_SQL = text(
    """
    SELECT lane,
           status,
           count(*) AS total,
           min(created_at) FILTER (WHERE status = 'queued') AS oldest_queued
      FROM speech_jobs
     GROUP BY lane, status
    """
)

#: Правило «отказ ещё не решён» — одной строкой SQL, чтобы оба запроса ниже
#: и маршрут администратора говорили об одном множестве.
#:
#: Не `IN ('failed','dead')`, как было: **очередь статус `failed` не пишет
#: никогда**. Отказ, которому остались попытки, возвращается в `queued`
#: (см. `_FAIL_SQL`), и прежний фильтр молча выбрасывал ровно те отказы,
#: которые ждут повтора прямо сейчас. Значение `JobStatus.FAILED` в контракте
#: есть, и это сбивает: оно используется при переобработке, а не очередью.
#:
#: `<> 'done'` покрывает все три живых случая: `queued` — ждёт повтора,
#: `running` — повторяется сейчас, `dead` — попытки исчерпаны.
#:
#: Условие записано в каждом запросе своими словами, а не вынесено в константу
#: и подставлено: подстановка кусков SQL строками читается хуже самого SQL, а
#: два места — не то количество, ради которого стоит заводить механизм.

_ERRORS_SQL = text(
    """
    SELECT last_error_class, count(*) AS total
      FROM speech_jobs
     WHERE last_error_at >= now() - interval '24 hours'
       AND last_error_class IS NOT NULL
       AND status <> 'done'
     GROUP BY last_error_class
    """
)

_FAILED_24H_SQL = text(
    """
-- Только нерешённое.
--
-- `last_error_at` не сбрасывается успехом: `complete` меняет статус, а историю
-- отказа оставляет — и правильно, она нужна. Но задание, отказавшее в 03:00 и
-- прошедшее с повтора в 03:05, при счёте по одному `last_error_at` остаётся
-- «сбоем» ещё сутки. Повторы у нас штатны (провайдер, таймауты), поэтому число
-- было бы завышено всегда, а к постоянно ненулевому счётчику привыкают и
-- перестают смотреть.
--
-- Экран, который это показывает, смотрят во время происшествия: там нужен
-- вопрос «что сломано сейчас», а не «что когда-либо моргнуло». Мигание видно
-- в `/metrics` — `speech_errors_recent_jobs` считает по окну **без** оглядки
-- на статус, и это намеренно разные вопросы.
    SELECT count(*)
      FROM speech_jobs
     WHERE last_error_at >= now() - interval '24 hours'
       AND status <> 'done'
    """
)


class PostgresJobQueue:
    """Очередь заданий в таблице `speech_jobs`."""

    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        *,
        engine: AsyncEngine | None = None,
    ) -> None:
        """Настроить PostgresJobQueue из переданных параметров и зависимостей."""
        self._sessionmaker = sessionmaker
        # Отдельный движок нужен только слотам расшифровки: advisory-блокировка
        # уровня сессии живёт, пока жива её физическая сессия, а её надо
        # удерживать через несколько транзакций подряд.
        self._engine = engine

    async def enqueue(self, session: AsyncSession, job: NewJob) -> uuid.UUID | None:
        """Поставить задание в текущей SQL-транзакции и вернуть его UUID."""
        result = await session.execute(
            _ENQUEUE_SQL,
            {
                "call_id": job.call_id,
                "kind": job.kind.value,
                "lane": job.lane.value,
                "max_attempts": job.max_attempts,
                "run_after": job.run_after,
                "lease_seconds": job.lease_seconds,
                "payload": json.dumps(job.payload),
                "priority": job.priority,
            },
        )
        row = result.first()
        return None if row is None else uuid.UUID(str(row[0]))

    async def reprocess(
        self,
        session: AsyncSession,
        job: NewJob,
        *,
        retry_job_id: uuid.UUID | None = None,
    ) -> uuid.UUID:
        """Prepare one stage; results and audit belong to the caller's transaction."""
        # Потолок ожидания ставится **до** захвата: иначе запрос уже стоит в
        # очереди за чужой блокировкой, и ограничивать его поздно.
        await session.execute(_SET_CALL_LOCK_TIMEOUT_SQL)
        try:
            locked = await session.scalar(_LOCK_CALL_SQL, {"call_id": job.call_id})
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) == "55P03":
                # Истёкшее ожидание и запрещённое ожидание отвечают одинаково, и
                # это правильно: снаружи и то и другое — «конвейер занят».
                raise ReprocessingConflict("pipeline_is_busy") from exc
            raise
        if locked is None:
            raise ReprocessingCallNotFound(str(job.call_id))
        try:
            rows = (
                (await session.execute(_LOCK_CALL_JOBS_SQL, {"call_id": job.call_id}))
                .mappings()
                .all()
            )
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) == "55P03":
                raise ReprocessingConflict("pipeline_is_busy") from exc
            raise
        if any(row["status"] in (JobStatus.QUEUED, JobStatus.RUNNING) for row in rows):
            raise ReprocessingConflict("call_has_queued_or_running_jobs")

        selected = next((row for row in rows if row["kind"] == job.kind.value), None)
        if retry_job_id is not None and (
            selected is None
            or selected["id"] != retry_job_id
            or selected["status"] not in (JobStatus.FAILED, JobStatus.DEAD)
        ):
            raise ReprocessingConflict("job_is_not_failed_or_dead")
        if selected is not None and selected["status"] not in (
            JobStatus.DONE,
            JobStatus.FAILED,
            JobStatus.DEAD,
        ):
            raise ReprocessingConflict("stage_is_not_terminal")

        # JobKind explicitly defines execution order, not alphabetical order.
        stages = list(JobKind)
        downstream = stages[stages.index(job.kind) + 1 :]
        if downstream:
            await session.execute(
                _DELETE_DOWNSTREAM_SQL,
                {"call_id": job.call_id, "kinds": [kind.value for kind in downstream]},
            )
        if selected is None:
            job_id = await self.enqueue(session, job)
        else:
            job_id = await session.scalar(
                _RESET_JOB_SQL,
                {
                    "job_id": selected["id"],
                    "lane": job.lane.value,
                    "max_attempts": job.max_attempts,
                    "run_after": job.run_after,
                    "lease_seconds": job.lease_seconds,
                    "payload": json.dumps(job.payload),
                },
            )
        if job_id is None:
            # Includes a concurrent insert from a producer outside this protocol.
            # The caller must roll back the downstream cleanup as well.
            raise ReprocessingConflict("stage_already_exists")
        return uuid.UUID(str(job_id))

    async def claim(
        self,
        lanes: Sequence[JobLane],
        worker_id: str,
        *,
        kinds: Sequence[JobKind] | None = None,
    ) -> ClaimedJob | None:
        """Захватить подходящее задание с арендой либо вернуть None при пустой очереди."""
        if not lanes:
            # Воркер без полос не «ничего не нашёл», а неправильно запущен.
            raise ValueError("claim() requires at least one lane")
        # Этапы фильтруются наравне с полосами. Воркер, берущий задание, для
        # которого у него нет обработчика, провалит его и будет повторять до
        # исчерпания попыток — снаружи это выглядит как отказ сервиса, а не как
        # неполный набор этапов. Умолчание — все этапы.
        wanted = list(kinds) if kinds else list(JobKind)
        async with self._sessionmaker() as session, session.begin():
            result = await session.execute(
                _CLAIM_SQL,
                {
                    "lanes": [lane.value for lane in lanes],
                    "kinds": [kind.value for kind in wanted],
                    "worker_id": worker_id,
                },
            )
            row = result.mappings().first()
            if row is None:
                return None
            return ClaimedJob(
                id=uuid.UUID(str(row["id"])),
                call_id=uuid.UUID(str(row["call_id"])),
                kind=JobKind(row["kind"]),
                lane=JobLane(row["lane"]),
                attempts=int(row["attempts"]),
                max_attempts=int(row["max_attempts"]),
                payload=dict(row["payload"] or {}),
                lease_seconds=int(row["lease_seconds"]),
                priority=int(row["priority"]),
            )

    async def heartbeat(self, job_id: uuid.UUID, worker_id: str) -> bool:
        """Продлить аренду задания, если оно всё ещё принадлежит указанному воркеру."""
        async with self._sessionmaker() as session, session.begin():
            result = await session.execute(
                _HEARTBEAT_SQL, {"job_id": job_id, "worker_id": worker_id}
            )
            # `Result` не объявляет `rowcount` в типах, но у UPDATE он есть:
            # это единственный способ отличить «продлили аренду» от «задание
            # уже отобрал другой воркер».
            updated: int = result.rowcount  # type: ignore[attr-defined]
            return updated > 0

    async def complete(self, session: AsyncSession, job_id: uuid.UUID, worker_id: str) -> bool:
        """Завершить задание текущего владельца аренды в переданной транзакции."""
        result = await session.execute(_COMPLETE_SQL, {"job_id": job_id, "worker_id": worker_id})
        # Ноль строк означает, что задание больше не наше: аренда истекла и его
        # переиграл другой воркер. Вызывающий обязан откатить транзакцию.
        updated: int = result.rowcount  # type: ignore[attr-defined]
        return updated > 0

    async def fail(
        self,
        job_id: uuid.UUID,
        worker_id: str,
        error_class: JobErrorClass,
    ) -> JobStatus | None:
        """Завершить неудачную попытку задания, назначив повтор либо окончательный отказ."""
        async with self._sessionmaker() as session, session.begin():
            current = await session.execute(
                text("SELECT kind, attempts FROM speech_jobs WHERE id = :job_id"),
                {"job_id": job_id},
            )
            row = current.mappings().first()
            if row is None:
                return None
            kind = JobKind(row["kind"])
            delay = backoff_seconds(kind, error_class, int(row["attempts"]))
            result = await session.execute(
                _FAIL_SQL,
                {
                    "job_id": job_id,
                    "worker_id": worker_id,
                    "backoff": delay,
                    "error_class": error_class.value,
                    # Детерминированный отказ не повторяют: повтор занимает
                    # воркера и заканчивается тем же самым. Этап берётся из
                    # строки, а не из аргумента: терминальность `not_found`
                    # зависит от этапа, и строка — единственный источник,
                    # который не может разойтись с тем, что захвачено.
                    "terminal": is_terminal_error(kind, error_class),
                },
            )
            status_row = result.first()
            return None if status_row is None else JobStatus(status_row[0])

    async def defer(self, job_id: uuid.UUID, worker_id: str, *, delay_seconds: float) -> bool:
        """Вернуть задание в очередь, не считая это отказом. Протокол — §8.1 ADR."""
        async with self._sessionmaker() as session, session.begin():
            result = await session.execute(
                _DEFER_SQL,
                {"job_id": job_id, "worker_id": worker_id, "delay": max(0.0, delay_seconds)},
            )
            return result.first() is not None

    async def requeue_expired(self) -> list[ExpiredJob]:
        """Забрать задания с истёкшей арендой. Что с ними стало — в ответе.

        Бюджет попыток учитывается **здесь**, а не только в `fail`, и это была
        настоящая дыра: воркер, который не отказал, а умер — OOM, `docker kill`,
        падение процесса — никогда не проходил через `fail`. Задание
        возвращалось в очередь по истечении аренды, снова убивало воркера, и так
        по кругу. Счётчик попыток при этом рос, но его никто не проверял: в
        захвате его нет, а в `fail` дело не доходило. ADR обещает, что задание,
        стабильно убивающее воркера, станет `dead` — код обещание не исполнял.

        Возвращается список, а не число: звонок, чьё задание стало `dead`, надо
        перевести в отказ, иначе он навсегда останется «в очереди» и будет
        считаться таковым в статистике.
        """
        async with self._sessionmaker() as session, session.begin():
            rows = (await session.execute(_REQUEUE_EXPIRED_SQL)).mappings().all()
        return [
            ExpiredJob(
                id=uuid.UUID(str(row["id"])),
                call_id=uuid.UUID(str(row["call_id"])),
                kind=JobKind(row["kind"]),
                status=JobStatus(row["status"]),
            )
            for row in rows
        ]

    async def snapshot(self) -> QueueSnapshot:
        """Вернуть согласованные счётчики очереди и возраст её полос."""
        async with self._sessionmaker() as session:
            rows = (await session.execute(_SNAPSHOT_SQL)).mappings().all()
            error_rows = (await session.execute(_ERRORS_SQL)).mappings().all()
            failed_24h = await session.scalar(_FAILED_24H_SQL)
            now = await session.scalar(text("SELECT now()"))

        lanes: list[LaneDepth] = []
        dead_total = 0
        for row in rows:
            status = JobStatus(row["status"])
            count = int(row["total"])
            if status is JobStatus.DEAD:
                dead_total += count
            oldest = row["oldest_queued"]
            lanes.append(
                LaneDepth(
                    lane=JobLane(row["lane"]),
                    status=status,
                    count=count,
                    oldest_queued_age=(now - oldest) if oldest is not None and now else None,
                )
            )
        return QueueSnapshot(
            lanes=tuple(lanes),
            dead_total=dead_total,
            failed_last_24h=int(failed_24h or 0),
            errors_by_class={
                JobErrorClass(row["last_error_class"]): int(row["total"])
                for row in error_rows
                if row["last_error_class"] in {e.value for e in JobErrorClass}
            },
        )

    @asynccontextmanager
    async def stage_slot(self, slots: int) -> AsyncIterator[bool]:
        """Один из ограниченного числа слотов расшифровки.

        Ограничение внешнее: сервер Whisper один, и десять воркеров, пришедших
        к нему одновременно, не ускорят обработку, а превратят её в набор
        таймаутов.

        Слот — advisory-блокировка **уровня сессии**, а не транзакции: этап
        расшифровки живёт дольше одной транзакции. Поэтому нужна выделенная
        физическая сессия, которую нельзя вернуть в пул до освобождения слота.

        Выдаёт `False`, если свободных слотов нет. Вызывающий обязан отложить
        задание, а не ждать: занятый воркер, ждущий слота, — это воркер,
        который не берёт другую работу.
        """
        if self._engine is None:
            raise RuntimeError(
                "stage_slot requires an engine: a session-level advisory lock "
                "must be held on one dedicated connection, which a sessionmaker "
                "alone cannot guarantee"
            )
        if slots < 1:
            raise ValueError("stage_slot requires at least one slot")

        async with self._engine.connect() as connection:
            acquired: int | None = None
            for slot in range(slots):
                got = await connection.scalar(
                    text("SELECT pg_try_advisory_lock(:ns, :slot)"),
                    {"ns": ADVISORY_NAMESPACE, "slot": slot},
                )
                if got:
                    acquired = slot
                    break
            try:
                yield acquired is not None
            finally:
                if acquired is not None:
                    await connection.exec_driver_sql(
                        f"SELECT pg_advisory_unlock({ADVISORY_NAMESPACE}, {acquired})"
                    )


async def acquire_singleton(session: AsyncSession, name: str) -> bool:
    """Захватить именованную блокировку на время транзакции.

    Для суточного прогона: два запущенных одновременно не сломают данные —
    от этого защищает уникальность `session_id` и `(call_id, kind)`, — но
    удвоят работу и запутают журнал. Блокировка уровня транзакции, а не сессии:
    прогон и есть транзакция по смыслу, а забытая сессионная блокировка
    пережила бы упавший процесс.
    """
    key = stable_advisory_key(name)
    got = await session.scalar(
        text("SELECT pg_try_advisory_xact_lock(:ns, :key)"),
        {"ns": ADVISORY_NAMESPACE, "key": key},
    )
    return bool(got)


def stable_advisory_key(name: str) -> int:
    """Устойчивый 31-битный ключ из имени.

    `hash()` не годится: он рандомизирован между процессами, и два воркера
    получили бы разные ключи для одного имени — то есть блокировка не
    блокировала бы ничего.
    """
    digest = hashlib.sha256(name.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") & 0x7FFFFFFF
