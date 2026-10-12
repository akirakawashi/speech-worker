"""Воркер конвейера: берёт задание, выполняет этап, записывает результат.

Запуск:

    python -m src.presentation.cli.worker --lanes daily
    python -m src.presentation.cli.worker --lanes backfill,daily

Полосы задаются аргументом, а не настройкой в базе: какие задания берёт этот
процесс — свойство развёртывания. Воркеры полосы `backfill` перечисляют обе,
чтобы добирать суточные задания, когда те есть; приоритет в очереди
гарантирует, что суточные идут первыми.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import random
import signal
import socket
import uuid
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any, Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.dto.calls import (
    SPEECH_DEFAULT_SETTINGS,
    AnalysisStatus,
    JobErrorClass,
    JobKind,
    JobLane,
    JobStatus,
    ProcessingStatus,
)
from src.application.dto.pipeline_settings import PipelineSettings
from src.application.pipeline.stages import (
    StageError,
    StageHandler,
    StageRegistry,
    StageResult,
)
from src.application.ports.queue import ClaimedJob, NewJob
from src.infrastructure.persistence.settings_store import read_runtime_settings
from src.infrastructure.persistence.stages.repository import PostgresStagePersistence
from src.infrastructure.queue.postgres import PostgresJobQueue
from src.infrastructure.queue.resources import WorkerResources

logger = logging.getLogger(__name__)

#: Пауза, когда очередь пуста. Джиттер разводит воркеров, которые иначе
#: просыпались бы в одну секунду и одновременно били по одному индексу.
IDLE_SLEEP_SECONDS = 2.0
IDLE_JITTER_SECONDS = 0.5

#: Как часто подтверждать аренду. Втрое чаще самой короткой аренды: одно
#: пропущенное подтверждение не должно приводить к отбору задания.
HEARTBEAT_INTERVAL_SECONDS = 30.0

#: Сколько подряд неудачных подтверждений аренды терпим, прежде чем сдаться.
#:
#: Не единица: одна осечка базы — исчерпан пул, перезапущен Postgres — не
#: должна стоить аренды посреди пятнадцатиминутной расшифровки. И не
#: бесконечность: если база недоступна всерьёз, продолжать делать вид, что
#: аренда наша, значит готовить столкновение с тем, кто её уже перехватил.
HEARTBEAT_MAX_FAILURES = 3

#: Как часто возвращать в очередь задания с истёкшей арендой.
REAPER_INTERVAL_SECONDS = 60.0

#: Через сколько пробовать задание, которому не хватило слота. Порядок величины
#: — время одной расшифровки: раньше слот всё равно не освободится, а частые
#: возвраты только гоняют очередь вхолостую.
SLOT_RETRY_SECONDS = 60.0

#: Долгая фаза отказала и уже записала это. Отдельное значение, а не `None`:
#: `None` — законный результат этапа, которому нечего передать в запись.
_FAILED: Final = object()


def worker_identity() -> str:
    """Кто держит задание — так, чтобы это можно было найти.

    Имя контейнера плюс идентификатор процесса плюс короткий случайный хвост.
    Только случайного мало: по нему не найти контейнер, который надо смотреть.
    Только имени контейнера мало: перезапущенный процесс получит то же имя и
    сможет подтвердить аренду задания, которое держал его предшественник.
    """
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"


class Worker:
    """Цикл обработки заданий одной или нескольких полос."""

    def __init__(
        self,
        queue: PostgresJobQueue,
        registry: StageRegistry,
        deps: WorkerResources,
        *,
        lanes: Sequence[JobLane],
        worker_id: str | None = None,
        idle_sleep: float = IDLE_SLEEP_SECONDS,
    ) -> None:
        """Настроить Worker из переданных параметров и зависимостей."""
        if not lanes:
            raise ValueError("worker requires at least one lane")
        # Неполный набор этапов допустим, но не молча. Раньше здесь стоял
        # безусловный `assert_complete()` с верной причиной: воркер, берущий
        # этап, которого не умеет, проваливает его и повторяет, и снаружи это
        # выглядит как отказ сервиса. Причина закрыта иначе — воркер забирает
        # из очереди **только свои** этапы, — а сам факт неполноты объявляется
        # при старте, каждый раз.
        skipped = [kind for kind in JobKind if kind not in registry.kinds]
        if skipped:
            logger.warning(
                "worker takes only %s; jobs of stage(s) %s stay queued for another worker",
                ",".join(kind.value for kind in registry.ordered_kinds),
                ",".join(kind.value for kind in skipped),
            )
        self._queue = queue
        self._registry = registry
        self._deps = deps
        if deps.sessionmaker is None:
            raise ValueError("worker requires a transaction factory")
        self._sessions = deps.sessionmaker
        self._lanes = list(lanes)
        self._id = worker_id or worker_identity()
        self._idle_sleep = idle_sleep
        self._stopping = asyncio.Event()

    @property
    def worker_id(self) -> str:
        """Вернуть идентификатор владельца аренды заданий."""
        return self._id

    def request_stop(self) -> None:
        """Остановиться после текущего задания.

        Не отменяет выполняющееся: прерванная расшифровка вернётся в очередь по
        истечении аренды и будет сделана заново, то есть работа, за которую уже
        заплачено временем GPU, выбрасывается. Досчитать начатое дешевле.
        """
        self._stopping.set()

    async def run_forever(self) -> None:
        """Обрабатывать очередь до остановки, сопровождая аренды и просроченные задания."""
        reaper = asyncio.create_task(self._reap_loop())
        try:
            while not self._stopping.is_set():
                processed = await self.run_once()
                if not processed:
                    await self._sleep_idle()
        finally:
            reaper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reaper

    async def run_once(self) -> bool:
        """Взять и обработать одно задание. `False`, если очередь пуста."""
        async with self._sessions() as session:
            configured = await read_runtime_settings(session)
        # Copy instead of mutating shared dependencies used by another worker.
        self._deps = self._deps.model_copy(
            update={"runtime_settings": PipelineSettings.model_validate(configured)}
        )
        job = await self._queue.claim(self._lanes, self._id, kinds=self._registry.ordered_kinds)
        if job is None:
            return False
        await self._process(job)
        return True

    async def _process(self, job: ClaimedJob) -> None:
        """Выполнить захваченное задание с heartbeat и атомарной записью результата."""
        heartbeat = asyncio.create_task(self._heartbeat_loop(job))
        log = logger.getChild(job.kind.value)
        try:
            handler = self._registry.get(job.kind)
            limit = handler.slot_limit(self._deps)
            async with self._slot(limit) as admitted:
                if not admitted:
                    # Внешний ресурс занят. Это не отказ: задание уходит обратно
                    # в очередь без класса ошибки и с возвращённой попыткой,
                    # а воркер берёт следующее. Ждать освобождения нельзя —
                    # ждущий воркер это воркер, не берущий работу (§8 ADR).
                    log.info(
                        "all %s slot(s) for stage %s are taken, deferring job=%s",
                        limit,
                        job.kind.value,
                        job.id,
                    )
                    await self._queue.defer(job.id, self._id, delay_seconds=SLOT_RETRY_SECONDS)
                    return
                work = await self._work(job, handler, log)
            if work is _FAILED:
                return
            await self._store(job, handler, work, log)
        finally:
            await _stop_heartbeat(heartbeat, job, log)

    @asynccontextmanager
    async def _slot(self, limit: int | None) -> AsyncIterator[bool]:
        """Занять слот этапа, если он ограничен.

        Без ограничения — вход свободный, и никакого соединения к базе на это
        не тратится.
        """
        if limit is None:
            yield True
            return
        async with self._queue.stage_slot(limit) as admitted:
            yield admitted

    async def _work(self, job: ClaimedJob, handler: StageHandler[Any], log: logging.Logger) -> Any:
        """Долгая фаза с разбором её отказов. `_FAILED`, если этап отказал.

        Отдельный метод, а не вложенный `try`, потому что вокруг него теперь
        стоит слот: возврат из середины `async with` оставил бы слот занятым до
        выхода из блока, а `return` внутри контекстного менеджера читается тем
        хуже, чем длиннее блок.
        """
        try:
            return await handler.work(job, self._deps)
        except StageError as exc:
            log.warning(
                "stage failed: job=%s call=%s attempt=%s class=%s",
                job.id,
                job.call_id,
                job.attempts,
                exc.error_class.value,
            )
            await self._fail(job, exc.error_class)
            return _FAILED
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - класс неизвестен, и врать о нём нельзя
            log.error(
                "stage raised an unclassified error: job=%s call=%s type=%s",
                job.id,
                job.call_id,
                type(exc).__name__,
            )
            del exc
            await self._fail(job, JobErrorClass.UNEXPECTED_ERROR)
            return _FAILED

    async def _fail(self, job: ClaimedJob, error_class: JobErrorClass) -> None:
        """Записать отказ и, если задание умерло окончательно, пометить звонок.

        Без второй половины звонок с исчерпанными попытками навсегда остаётся
        в статусе «в очереди» — и попадает в счётчик, который видит
        пользователь.
        """
        status = await self._queue.fail(job.id, self._id, error_class)
        if status is not JobStatus.DEAD:
            return
        logger.error(
            "job=%s call=%s stage=%s is dead after %s attempt(s), class=%s",
            job.id,
            job.call_id,
            job.kind.value,
            job.attempts,
            error_class.value,
        )
        async with self._sessions() as session, session.begin():
            await mark_call_after_dead_job(session, job.call_id, job.kind)

    async def _store(
        self,
        job: ClaimedJob,
        handler: StageHandler[Any],
        work: Any,
        log: logging.Logger,
    ) -> None:
        """Записать результат, завершить задание и поставить следующее — вместе."""
        max_attempts = stage_max_attempts(self._deps.runtime_settings.model_dump())
        try:
            async with self._sessions() as session, session.begin():
                result = await handler.store(PostgresStagePersistence(session), job, work)
                owned = await self._queue.complete(session, job.id, self._id)
                if not owned:
                    # Аренда истекла, задание переиграл другой воркер. Наш
                    # результат устарел; записать его — значит затереть чужую
                    # актуальную работу. Откатываемся.
                    log.warning(
                        "job was taken over while we worked on it, discarding our result: "
                        "job=%s call=%s",
                        job.id,
                        job.call_id,
                    )
                    raise _LostLease
                await _apply_call_status(session, job, result)
                if result.next_kind is not None:
                    await self._queue.enqueue(
                        session,
                        NewJob(
                            call_id=job.call_id,
                            kind=result.next_kind,
                            lane=job.lane,
                            max_attempts=max_attempts.get(result.next_kind, 3),
                            lease_seconds=_lease_for(result.next_kind),
                            # Срочность — свойство звонка, а не этапа: отложенный
                            # на ночь звонок и после приёма идёт за свежими.
                            priority=job.priority,
                        ),
                    )
        except _LostLease:
            return
        except StageError as exc:
            # Запись результата тоже может отказать по известной причине —
            # например, обработчик увидел в транзакции, что писать уже нечего.
            log.warning(
                "storing the result failed: job=%s call=%s attempt=%s class=%s",
                job.id,
                job.call_id,
                job.attempts,
                exc.error_class.value,
            )
            await self._fail(job, exc.error_class)
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - класс неизвестен, и врать о нём нельзя
            # Раньше этой ветки не было, и любая осечка базы на коммите —
            # разорванное соединение, исчерпанный пул, взаимная блокировка —
            # вылетала из `run_forever` и убивала процесс воркера целиком,
            # вместе со всеми остальными заданиями, которые он мог бы взять.
            # Транзакция здесь одна и последним в ней идёт коммит, поэтому
            # отказ означает, что не записано ничего: повтор этапа безопасен.
            # Tracebacks include SQL parameters and chained provider responses.
            # Record identifiers and a type only, never the exception text.
            log.error(
                "storing the result raised an unclassified error: job=%s call=%s type=%s",
                job.id,
                job.call_id,
                type(exc).__name__,
            )
            del exc
            await self._fail(job, JobErrorClass.UNEXPECTED_ERROR)
            return
        log.info(
            "stage done: job=%s call=%s next=%s",
            job.id,
            job.call_id,
            result.next_kind.value if result.next_kind else "-",
        )

    async def _heartbeat_loop(self, job: ClaimedJob) -> None:
        """Продлевать аренду, пока идёт работа.

        Цикл **не имеет права выбросить исключение**, и это не общее правило
        аккуратности, а конкретный урок: сборщик просроченных ниже такую защиту
        имеет, а этот цикл её сначала не имел. Разница обходилась дорого. Одна
        осечка базы — исчерпан пул, перезапущен Postgres — убивала задачу
        молча: аренда переставала обновляться без единой строки в журнале, а
        затем сохранённое в задаче исключение вылетало из блока `finally` в
        `_process` и роняло весь процесс воркера.

        Поэтому: осечка логируется и не прерывает цикл, а сдаёмся только после
        нескольких подряд. Одна неудачная попытка из тридцати за пятнадцать
        минут не должна стоить аренды.
        """
        interval = min(HEARTBEAT_INTERVAL_SECONDS, max(1.0, job.lease_seconds / 3))
        consecutive_failures = 0
        while True:
            await asyncio.sleep(interval)
            try:
                alive = await self._queue.heartbeat(job.id, self._id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - подтверждение аренды не должно ронять воркер
                consecutive_failures += 1
                logger.warning(
                    "heartbeat failed for job=%s (%s/%s consecutive) type=%s",
                    job.id,
                    consecutive_failures,
                    HEARTBEAT_MAX_FAILURES,
                    type(exc).__name__,
                )
                if consecutive_failures >= HEARTBEAT_MAX_FAILURES:
                    logger.error(
                        "giving up on the lease for job=%s after %s failed heartbeats; "
                        "the reaper will requeue it",
                        job.id,
                        consecutive_failures,
                    )
                    return
                continue
            consecutive_failures = 0
            if not alive:
                # Задание больше не наше. Работу не прерываем: она всё равно
                # уже сделана наполовину, а `complete` откажет и результат
                # будет отброшен там, где это видно и залогировано.
                logger.warning("lost the lease on job=%s", job.id)
                return

    async def _reap_loop(self) -> None:
        """Возвращать просроченные аренды очереди до сигнала остановки."""
        while True:
            await asyncio.sleep(REAPER_INTERVAL_SECONDS)
            try:
                expired = await self._queue.requeue_expired()
            except Exception as exc:  # noqa: BLE001 - сборщик не должен ронять воркер
                logger.error("reaper failed type=%s", type(exc).__name__)
                continue
            if not expired:
                continue
            requeued = [job for job in expired if job.status is JobStatus.QUEUED]
            buried = [job for job in expired if job.status is JobStatus.DEAD]
            if requeued:
                logger.info("returned %s expired job(s) to the queue", len(requeued))
            for job in buried:
                # Задание исчерпало попытки, умирая вместе с воркерами. Звонок
                # обязан перестать числиться в очереди: иначе он останется в
                # ней навсегда и будет попадать в счётчик «в очереди на
                # расшифровку», который смотрит пользователь.
                logger.error(
                    "job=%s call=%s stage=%s died of repeated lease expiry; marking the call",
                    job.id,
                    job.call_id,
                    job.kind.value,
                )
                try:
                    await self._mark_call_dead(job.call_id, job.kind)
                except Exception as exc:  # noqa: BLE001 - сборщик не должен ронять воркер
                    logger.error(
                        "could not mark call=%s after a dead job type=%s",
                        job.call_id,
                        type(exc).__name__,
                    )

    async def _mark_call_dead(self, call_id: uuid.UUID, kind: JobKind) -> None:
        """Обновить статус звонка после окончательного отказа его задания."""
        async with self._sessions() as session, session.begin():
            await mark_call_after_dead_job(session, call_id, kind)

    async def _sleep_idle(self) -> None:
        """Дождаться следующего опроса очереди либо остановки воркера."""
        delay = self._idle_sleep + random.uniform(-IDLE_JITTER_SECONDS, IDLE_JITTER_SECONDS)  # noqa: S311
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), timeout=max(0.1, delay))


class _LostLease(Exception):
    """Внутренний сигнал: задание отобрано, транзакцию надо откатить."""


async def mark_call_after_dead_job(
    session: AsyncSession, call_id: uuid.UUID, kind: JobKind
) -> None:
    """Перевести звонок в отказ после того, как его задание умерло.

    Разбор по этапу — не аккуратность, а требование PRC-05. Отказ разметки не
    должен убирать звонок из поиска: текст расшифрован, найти его можно,
    отсутствует только чип темы. А отказ скачивания или расшифровки означает,
    что искать нечего.

    Без этого звонок оставался бы в статусе «в очереди» навсегда и попадал бы в
    счётчик «в очереди на расшифровку» — тот, что видит пользователь на экране.
    """
    if kind is JobKind.ANALYZE:
        await session.execute(
            text(
                "UPDATE speech_calls "
                "SET analysis_status = :failed, processing_status = :done, "
                "    completed_at = COALESCE(completed_at, now()), updated_at = now() "
                "WHERE id = :call_id"
            ),
            {
                "failed": AnalysisStatus.FAILED.value,
                "done": ProcessingStatus.DONE.value,
                "call_id": call_id,
            },
        )
        return
    await session.execute(
        text(
            "UPDATE speech_calls "
            "SET processing_status = :failed, "
            "    completed_at = COALESCE(completed_at, now()), updated_at = now() "
            "WHERE id = :call_id"
        ),
        {"failed": ProcessingStatus.FAILED.value, "call_id": call_id},
    )


async def _stop_heartbeat(
    heartbeat: asyncio.Task[None], job: ClaimedJob, log: logging.Logger
) -> None:
    """Остановить подтверждение аренды, ничего не выбросив наружу.

    Вызывается из `finally`, и это накладывает жёсткое ограничение: исключение,
    выброшенное отсюда, **подменит собой исход самого задания** — успешное
    завершение превратится в отказ процесса, а настоящую причину никто не
    увидит.

    Тонкость, на которой это ломалось: `cancel()` у уже завершившейся задачи
    ничего не делает и возвращает `False`, а последующее `await` поднимает
    сохранённое в ней исключение. Подавления `CancelledError` для такого случая
    недостаточно — нужно снять исключение явно.
    """
    heartbeat.cancel()
    try:
        await heartbeat
    except asyncio.CancelledError:
        # Обычный путь: задача жила и была снята.
        pass
    except Exception as exc:  # noqa: BLE001 - из finally наружу не выпускаем ничего
        log.warning(
            "heartbeat task for job=%s ended with an error type=%s; the job outcome above stands",
            job.id,
            type(exc).__name__,
        )


async def _apply_call_status(session: AsyncSession, job: ClaimedJob, result: StageResult) -> None:
    """Перевести звонок в новый статус.

    Отдельной функцией, потому что это делает каждый этап и делать это должен
    одинаково: статус звонка — то, что видит пользователь, и разнобой здесь
    читается как случайные пропадания звонков из выдачи.

    `completed_at` проставляется только в конце цепочки. Это колонка
    безопасности для политик хранения: непустая она означает «работа над
    звонком закончена», и проставить её на середине значило бы разрешить
    удалять недоделанное.
    """
    await session.execute(
        text(
            "UPDATE speech_calls SET processing_status = :status, updated_at = now(), "
            "completed_at = CASE WHEN :terminal THEN now() ELSE completed_at END "
            "WHERE id = :call_id"
        ),
        {
            "status": result.processing_status.value,
            "terminal": result.next_kind is None,
            "call_id": job.call_id,
        },
    )


def stage_max_attempts(configured: dict[str, Any] | None = None) -> dict[JobKind, int]:
    """Сколько попыток даётся каждому этапу.

    Снимок базы читается перед захватом; умолчания нужны автономным тестам.
    """
    raw = (configured or SPEECH_DEFAULT_SETTINGS)["max_attempts"]
    assert isinstance(raw, dict)
    return {JobKind(k): int(v) for k, v in raw.items()}


def _lease_for(kind: JobKind) -> int:
    """Аренда по этапу.

    У расшифровки она втрое длиннее: часовая запись на загруженном Whisper
    обрабатывается дольше пяти минут, и короткая аренда вернула бы в очередь
    живую работу — а вместе с ней выбросила бы уже потраченное время GPU.
    """
    return 900 if kind is JobKind.TRANSCRIBE else 300


def parse_lanes(raw: str) -> list[JobLane]:
    """Прочитать список полос CLI; неизвестную или пустую полосу отклонить."""
    lanes: list[JobLane] = []
    for part in raw.split(","):
        name = part.strip()
        if not name:
            continue
        try:
            lane = JobLane(name)
        except ValueError:
            known = ", ".join(sorted(item.value for item in JobLane))
            raise SystemExit(f"unknown lane {name!r}. Known: {known}") from None
        if lane not in lanes:
            lanes.append(lane)
    if not lanes:
        raise SystemExit("--lanes must name at least one lane")
    return lanes


def _install_signal_handlers(worker: Worker) -> None:
    """Привязать сигналы остановки к завершению рабочего цикла воркера."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            # Windows не поддерживает add_signal_handler для SIGTERM; там
            # воркер останавливают иначе, и это не повод не ставить обработчик
            # там, где он работает.
            loop.add_signal_handler(sig, worker.request_stop)


def parse_stages(raw: str) -> tuple[JobKind, ...]:
    """Разобрать `--stages`. Пусто — все этапы.

    Порядок возвращается в порядке `JobKind`, а не в том, как их перечислил
    оператор: этапы образуют цепочку, и список, читающийся не по ней, путает
    того, кто смотрит в лог.
    """
    names = [part.strip() for part in raw.split(",") if part.strip()]
    if not names:
        return tuple(JobKind)
    unknown = [name for name in names if name not in {kind.value for kind in JobKind}]
    if unknown:
        raise ValueError(f"unknown stages: {', '.join(unknown)}")
    chosen = {JobKind(name) for name in names}
    return tuple(kind for kind in JobKind if kind in chosen)
