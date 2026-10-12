"""Воркер и суточный прогон на настоящем Postgres.

Обработчики этапов здесь подставные — намеренно. Проверяется не расшифровка, а
то, что вокруг неё: что результат и завершение задания записываются вместе, что
отказ возвращает задание с правильным классом, и что результат воркера,
потерявшего аренду, отбрасывается, а не затирает чужую работу.

Нужен `SPEECH_POSTGRES_TEST_URL` с применённой схемой.
"""

from __future__ import annotations

import logging
import random
from collections.abc import AsyncIterator
from datetime import date, datetime
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.application.dto.daily_ingest import DayShare
from src.application.pipeline.stages import (
    StageDeps,
    StageError,
    StageHandler,
    StageRegistry,
    StageResult,
)
from src.application.ports.pipeline import StagePersistence
from src.application.ports.queue import ClaimedJob, NewJob
from src.di.ingestion import run_day
from src.di.pipeline import build_stage_deps
from src.domain.calls.contracts import (
    AnalysisStatus,
    JobErrorClass,
    JobKind,
    JobLane,
    JobStatus,
    ProcessingStatus,
)
from src.infrastructure.persistence.daily_ingest import CURSOR_SOURCE
from src.infrastructure.persistence.stages.repository import PostgresStagePersistence
from src.infrastructure.queue.postgres import PostgresJobQueue
from src.infrastructure.queue.worker import Worker
from src.infrastructure.sources.ncc.source import LocalExportSource
from tests.speech_db_support import speech_test_database_url

_TEST_DATABASE_URL = speech_test_database_url()
pytestmark = pytest.mark.skipif(
    not _TEST_DATABASE_URL,
    reason="SPEECH_POSTGRES_TEST_URL is required",
)

KEY = "ncc-tm%2F2026%2F01%2F14%2Fnode_1_domain_2_nauss_0_1766092829_1138986.mp3"
KEY_B = "ncc-tm%2F2026%2F01%2F14%2Fnode_2_domain_1_nauss_1_1766092823_1603045.mp3"
DAY = date(2026, 1, 14)


class _Recorded(StageHandler[str]):
    """Обработчик, который ничего не делает и запоминает, что его звали."""

    def __init__(self, kind: JobKind, *, next_kind: JobKind | None, fail: StageError | None = None):
        # На экземпляре, а не на классе. `type(self).kind = kind` менял вид
        # этапа у **всех** уже созданных обработчиков: два таких в одном тесте
        # — обычное дело (этап и его продолжение), и второй молча переписывал
        # первый. `kind` объявлен у `StageHandler` переменной класса, поэтому
        # присваивание через экземпляр требует пометки для проверки типов.
        self.kind = kind  # type: ignore[misc]
        self._kind = kind
        self._next = next_kind
        self._fail = fail
        self.calls: list[ClaimedJob] = []

    async def work(self, job: ClaimedJob, deps: StageDeps) -> str:
        self.calls.append(job)
        if self._fail is not None:
            raise self._fail
        return f"work:{job.call_id}"

    async def store(self, persistence: StagePersistence, job: ClaimedJob, work: str) -> StageResult:
        session = cast(PostgresStagePersistence, persistence)._session
        await session.execute(
            text("UPDATE speech_calls SET summary = :s WHERE id = :id"),
            {"s": work, "id": job.call_id},
        )
        return StageResult(
            processing_status=(
                ProcessingStatus.DONE if self._next is None else ProcessingStatus.TRANSCRIBING
            ),
            next_kind=self._next,
        )


class _Exploding(StageHandler[str]):
    kind = JobKind.INGEST

    async def work(self, job: ClaimedJob, deps: StageDeps) -> str:
        raise RuntimeError("что-то, о чём мы не думали")

    async def store(self, persistence: StagePersistence, job: ClaimedJob, work: str) -> StageResult:
        raise AssertionError("не должно вызываться")


class _Limited(StageHandler[str]):
    """Этап, который упирается во внешний ресурс и объявляет его предел."""

    kind = JobKind.INGEST

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self.works = 0

    def slot_limit(self, deps: StageDeps) -> int | None:
        return self._limit

    async def work(self, job: ClaimedJob, deps: StageDeps) -> str:
        self.works += 1
        return "done"

    async def store(self, persistence: StagePersistence, job: ClaimedJob, work: str) -> StageResult:
        return StageResult(processing_status=ProcessingStatus.DONE, next_kind=None)


class _ExplodingOnStore(StageHandler[str]):
    """Работа удалась, запись — нет. Так выглядит осечка базы на коммите.

    Разорванное соединение, исчерпанный пул, взаимная блокировка — всё это
    приходит не из `work`, а из транзакции записи, и до появления этого
    обработчика такой отказ не проверялся ничем.
    """

    kind = JobKind.INGEST

    def __init__(self, error: Exception) -> None:
        self._error = error
        self.stores = 0

    async def work(self, job: ClaimedJob, deps: StageDeps) -> str:
        return f"work:{job.call_id}"

    async def store(self, persistence: StagePersistence, job: ClaimedJob, work: str) -> StageResult:
        self.stores += 1
        raise self._error


def _registry(*handlers: StageHandler[str]) -> StageRegistry:
    """Реестр, закрытый для всех этапов: часть — заглушками-пустышками."""
    registry = StageRegistry()
    given = {h.kind for h in handlers}
    for handler in handlers:
        registry.register(handler)
    for kind in JobKind:
        if kind not in given:
            registry.register(_Recorded(kind, next_kind=None))
    return registry


@pytest.fixture()
async def env(tmp_path: Path) -> AsyncIterator[tuple[PostgresJobQueue, StageDeps, Path]]:
    assert _TEST_DATABASE_URL is not None
    engine = create_async_engine(_TEST_DATABASE_URL)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def _clean() -> None:
        async with sessions() as session, session.begin():
            await session.execute(text("DELETE FROM speech_jobs"))
            await session.execute(text("DELETE FROM speech_calls"))
            await session.execute(text("DELETE FROM speech_ingest_cursors"))

    await _clean()
    (tmp_path / KEY).write_bytes(b"ID3fake")
    (tmp_path / KEY_B).write_bytes(b"ID3fake")
    deps = build_stage_deps(settings=None, sessionmaker=sessions)
    yield PostgresJobQueue(sessions, engine=engine), deps, tmp_path
    await _clean()
    await engine.dispose()


async def _seed_day(queue: PostgresJobQueue, deps: StageDeps, export: Path) -> None:
    await run_day(
        DAY, source=LocalExportSource(export), sessionmaker=deps.sessionmaker, queue=queue
    )


# --------------------------------------------------------------------------- #
# Суточный прогон
# --------------------------------------------------------------------------- #


async def test_daily_run_registers_calls_and_queues_them(env) -> None:
    queue, deps, export = env

    report = await run_day(
        DAY, source=LocalExportSource(export), sessionmaker=deps.sessionmaker, queue=queue
    )

    assert report.discovered == 2
    assert report.enqueued == 2
    assert report.already_known == 0
    async with deps.sessionmaker() as session:
        assert await session.scalar(text("SELECT count(*) FROM speech_calls")) == 2
        assert (
            await session.scalar(text("SELECT count(*) FROM speech_jobs WHERE kind = 'ingest'"))
            == 2
        )


async def test_running_the_same_day_twice_changes_nothing(env) -> None:
    """Повторный прогон за ту же дату — обычное дело: перезапуск после сбоя."""
    queue, deps, export = env
    await _seed_day(queue, deps, export)

    second = await run_day(
        DAY, source=LocalExportSource(export), sessionmaker=deps.sessionmaker, queue=queue
    )

    assert second.discovered == 2
    assert second.enqueued == 0, "второй раз задания не ставятся"
    assert second.already_known == 2
    async with deps.sessionmaker() as session:
        assert await session.scalar(text("SELECT count(*) FROM speech_calls")) == 2


async def test_the_processed_day_is_recorded(env) -> None:
    """Чтобы «звонков не было» отличалось от «прогон не запускался»."""
    queue, deps, export = env
    await _seed_day(queue, deps, export)

    async with deps.sessionmaker() as session:
        stored = await session.scalar(
            text("SELECT last_processed_day FROM speech_ingest_cursors WHERE source = :s"),
            {"s": CURSOR_SOURCE},
        )
    assert stored == DAY


async def test_time_precision_is_recorded_when_the_source_has_no_clock(env) -> None:
    """Выгрузка знает только дату — и это записано, а не замаскировано полночью."""
    queue, deps, export = env
    await _seed_day(queue, deps, export)

    async with deps.sessionmaker() as session:
        precision = await session.scalar(
            text("SELECT source_snapshot->>'time_precision' FROM speech_calls LIMIT 1")
        )
    assert precision == "day"


async def test_a_limited_run_takes_the_first_calls_and_leaves_the_day_open(env) -> None:
    """Пробный прогон на части звонков не выдаёт сутки за обработанные."""
    queue, deps, export = env

    report = await run_day(
        DAY, source=LocalExportSource(export), sessionmaker=deps.sessionmaker, queue=queue, limit=1
    )

    assert report.discovered == 2
    assert report.enqueued == 1
    assert report.limited_to == 1
    async with deps.sessionmaker() as session:
        assert await session.scalar(text("SELECT count(*) FROM speech_calls")) == 1
        assert await session.scalar(text("SELECT count(*) FROM speech_ingest_cursors")) == 0


async def test_a_limit_that_covers_the_day_is_a_full_run(env) -> None:
    queue, deps, export = env

    report = await run_day(
        DAY, source=LocalExportSource(export), sessionmaker=deps.sessionmaker, queue=queue, limit=10
    )

    assert report.enqueued == 2
    assert report.limited_to is None
    async with deps.sessionmaker() as session:
        stored = await session.scalar(
            text("SELECT last_processed_day FROM speech_ingest_cursors WHERE source = :s"),
            {"s": CURSOR_SOURCE},
        )
    assert stored == DAY


# --------------------------------------------------------------------------- #
# Воркер
# --------------------------------------------------------------------------- #


async def test_worker_stores_the_result_and_queues_the_next_stage(env) -> None:
    queue, deps, export = env
    await _seed_day(queue, deps, export)
    ingest = _Recorded(JobKind.INGEST, next_kind=JobKind.TRANSCRIBE)
    worker = Worker(queue, _registry(ingest), deps, lanes=[JobLane.DAILY], worker_id="w1")

    assert await worker.run_once() is True

    assert len(ingest.calls) == 1
    async with deps.sessionmaker() as session:
        done = await session.scalar(
            text("SELECT count(*) FROM speech_jobs WHERE kind='ingest' AND status='done'")
        )
        queued_next = await session.scalar(
            text("SELECT count(*) FROM speech_jobs WHERE kind='transcribe' AND status='queued'")
        )
        summary = await session.scalar(
            text("SELECT summary FROM speech_calls WHERE summary IS NOT NULL")
        )
    assert done == 1
    assert queued_next == 1, "следующий этап поставлен в той же транзакции"
    assert summary is not None, "результат этапа записан"


async def test_worker_returns_false_on_an_empty_queue(env) -> None:
    queue, deps, _export = env
    worker = Worker(queue, _registry(), deps, lanes=[JobLane.DAILY], worker_id="w1")

    assert await worker.run_once() is False


async def test_a_classified_failure_requeues_the_job_with_its_class(env) -> None:
    queue, deps, export = env
    await _seed_day(queue, deps, export)
    failing = _Recorded(
        JobKind.INGEST,
        next_kind=None,
        fail=StageError(JobErrorClass.NOT_FOUND, "записи нет у источника"),
    )
    worker = Worker(queue, _registry(failing), deps, lanes=[JobLane.DAILY], worker_id="w1")

    await worker.run_once()

    async with deps.sessionmaker() as session:
        row = (
            (
                await session.execute(
                    text(
                        "SELECT status, last_error_class, attempts FROM speech_jobs "
                        "WHERE kind='ingest' AND last_error_class IS NOT NULL"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert row["status"] == JobStatus.QUEUED.value
    assert row["last_error_class"] == JobErrorClass.NOT_FOUND.value
    assert row["attempts"] == 1


async def test_an_unclassified_error_does_not_pretend_to_know_its_class(env) -> None:
    """Угадывать класс по типу исключения — значит врать в статистике отказов."""
    queue, deps, export = env
    await _seed_day(queue, deps, export)
    worker = Worker(queue, _registry(_Exploding()), deps, lanes=[JobLane.DAILY], worker_id="w1")

    await worker.run_once()

    async with deps.sessionmaker() as session:
        error_class = await session.scalar(
            text(
                "SELECT last_error_class FROM speech_jobs "
                "WHERE last_error_class IS NOT NULL LIMIT 1"
            )
        )
    assert error_class == JobErrorClass.UNEXPECTED_ERROR.value


async def test_a_stage_without_a_free_slot_is_deferred_and_not_failed(env) -> None:
    """Занятый GPU не должен выглядеть как сбой Whisper.

    Слот занят снаружи, поэтому воркер обязан вернуть задание в очередь без
    класса отказа и без потраченной попытки, а сам взять следующее. Проверяется
    и то, и другое: воркер, который просто не упал, но и не отпустил задание,
    держал бы его до истечения аренды.
    """
    queue, deps, export = env
    await _seed_day(queue, deps, export)
    handler = _Limited(limit=1)
    worker = Worker(queue, _registry(handler), deps, lanes=[JobLane.DAILY], worker_id="w1")

    async with queue.stage_slot(1) as held:
        assert held is True, "слот занят снаружи — воркеру он не достанется"
        assert await worker.run_once() is True, "задание взято и обработано (отложено)"

    assert handler.works == 0, "долгая фаза не должна была начаться"
    async with deps.sessionmaker() as session:
        row = (
            (
                await session.execute(
                    text(
                        "SELECT status, attempts, last_error_class FROM speech_jobs "
                        "WHERE kind = 'ingest' ORDER BY attempts DESC LIMIT 1"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert row["status"] == JobStatus.QUEUED.value
    assert row["attempts"] == 0, "попытка возвращена: задание не отказывало"
    assert row["last_error_class"] is None


async def test_a_stage_with_a_free_slot_runs_normally(env) -> None:
    """Обратная сторона: ограничение не должно мешать, когда ресурс свободен."""
    queue, deps, export = env
    await _seed_day(queue, deps, export)
    handler = _Limited(limit=2)
    worker = Worker(queue, _registry(handler), deps, lanes=[JobLane.DAILY], worker_id="w1")

    assert await worker.run_once() is True

    assert handler.works == 1
    async with deps.sessionmaker() as session:
        done = await session.scalar(
            text("SELECT count(*) FROM speech_jobs WHERE kind = 'ingest' AND status = 'done'")
        )
    assert done == 1


async def test_a_failure_while_storing_does_not_kill_the_worker(env) -> None:
    """Осечка на записи результата обязана стоить задания, а не воркера.

    Отказ `work` обрабатывался с самого начала, а отказ записи — нет: он
    вылетал из `_store` через `_process` и `run_forever` наружу и убивал
    процесс. Одна взаимная блокировка на одном звонке — и ночную обработку
    доделывать некому, при том что остальные полторы тысячи звонков в очереди
    исправны.

    Проверяется не только «не упал»: воркер обязан взять следующее задание.
    Процесс, доживший до конца вызова и вставший, ничем не лучше упавшего.
    """
    queue, deps, export = env
    await _seed_day(queue, deps, export)
    handler = _ExplodingOnStore(RuntimeError("connection reset during commit"))
    worker = Worker(queue, _registry(handler), deps, lanes=[JobLane.DAILY], worker_id="w1")

    assert await worker.run_once() is True
    assert await worker.run_once() is True, "воркер должен был взять второе задание"

    assert handler.stores == 2
    async with deps.sessionmaker() as session:
        failed = await session.scalar(
            text(
                "SELECT count(*) FROM speech_jobs WHERE last_error_class = :c AND status = 'queued'"
            ),
            {"c": JobErrorClass.UNEXPECTED_ERROR.value},
        )
        # Транзакция записи одна, и коммит в ней последний: отказ означает, что
        # не записано ничего. Иначе повтор этапа не был бы безопасен.
        summaries = await session.scalar(
            text("SELECT count(*) FROM speech_calls WHERE summary IS NOT NULL")
        )
    assert failed == 2, "оба задания вернулись в очередь с честным классом отказа"
    assert summaries == 0, "частичный результат не должен был остаться в базе"


async def test_a_classified_failure_while_storing_keeps_its_class(env) -> None:
    """Обработчик вправе отказать и на записи — класс отказа обязан дойти."""
    queue, deps, export = env
    await _seed_day(queue, deps, export)
    handler = _ExplodingOnStore(StageError(JobErrorClass.NOT_FOUND, "нечего записывать"))
    worker = Worker(queue, _registry(handler), deps, lanes=[JobLane.DAILY], worker_id="w1")

    await worker.run_once()

    async with deps.sessionmaker() as session:
        error_class = await session.scalar(
            text(
                "SELECT last_error_class FROM speech_jobs "
                "WHERE last_error_class IS NOT NULL LIMIT 1"
            )
        )
    assert error_class == JobErrorClass.NOT_FOUND.value


async def test_a_worker_that_lost_its_lease_discards_its_result(env) -> None:
    """Главная защита от зомби, проверенная через воркер, а не через очередь.

    Пока первый воркер работал, аренда истекла и задание переиграл второй.
    Результат первого устарел; записать его — значит затереть чужую актуальную
    работу. Транзакция должна откатиться целиком: ни результата, ни
    завершения, ни следующего этапа.
    """
    queue, deps, export = env
    await _seed_day(queue, deps, export)

    stolen = await queue.claim([JobLane.DAILY], "thief")
    assert stolen is not None
    # Первый воркер держит то же задание, но его аренда уже отобрана.
    ghost = ClaimedJob(
        id=stolen.id,
        call_id=stolen.call_id,
        kind=stolen.kind,
        lane=stolen.lane,
        attempts=stolen.attempts,
        max_attempts=stolen.max_attempts,
        payload=stolen.payload,
        lease_seconds=stolen.lease_seconds,
    )
    handler = _Recorded(JobKind.INGEST, next_kind=JobKind.TRANSCRIBE)
    worker = Worker(queue, _registry(handler), deps, lanes=[JobLane.DAILY], worker_id="ghost")

    await worker._store(ghost, handler, "work", __import__("logging").getLogger("t"))  # noqa: SLF001

    async with deps.sessionmaker() as session:
        summary = await session.scalar(
            text("SELECT summary FROM speech_calls WHERE id = :id"), {"id": ghost.call_id}
        )
        next_jobs = await session.scalar(
            text("SELECT count(*) FROM speech_jobs WHERE kind = 'transcribe'")
        )
        status = await session.scalar(
            text("SELECT status FROM speech_jobs WHERE id = :id"), {"id": ghost.id}
        )
    assert summary is None, "результат зомби записан"
    assert next_jobs == 0, "зомби поставил следующий этап"
    assert status == JobStatus.RUNNING.value, "задание осталось у настоящего владельца"


async def test_a_worker_without_all_handlers_takes_only_its_own_stages(
    env, caplog: pytest.LogCaptureFixture
) -> None:
    """Неполный набор этапов разрешён, но задание чужого этапа не берётся.

    Раньше здесь стоял безусловный отказ стартовать, и причина была верной:
    воркер, берущий этап, которого он не умеет, проваливает задание и повторяет
    его до исчерпания попыток — со стороны это неотличимо от сбоя внешней
    системы, хотя код просто не написан.

    Причина закрыта иначе. Неполный набор понадобился по-настоящему: приложения
    LLM ещё нет, а расшифровывать записи уже нужно. Теперь
    воркер забирает из очереди **только свои** этапы — чужое остаётся в
    очереди, — а сам факт неполноты объявляется при старте, каждый раз: молчащий
    воркер оставил бы суточный пакет наполовину обработанным без единого
    сообщения.
    """
    queue, deps, _export = env
    incomplete = StageRegistry()
    incomplete.register(_Recorded(JobKind.INGEST, next_kind=None))

    with caplog.at_level(logging.WARNING):
        worker = Worker(queue, incomplete, deps, lanes=[JobLane.DAILY])

    assert "transcribe" in caplog.text and "analyze" in caplog.text, "неполнота набора не объявлена"

    # Чужое задание лежит в очереди — и остаётся там.
    queue_, deps_, export = env
    await _seed_day(queue_, deps_, export)
    async with deps.sessionmaker() as session, session.begin():
        call_id = await session.scalar(text("SELECT id FROM speech_calls LIMIT 1"))
        await session.execute(text("DELETE FROM speech_jobs"))
        await queue.enqueue(
            session,
            NewJob(call_id=call_id, kind=JobKind.TRANSCRIBE, lane=JobLane.DAILY, max_attempts=1),
        )

    assert await worker.run_once() is False, "взят этап, которого воркер не умеет"

    async with deps.sessionmaker() as session:
        status = await session.scalar(
            text("SELECT status FROM speech_jobs WHERE kind = 'transcribe'")
        )
    assert status == JobStatus.QUEUED.value, "чужой этап уведён из очереди"


def test_a_worker_without_lanes_is_a_misconfiguration(env) -> None:
    queue, deps, _export = env
    with pytest.raises(ValueError, match="at least one lane"):
        Worker(queue, _registry(), deps, lanes=[])


# --------------------------------------------------------------------------- #
# Регрессии по находкам состязательного ревью 2026-09-09
# --------------------------------------------------------------------------- #


class _Slow(StageHandler[str]):
    """Этап, который работает достаточно долго, чтобы аренду успели подтвердить."""

    kind = JobKind.INGEST

    def __init__(self, seconds: float = 1.4) -> None:
        self._seconds = seconds

    async def work(self, job: ClaimedJob, deps: StageDeps) -> str:
        import asyncio

        await asyncio.sleep(self._seconds)
        return "done"

    async def store(self, persistence: StagePersistence, job: ClaimedJob, work: str) -> StageResult:
        return StageResult(processing_status=ProcessingStatus.DONE, next_kind=None)


async def test_a_failing_heartbeat_does_not_kill_the_worker(env, monkeypatch) -> None:
    """Находка ревью: осечка подтверждения аренды роняла весь процесс.

    Механика была такая. `_heartbeat_loop` не ловил исключений, задача умирала с
    сохранённым исключением, `cancel()` у завершившейся задачи ничего не делал,
    а `await` в блоке `finally` поднимал это исключение заново — и подавление
    `CancelledError` его не ловило. Оно вылетало из `finally`, подменяя собой
    исход задания, и убивало воркер.

    Соседний цикл сборщика просроченных такую защиту имел с самого начала.
    Разница между ними и была дефектом.
    """
    queue, deps, export = env
    await _seed_day(queue, deps, export)
    # Короткая аренда: интервал подтверждения — треть от неё, и при штатных
    # 300 секундах оно просто не успело бы сработать за время теста.
    async with deps.sessionmaker() as session, session.begin():
        await session.execute(text("UPDATE speech_jobs SET lease_seconds = 3"))

    attempts = {"n": 0}

    async def exploding_heartbeat(job_id, worker_id):  # noqa: ANN001, ANN202
        attempts["n"] += 1
        raise RuntimeError("пул исчерпан")

    monkeypatch.setattr(queue, "heartbeat", exploding_heartbeat)

    worker = Worker(
        queue,
        _registry(_Slow(1.4)),
        deps,
        lanes=[JobLane.DAILY],
        worker_id="hb",
    )

    # Не должно выброситься наружу вообще ничего.
    assert await worker.run_once() is True
    assert attempts["n"] >= 1, "подтверждение аренды даже не пробовало выполниться"

    # В выгрузке два звонка, а обработано одно задание — считаем завершённые,
    # а не читаем статус первого попавшегося.
    async with deps.sessionmaker() as session:
        done = await session.scalar(text("SELECT count(*) FROM speech_jobs WHERE status = 'done'"))
    assert done == 1, "задание должно было завершиться штатно, несмотря на осечку аренды"


async def test_a_job_that_keeps_killing_workers_eventually_dies(env) -> None:
    """Находка ревью: такое задание повторялось вечно.

    Воркер, который не отказал, а умер — нехватка памяти, `docker kill`, —
    через `fail` не проходит вовсе. Сборщик просроченных возвращал задание в
    очередь, не глядя на бюджет попыток, а в захвате проверки попыток нет.
    ADR обещает, что задание, стабильно убивающее воркеров, станет `dead`;
    код обещание не исполнял.
    """
    queue, deps, export = env
    await _seed_day(queue, deps, export)
    async with deps.sessionmaker() as session, session.begin():
        await session.execute(text("UPDATE speech_jobs SET max_attempts = 2"))

    seen_statuses: list[JobStatus] = []
    for _ in range(4):
        claimed = await queue.claim([JobLane.DAILY], "doomed")
        if claimed is None:
            break
        # Воркер умер, не сказав ни слова: аренда просто истекает.
        async with deps.sessionmaker() as session, session.begin():
            await session.execute(
                text("UPDATE speech_jobs SET locked_at = now() - interval '1 hour' WHERE id = :id"),
                {"id": claimed.id},
            )
        expired = await queue.requeue_expired()
        seen_statuses.extend(job.status for job in expired)

    assert JobStatus.DEAD in seen_statuses, "задание так и не умерло, повторялось бы вечно"
    assert await queue.claim([JobLane.DAILY], "next") is None, "мёртвое задание снова взяли"


async def test_a_dead_job_takes_the_call_out_of_the_queue_counter(env) -> None:
    """Звонок с мёртвым заданием не должен вечно числиться «в очереди».

    Иначе он навсегда остаётся в счётчике «в очереди на расшифровку», который
    показан пользователю на экране.
    """
    queue, deps, export = env
    await _seed_day(queue, deps, export)
    failing = _Recorded(
        JobKind.INGEST,
        next_kind=None,
        fail=StageError(JobErrorClass.INVALID_OUTPUT, "детерминированный отказ"),
    )
    worker = Worker(queue, _registry(failing), deps, lanes=[JobLane.DAILY], worker_id="w1")

    await worker.run_once()

    async with deps.sessionmaker() as session:
        row = (
            (
                await session.execute(
                    text(
                        "SELECT c.processing_status, j.status FROM speech_calls c "
                        "JOIN speech_jobs j ON j.call_id = c.id "
                        "WHERE j.last_error_class IS NOT NULL"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert row["status"] == JobStatus.DEAD.value
    assert row["processing_status"] == ProcessingStatus.FAILED.value


async def test_a_dead_analysis_leaves_the_call_searchable(env) -> None:
    """Требование PRC-05: отказ разметки не убирает звонок из поиска.

    Текст расшифрован, найти его можно, отсутствует только тема. Пометить
    такой звонок сломанным значило бы спрятать работу, которая сделана.
    """
    queue, deps, export = env
    await _seed_day(queue, deps, export)
    async with deps.sessionmaker() as session, session.begin():
        call_id = await session.scalar(text("SELECT id FROM speech_calls LIMIT 1"))
        await session.execute(text("DELETE FROM speech_jobs"))
        await queue.enqueue(
            session,
            NewJob(
                call_id=call_id,
                kind=JobKind.ANALYZE,
                lane=JobLane.DAILY,
                max_attempts=1,
            ),
        )
    failing = _Recorded(
        JobKind.ANALYZE,
        next_kind=None,
        fail=StageError(JobErrorClass.INVALID_OUTPUT, "модель вернула не JSON"),
    )
    worker = Worker(queue, _registry(failing), deps, lanes=[JobLane.DAILY], worker_id="w1")

    await worker.run_once()

    async with deps.sessionmaker() as session:
        row = (
            (
                await session.execute(
                    text(
                        "SELECT processing_status, analysis_status FROM speech_calls WHERE id = :id"
                    ),
                    {"id": call_id},
                )
            )
            .mappings()
            .one()
        )
    assert row["analysis_status"] == AnalysisStatus.FAILED.value
    assert row["processing_status"] == ProcessingStatus.DONE.value, "звонок пропал из поиска"


# --------------------------------------------------------------------------- #
# Дневная доля: часть новых звонков сразу, остальные — с начала ночи
# --------------------------------------------------------------------------- #

MSK = ZoneInfo("Europe/Moscow")
#: Полдень и поздний вечер по Москве: день и ночь для доли «19 → 7».
#:
#: Год дальний нарочно. Отсрочка сверяется с часами базы (`run_after > now()`),
#: и воркер берёт задание по ним же: дата, оставшаяся в прошлом, превращает
#: «отложено до ночи» в «уже можно брать», и тесты падают с того же вечера.
NOON = datetime(2099, 10, 8, 12, 0, tzinfo=MSK)
EVENING = datetime(2099, 10, 8, 21, 0, tzinfo=MSK)
NIGHT_START = datetime(2099, 10, 8, 19, 0, tzinfo=MSK)


def _share(share: float) -> DayShare:
    return DayShare(share=share, night_from=19, night_until=7)


class _Lots(random.Random):
    """Жребий с заданными исходами — по одному на звонок, в порядке источника."""

    def __init__(self, *outcomes: float) -> None:
        super().__init__()
        self._outcomes = list(outcomes)

    def random(self) -> float:
        return self._outcomes.pop(0)


async def _ingest_jobs(deps: StageDeps) -> list[dict]:
    async with deps.sessionmaker() as session:
        rows = await session.execute(
            text(
                "SELECT j.priority, j.run_after, j.payload, j.run_after > now() AS held "
                "FROM speech_jobs AS j JOIN speech_calls AS c ON c.id = j.call_id "
                "WHERE j.kind = 'ingest' ORDER BY c.session_id"
            )
        )
        return [dict(row) for row in rows.mappings()]


async def _release(deps: StageDeps) -> None:
    """Ночь наступила: отсрочка заданий кончилась."""
    async with deps.sessionmaker() as session, session.begin():
        await session.execute(text("UPDATE speech_jobs SET run_after = now()"))


async def test_by_day_a_share_of_new_calls_starts_at_once_and_the_rest_waits_for_the_night(
    env,
) -> None:
    queue, deps, export = env

    report = await run_day(
        DAY,
        source=LocalExportSource(export),
        sessionmaker=deps.sessionmaker,
        queue=queue,
        day_share=_share(0.5),
        now=NOON,
        # Первый звонок вытянул «сразу», второй — «ночью».
        rng=_Lots(0.2, 0.7),
    )

    assert (report.enqueued, report.deferred) == (2, 1)
    assert "до ночи отложено 1" in report.describe()
    now_job, night_job = await _ingest_jobs(deps)
    assert (now_job["priority"], now_job["held"], now_job["payload"]) == (0, False, {})
    # Отложенный ждёт начала ночи и уступает свежим и после неё.
    assert (night_job["priority"], night_job["held"]) == (10, True)
    assert night_job["run_after"] == NIGHT_START
    assert night_job["payload"] == {"not_before": NIGHT_START.isoformat()}

    # Воркер днём берёт только свежий; отложенного для него нет.
    worker = Worker(
        queue,
        _registry(_Recorded(JobKind.INGEST, next_kind=None)),
        deps,
        lanes=[JobLane.DAILY],
        worker_id="w1",
    )
    assert await worker.run_once() is True
    assert await worker.run_once() is False


async def test_at_night_every_new_call_starts_at_once(env) -> None:
    queue, deps, export = env

    report = await run_day(
        DAY,
        source=LocalExportSource(export),
        sessionmaker=deps.sessionmaker,
        queue=queue,
        # Доля ноль — днём не шло бы ничего. Ночью она не действует.
        day_share=_share(0.0),
        now=EVENING,
    )

    assert (report.enqueued, report.deferred) == (2, 0)
    assert [(job["priority"], job["held"]) for job in await _ingest_jobs(deps)] == [
        (0, False),
        (0, False),
    ]


async def test_a_share_of_zero_holds_the_whole_day_and_one_holds_nothing(env) -> None:
    queue, deps, export = env
    source = LocalExportSource(export)

    held = await run_day(
        DAY,
        source=source,
        sessionmaker=deps.sessionmaker,
        queue=queue,
        day_share=_share(0.0),
        now=NOON,
    )
    assert (held.enqueued, held.deferred) == (2, 2)

    # Повторный прогон жребий не перебрасывает: заведённое задание он не трогает,
    # даже если доля с тех пор стала «всё сразу».
    again = await run_day(
        DAY,
        source=source,
        sessionmaker=deps.sessionmaker,
        queue=queue,
        day_share=_share(1.0),
        now=NOON,
    )
    assert (again.enqueued, again.deferred) == (0, 0)
    assert [job["held"] for job in await _ingest_jobs(deps)] == [True, True]


async def test_the_priority_of_a_run_puts_a_lesser_source_behind(env) -> None:
    """`--priority`: продажи идут после поддержки, а отложенное — после свежего."""
    queue, deps, export = env

    await run_day(
        DAY,
        source=LocalExportSource(export),
        sessionmaker=deps.sessionmaker,
        queue=queue,
        priority=20,
        day_share=_share(0.5),
        now=NOON,
        rng=_Lots(0.2, 0.7),
    )

    assert [job["priority"] for job in await _ingest_jobs(deps)] == [20, 30]


async def test_a_held_call_stays_behind_fresh_ones_through_every_stage(env) -> None:
    """Срочность — свойство звонка: следующий этап наследует приоритет, а не сбрасывает его."""
    queue, deps, export = env
    await run_day(
        DAY,
        source=LocalExportSource(export),
        sessionmaker=deps.sessionmaker,
        queue=queue,
        day_share=_share(0.5),
        now=NOON,
        rng=_Lots(0.2, 0.7),
    )
    await _release(deps)
    ingest = _Recorded(JobKind.INGEST, next_kind=JobKind.TRANSCRIBE)
    transcribe = _Recorded(JobKind.TRANSCRIBE, next_kind=None)
    worker = Worker(
        queue, _registry(ingest, transcribe), deps, lanes=[JobLane.DAILY], worker_id="w"
    )

    while await worker.run_once():
        pass

    assert [job.priority for job in ingest.calls] == [0, 10]
    assert [job.priority for job in transcribe.calls] == [0, 10]
    # Свежий звонок проходит оба этапа раньше, чем отложенный начинает первый:
    # приём отложенного (10) уступает расшифровке свежего (0).
    async with deps.sessionmaker() as session:
        finished = (
            await session.execute(
                text(
                    "SELECT kind, priority FROM speech_jobs WHERE status = 'done' "
                    "ORDER BY finished_at"
                )
            )
        ).all()
    assert [tuple(row) for row in finished] == [
        ("ingest", 0),
        ("transcribe", 0),
        ("ingest", 10),
        ("transcribe", 10),
    ]


async def test_without_a_share_the_run_behaves_as_before(env) -> None:
    queue, deps, export = env

    report = await run_day(
        DAY, source=LocalExportSource(export), sessionmaker=deps.sessionmaker, queue=queue
    )

    assert report.deferred == 0
    assert [(job["priority"], job["held"], job["payload"]) for job in await _ingest_jobs(deps)] == [
        (0, False, {}),
        (0, False, {}),
    ]
