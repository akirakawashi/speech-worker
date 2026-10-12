"""Протокол очереди на настоящем Postgres.

Проверяется не «метод возвращает значение», а шесть способов, которыми очередь
могла бы потерять или продублировать работу. Каждый из них — свойство базы, а
не Python: `SKIP LOCKED`, `ON CONFLICT`, условие `locked_by` в `UPDATE`.
Проверить их на подставном объекте нельзя — подставной объект как раз и вернёт
то, что мы ожидаем.

Нужен `SPEECH_POSTGRES_TEST_URL` с применённой схемой Speech Analytics.
Соглашение то же, что у `test_workflow_terminal_race_postgres.py`.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.application.ports.queue import NewJob
from src.domain.calls.contracts import JobErrorClass, JobKind, JobLane, JobStatus
from src.infrastructure.queue.postgres import PostgresJobQueue, acquire_singleton
from tests.speech_db_support import speech_test_database_url

_TEST_DATABASE_URL = speech_test_database_url()
pytestmark = pytest.mark.skipif(
    not _TEST_DATABASE_URL,
    reason="SPEECH_POSTGRES_TEST_URL is required for the queue protocol proof",
)


@pytest.fixture()
async def queue() -> AsyncIterator[PostgresJobQueue]:
    assert _TEST_DATABASE_URL is not None
    engine = create_async_engine(_TEST_DATABASE_URL)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session, session.begin():
        await session.execute(text("DELETE FROM speech_jobs"))
        await session.execute(text("DELETE FROM speech_calls"))
    yield PostgresJobQueue(sessions, engine=engine)
    async with sessions() as session, session.begin():
        await session.execute(text("DELETE FROM speech_jobs"))
        await session.execute(text("DELETE FROM speech_calls"))
    await engine.dispose()


async def _make_call(queue: PostgresJobQueue, session_id: str | None = None) -> uuid.UUID:
    call_id = uuid.uuid4()
    now = datetime.now(UTC)
    async with queue._sessionmaker() as session, session.begin():  # noqa: SLF001 - фикстура теста
        await session.execute(
            text(
                "INSERT INTO speech_calls (id, session_id, direction, started_at, ended_at, "
                "duration_s, audio_status, processing_status, analysis_status, source_snapshot, "
                "first_seen_at, created_at, updated_at) "
                "VALUES (:id, :sid, 'inbound', :now, :now, 120, 'pending', 'new', 'pending', "
                "'{}', :now, :now, :now)"
            ),
            {
                "id": call_id,
                "sid": session_id or f"node_1_domain_1_nauss_1_1_{uuid.uuid4().hex}",
                "now": now,
            },
        )
    return call_id


async def _enqueue(
    queue: PostgresJobQueue,
    call_id: uuid.UUID,
    *,
    kind: JobKind = JobKind.INGEST,
    lane: JobLane = JobLane.DAILY,
    max_attempts: int = 3,
) -> uuid.UUID | None:
    async with queue._sessionmaker() as session, session.begin():  # noqa: SLF001
        return await queue.enqueue(
            session, NewJob(call_id=call_id, kind=kind, lane=lane, max_attempts=max_attempts)
        )


async def test_the_same_stage_is_never_queued_twice(queue: PostgresJobQueue) -> None:
    """Повторный суточный прогон за ту же дату — обычное дело, а не ошибка."""
    call_id = await _make_call(queue)

    first = await _enqueue(queue, call_id)
    second = await _enqueue(queue, call_id)

    assert first is not None
    assert second is None, "второе задание того же этапа не создаётся"


async def test_two_workers_never_claim_the_same_job(queue: PostgresJobQueue) -> None:
    """`SKIP LOCKED`: конкурент пропускает заблокированную строку, а не ждёт."""
    for _ in range(6):
        await _enqueue(queue, await _make_call(queue))

    claims = await asyncio.gather(*(queue.claim([JobLane.DAILY], f"worker-{n}") for n in range(6)))

    got = [c for c in claims if c is not None]
    assert len(got) == 6
    assert len({c.id for c in got}) == 6, "одно задание досталось двум воркерам"


async def test_daily_outranks_backfill(queue: PostgresJobQueue) -> None:
    """Бэкфилл не должен вытеснять пакет, обязанный быть готовым к утру."""
    backfill_call = await _make_call(queue)
    await _enqueue(queue, backfill_call, lane=JobLane.BACKFILL)
    daily_call = await _make_call(queue)
    await _enqueue(queue, daily_call, lane=JobLane.DAILY)

    claimed = await queue.claim([JobLane.BACKFILL, JobLane.DAILY], "mixed-worker")

    assert claimed is not None
    assert claimed.call_id == daily_call, "суточное задание обязано идти первым"


async def test_a_daily_worker_does_not_touch_backfill(queue: PostgresJobQueue) -> None:
    await _enqueue(queue, await _make_call(queue), lane=JobLane.BACKFILL)

    assert await queue.claim([JobLane.DAILY], "daily-only") is None


async def test_attempts_grow_on_claim_not_on_failure(queue: PostgresJobQueue) -> None:
    """Задание, взятое и потерянное вместе с воркером, тоже стоило попытки.

    Иначе воркер, стабильно убивающий себя на одном звонке, повторял бы его
    вечно: попытка засчитывалась бы только при аккуратном отказе, которого в
    этом сценарии не происходит.
    """
    call_id = await _make_call(queue)
    await _enqueue(queue, call_id)

    claimed = await queue.claim([JobLane.DAILY], "worker-1")

    assert claimed is not None
    assert claimed.attempts == 1


async def test_an_expired_lease_returns_the_job(queue: PostgresJobQueue) -> None:
    """Упавший воркер не уносит задание с собой."""
    call_id = await _make_call(queue)
    await _enqueue(queue, call_id)
    claimed = await queue.claim([JobLane.DAILY], "doomed-worker")
    assert claimed is not None

    # Аренда истекла: воркера больше нет, подтверждать её некому.
    async with queue._sessionmaker() as session, session.begin():  # noqa: SLF001
        await session.execute(
            text("UPDATE speech_jobs SET locked_at = now() - interval '1 hour' WHERE id = :id"),
            {"id": claimed.id},
        )

    returned = await queue.requeue_expired()

    assert len(returned) == 1
    assert returned[0].status is JobStatus.QUEUED, "попытки ещё не исчерпаны"
    assert returned[0].call_id == call_id
    assert await queue.claim([JobLane.DAILY], "worker-2") is not None


async def test_a_zombie_worker_cannot_complete_a_job_taken_from_it(
    queue: PostgresJobQueue,
) -> None:
    """Самая тонкая гарантия протокола.

    Воркер потерял аренду, задание переиграл другой — и первый всё-таки
    возвращается со своим результатом. Без условия `locked_by` в `UPDATE` он
    затёр бы чужую, актуальную работу своей устаревшей.
    """
    call_id = await _make_call(queue)
    await _enqueue(queue, call_id)
    zombie = await queue.claim([JobLane.DAILY], "zombie")
    assert zombie is not None

    async with queue._sessionmaker() as session, session.begin():  # noqa: SLF001
        await session.execute(
            text("UPDATE speech_jobs SET locked_at = now() - interval '1 hour' WHERE id = :id"),
            {"id": zombie.id},
        )
    await queue.requeue_expired()
    winner = await queue.claim([JobLane.DAILY], "winner")
    assert winner is not None

    async with queue._sessionmaker() as session, session.begin():  # noqa: SLF001
        completed_by_zombie = await queue.complete(session, zombie.id, "zombie")

    assert completed_by_zombie is False, "зомби завершил чужую попытку"

    async with queue._sessionmaker() as session, session.begin():  # noqa: SLF001
        assert await queue.complete(session, winner.id, "winner") is True


async def test_heartbeat_fails_once_the_job_is_gone(queue: PostgresJobQueue) -> None:
    call_id = await _make_call(queue)
    await _enqueue(queue, call_id)
    claimed = await queue.claim([JobLane.DAILY], "worker-1")
    assert claimed is not None

    assert await queue.heartbeat(claimed.id, "worker-1") is True
    assert await queue.heartbeat(claimed.id, "someone-else") is False


async def test_failure_requeues_until_attempts_run_out(queue: PostgresJobQueue) -> None:
    call_id = await _make_call(queue)
    await _enqueue(queue, call_id, max_attempts=2)

    first = await queue.claim([JobLane.DAILY], "worker-1")
    assert first is not None
    assert await queue.fail(first.id, "worker-1", JobErrorClass.TIMEOUT) is JobStatus.QUEUED

    # Задержка повтора отодвинула задание в будущее — сразу его не взять.
    assert await queue.claim([JobLane.DAILY], "worker-1") is None
    async with queue._sessionmaker() as session, session.begin():  # noqa: SLF001
        await session.execute(text("UPDATE speech_jobs SET run_after = now()"))

    second = await queue.claim([JobLane.DAILY], "worker-1")
    assert second is not None
    assert await queue.fail(second.id, "worker-1", JobErrorClass.TIMEOUT) is JobStatus.DEAD
    assert await queue.claim([JobLane.DAILY], "worker-1") is None, "dead не берут заново"


async def test_a_deterministic_failure_is_not_retried(queue: PostgresJobQueue) -> None:
    """Невалидный ответ модели повторять незачем: он будет тем же самым."""
    call_id = await _make_call(queue)
    await _enqueue(queue, call_id, kind=JobKind.ANALYZE, max_attempts=5)
    claimed = await queue.claim([JobLane.DAILY], "worker-1")
    assert claimed is not None

    status = await queue.fail(claimed.id, "worker-1", JobErrorClass.INVALID_OUTPUT)

    assert status is JobStatus.DEAD, "детерминированный отказ не должен повторяться"


async def test_snapshot_reports_depth_and_the_oldest_wait(queue: PostgresJobQueue) -> None:
    """Глубина очереди и возраст самого старого ожидания.

    Возраст проверяется величиной, а не знаком: `age >= timedelta(0)` верно для
    любого ответа, включая ноль, и такая проверка прошла бы на метрике, которая
    всегда показывает «только что» — то есть ровно тогда, когда срыв ночной
    обработки и надо заметить.

    Одному заданию время создания отодвигается на час назад: тогда видно и то,
    что возраст измеряется, и то, что берётся **самое старое**, а не любое.
    """
    for _ in range(3):
        await _enqueue(queue, await _make_call(queue))
    await _enqueue(queue, await _make_call(queue), lane=JobLane.BACKFILL)
    running = await queue.claim([JobLane.DAILY], "worker-1")
    assert running is not None
    async with queue._sessionmaker() as session, session.begin():  # noqa: SLF001
        await session.execute(
            text(
                "UPDATE speech_jobs SET created_at = now() - interval '1 hour' WHERE id = ("
                "SELECT id FROM speech_jobs WHERE status = 'queued' AND lane = 'daily' "
                "ORDER BY created_at LIMIT 1)"
            )
        )

    snapshot = await queue.snapshot()

    assert snapshot.queued(JobLane.DAILY) == 2
    assert snapshot.queued(JobLane.BACKFILL) == 1
    age = snapshot.oldest_queued_age(JobLane.DAILY)
    assert age is not None
    assert timedelta(minutes=55) < age < timedelta(minutes=65), (
        f"возраст самого старого ожидания {age} не похож на отодвинутый час"
    )


async def test_only_one_process_holds_the_daily_run_lock(queue: PostgresJobQueue) -> None:
    """Два суточных прогона одновременно — не порча данных, но двойная работа."""
    async with queue._sessionmaker() as first, first.begin():  # noqa: SLF001
        assert await acquire_singleton(first, "speech:daily-ingest") is True
        async with queue._sessionmaker() as second, second.begin():  # noqa: SLF001
            assert await acquire_singleton(second, "speech:daily-ingest") is False

    # Блокировка уровня транзакции: после её конца ключ снова свободен.
    async with queue._sessionmaker() as third, third.begin():  # noqa: SLF001
        assert await acquire_singleton(third, "speech:daily-ingest") is True


async def test_stage_slots_are_limited(queue: PostgresJobQueue) -> None:
    """Сервер Whisper один: слоты ограничивают одновременные расшифровки."""
    async with queue.stage_slot(1) as first:
        assert first is True
        async with queue.stage_slot(1) as second:
            assert second is False, "второй слот выдан там, где он один"

    # Слот освобождён вместе с контекстом.
    async with queue.stage_slot(1) as again:
        assert again is True


async def test_a_deferred_job_gets_its_attempt_back(queue: PostgresJobQueue) -> None:
    """Занятость внешнего ресурса — не отказ, и стоить попытки не должна.

    Попытка засчитывается при захвате. Не вернув её, мы получили бы задание,
    которое за `max_attempts` неудачных заходов за слотом становится `dead`, ни
    разу не дойдя до модели, — и в журнале это выглядело бы как отказ Whisper.
    """
    call_id = await _make_call(queue)
    await _enqueue(queue, call_id, kind=JobKind.TRANSCRIBE)
    claimed = await queue.claim([JobLane.DAILY], "worker-1")
    assert claimed is not None and claimed.attempts == 1

    deferred = await queue.defer(claimed.id, "worker-1", delay_seconds=0)

    assert deferred is True
    async with queue._sessionmaker() as session:  # noqa: SLF001
        row = (
            (
                await session.execute(
                    text(
                        "SELECT status, attempts, last_error_class, locked_by "
                        "FROM speech_jobs WHERE id = :id"
                    ),
                    {"id": claimed.id},
                )
            )
            .mappings()
            .one()
        )
    assert row["status"] == JobStatus.QUEUED.value
    assert row["attempts"] == 0, "потраченная попытка возвращена"
    assert row["last_error_class"] is None, "класса отказа быть не должно: отказа не было"
    assert row["locked_by"] is None

    again = await queue.claim([JobLane.DAILY], "worker-2")
    assert again is not None and again.id == claimed.id


async def test_a_worker_cannot_defer_a_job_taken_from_it(queue: PostgresJobQueue) -> None:
    """Та же защита, что у `complete`: аренда истекла — распоряжаться нечем.

    Проверяется через **настоящую** потерю аренды, а не выдуманным чужим
    именем: раньше задание захватывал `owner`, а откладывать пробовал
    `someone-else`, которого в базе не было никогда. Такой тест проходит и на
    защите, которая срабатывает только для несуществующих имён, — а нужна она
    ровно в обратном случае: аренда истекла, задание переиграл другой воркер, и
    первый возвращается и просит отложить **свою** попытку.
    """
    call_id = await _make_call(queue)
    await _enqueue(queue, call_id, kind=JobKind.TRANSCRIBE)
    stale = await queue.claim([JobLane.DAILY], "stale")
    assert stale is not None

    async with queue._sessionmaker() as session, session.begin():  # noqa: SLF001
        await session.execute(
            text("UPDATE speech_jobs SET locked_at = now() - interval '1 hour' WHERE id = :id"),
            {"id": stale.id},
        )
    await queue.requeue_expired()
    winner = await queue.claim([JobLane.DAILY], "winner")
    assert winner is not None and winner.id == stale.id

    assert await queue.defer(stale.id, "stale", delay_seconds=0) is False, (
        "воркер с истёкшей арендой отложил задание, которое ему больше не принадлежит"
    )

    # Новый владелец распоряжается заданием как обычно: отказ выше — про
    # потерянную аренду, а не про сломанный `defer`.
    assert await queue.defer(winner.id, "winner", delay_seconds=0) is True


# Задержка повтора и терминальность класса отказа переехали в
# `test_speech_queue_policy.py`. Они не нуждались в базе и здесь просто не
# выполнялись: у этого модуля `skipif` на весь файл.


async def test_a_failure_that_later_succeeded_stops_counting_as_a_failure(
    queue: PostgresJobQueue,
) -> None:
    """«Сбоев за сутки» отвечает на вопрос «что сломано», а не «что моргнуло».

    `complete` меняет статус, но `last_error_at` оставляет — история отказа
    нужна. Пока счёт шёл по одному только `last_error_at`, задание, отказавшее
    в 03:00 и прошедшее с повтора в 03:05, числилось сбоем ещё сутки. Повторы
    штатны, поэтому число было бы завышено всегда, а к постоянно ненулевому
    счётчику привыкают и перестают смотреть.

    Проверяются обе половины одним ходом: пока отказ не решён — он в счётчике;
    как только прошёл — его там нет. Односторонняя проверка прошла бы и на
    счётчике, который всегда ноль.
    """
    call_id = await _make_call(queue)
    await _enqueue(queue, call_id, max_attempts=3)

    claimed = await queue.claim([JobLane.DAILY], "worker-1")
    assert claimed is not None
    # Повторяемый отказ возвращает задание в очередь, а не в `failed`: статуса
    # `failed` очередь не пишет вовсе.
    after = await queue.fail(claimed.id, "worker-1", JobErrorClass.PROVIDER_ERROR)
    assert after is JobStatus.QUEUED

    assert (await queue.snapshot()).failed_last_24h == 1, (
        "отказ, ждущий повтора, — нерешённый: он обязан быть виден"
    )

    # Отсрочку снимаем: ждать её в тесте значило бы проверять таймер.
    async with queue._sessionmaker() as session, session.begin():  # noqa: SLF001
        await session.execute(text("UPDATE speech_jobs SET run_after = now()"))

    retried = await queue.claim([JobLane.DAILY], "worker-1")
    assert retried is not None
    async with queue._sessionmaker() as session, session.begin():  # noqa: SLF001
        assert await queue.complete(session, retried.id, "worker-1") is True

    async with queue._sessionmaker() as session:  # noqa: SLF001
        kept = await session.scalar(text("SELECT last_error_at FROM speech_jobs"))
    assert kept is not None, "история отказа не должна стираться успехом"

    assert (await queue.snapshot()).failed_last_24h == 0, (
        "задание прошло — считать его сбоем больше не за что"
    )


async def test_the_breakdown_by_class_counts_the_same_jobs_as_the_total(
    queue: PostgresJobQueue,
) -> None:
    """Итог и разбивка на одном экране обязаны описывать одно множество.

    Иначе сумма по классам не сходится с числом над ней, и разбираться в этом
    приходится ровно тогда, когда некогда.
    """
    for _ in range(2):
        call_id = await _make_call(queue)
        await _enqueue(queue, call_id, max_attempts=3)
        claimed = await queue.claim([JobLane.DAILY], "worker-1")
        assert claimed is not None
        await queue.fail(claimed.id, "worker-1", JobErrorClass.PROVIDER_ERROR)

    snapshot = await queue.snapshot()

    assert snapshot.failed_last_24h == 2
    assert sum(snapshot.errors_by_class.values()) == snapshot.failed_last_24h
