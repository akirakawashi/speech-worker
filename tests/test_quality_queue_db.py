"""Оценка и завершение аренды сохраняются вместе, статус звонка не меняется."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import text

from src.application.dto.calls import JobErrorClass, JobKind, JobLane
from src.application.dto.quality import Evaluation
from src.application.dto.quality_execution import QualityTotals, QualityWork
from src.application.pipeline.evaluate import EvaluateHandler
from src.application.pipeline.stages import StageError, StageRegistry
from src.application.ports.queue import NewJob
from src.infrastructure.persistence.evaluation import PostgresEvaluationRepository
from src.infrastructure.queue.worker import Worker, mark_call_after_dead_job
from tests.speech_db_support import speech_test_database_url
from tests.test_speech_worker_and_daily_run import _seed_day, env as env

pytestmark = pytest.mark.skipif(
    not speech_test_database_url(), reason="SPEECH_POSTGRES_TEST_URL is required"
)


@pytest.fixture
async def ready(env):
    """Создать расшифрованный звонок и единственное задание оценки в настоящей очереди."""
    queue, deps, export = env
    await _seed_day(queue, deps, export)
    sessions = deps.sessionmaker
    async with sessions() as session, session.begin():
        call_id = await session.scalar(text("SELECT id FROM speech_calls LIMIT 1"))
        await session.execute(text("DELETE FROM speech_jobs"))
        await session.execute(
            text(
                "UPDATE speech_calls SET source = 'telphin', processing_status = 'done', "
                "completed_at = :at WHERE id = :id"
            ),
            {"id": call_id, "at": datetime(2026, 1, 1, tzinfo=UTC)},
        )
        await session.execute(
            text(
                "INSERT INTO speech_transcripts (id, call_id, engine, model_name, language, "
                "role_method, segments_count, created_at) "
                "VALUES (:id, :call, 'test', 'test', 'ru', 'none', 0, now())"
            ),
            {"id": uuid4(), "call": call_id},
        )
        await queue.enqueue(
            session,
            NewJob(call_id=call_id, kind=JobKind.EVALUATE, lane=JobLane.DAILY, max_attempts=3),
        )
    snapshot = await PostgresEvaluationRepository(sessions).load_lines(call_id)
    work = QualityWork(
        snapshot=snapshot,
        model_name="backend-model",
        script_version="test-script",
        totals=QualityTotals(),
        evaluation=Evaluation(
            script_version="test-script", model_name="backend-model", script_applies=False
        ),
    )
    executor = AsyncMock()
    executor.evaluate.return_value = work
    deps.evaluation = executor
    return queue, deps, call_id, executor


def _legacy_evaluation_registry() -> StageRegistry:
    """Проверить сохранённый переходный обработчик, не регистрируя его в основном воркере."""
    registry = StageRegistry()
    registry.register(EvaluateHandler())
    return registry


async def test_result_and_job_complete_together_without_changing_call_status(ready) -> None:
    """Расшифрованный звонок остаётся готовым, completed_at не сдвигается оценкой."""
    queue, deps, call_id, _ = ready
    worker = Worker(
        queue,
        _legacy_evaluation_registry(),
        deps,
        lanes=[JobLane.DAILY],
        worker_id="quality-worker",
    )
    assert await worker.run_once()
    async with deps.sessionmaker() as session:
        assert await session.scalar(text("SELECT status FROM speech_jobs")) == "done"
        assert (
            await session.scalar(
                text("SELECT count(*) FROM speech_call_evaluations WHERE call_id = :id"),
                {"id": call_id},
            )
            == 1
        )
        call = (
            await session.execute(
                text("SELECT processing_status, completed_at FROM speech_calls WHERE id = :id"),
                {"id": call_id},
            )
        ).one()
        assert call == ("done", datetime(2026, 1, 1, tzinfo=UTC))


async def test_lost_lease_rolls_back_quality_result(ready) -> None:
    """Чужая аренда откатывает даже уже записанную в транзакции оценку."""
    queue, deps, call_id, _ = ready
    job = await queue.claim([JobLane.DAILY], "real-owner", kinds=[JobKind.EVALUATE])
    worker = Worker(
        queue,
        _legacy_evaluation_registry(),
        deps,
        lanes=[JobLane.DAILY],
        worker_id="lost-owner",
    )
    await worker._process(job)
    async with deps.sessionmaker() as session:
        assert (
            await session.scalar(
                text("SELECT count(*) FROM speech_call_evaluations WHERE call_id = :id"),
                {"id": call_id},
            )
            == 0
        )
        assert await session.scalar(text("SELECT status FROM speech_jobs")) == "running"


async def test_changed_transcript_retries_and_never_stores_stale_result(ready) -> None:
    """Переобработка во время ответа модели защищена проверкой серверного времени входа."""
    queue, deps, call_id, _ = ready
    async with deps.sessionmaker() as session, session.begin():
        await session.execute(
            text(
                "UPDATE speech_transcripts SET created_at = clock_timestamp() WHERE call_id = :id"
            ),
            {"id": call_id},
        )
    worker = Worker(
        queue,
        _legacy_evaluation_registry(),
        deps,
        lanes=[JobLane.DAILY],
        worker_id="quality-worker",
    )
    assert await worker.run_once()
    async with deps.sessionmaker() as session:
        assert await session.scalar(text("SELECT count(*) FROM speech_call_evaluations")) == 0
        assert await session.scalar(text("SELECT status FROM speech_jobs")) == "queued"
        assert (
            await session.scalar(
                text("SELECT processing_status FROM speech_calls WHERE id = :id"), {"id": call_id}
            )
            == "done"
        )


async def test_backend_totals_are_written_without_local_recalculation(ready) -> None:
    """Правило расчёта можно заменить в бэкенде, не меняя сохраняющий адаптер воркера."""
    queue, deps, call_id, executor = ready
    work = executor.evaluate.return_value
    executor.evaluate.return_value = work.model_copy(
        update={
            "evaluation": work.evaluation.model_copy(update={"script_applies": True}),
            "totals": QualityTotals(score=72, passed_count=5, stage_count=7, all_passed=False),
        }
    )
    worker = Worker(
        queue,
        _legacy_evaluation_registry(),
        deps,
        lanes=[JobLane.DAILY],
        worker_id="quality-worker",
    )
    assert await worker.run_once()
    async with deps.sessionmaker() as session:
        result = (
            await session.execute(
                text(
                    "SELECT score, passed_count, stage_count, all_passed "
                    "FROM speech_call_evaluations WHERE call_id = :id"
                ),
                {"id": call_id},
            )
        ).one()
        assert result == (72, 5, 7, False)


async def test_dead_quality_and_reaper_do_not_mark_transcription_failed(ready) -> None:
    """Исчерпанные попытки оценки не убирают готовую расшифровку из поиска."""
    queue, deps, call_id, executor = ready
    executor.evaluate.side_effect = StageError(JobErrorClass.PROVIDER_ERROR, "model unavailable")
    async with deps.sessionmaker() as session, session.begin():
        await session.execute(text("UPDATE speech_jobs SET max_attempts = 1"))
    worker = Worker(
        queue,
        _legacy_evaluation_registry(),
        deps,
        lanes=[JobLane.DAILY],
        worker_id="quality-worker",
    )
    assert await worker.run_once()
    async with deps.sessionmaker() as session, session.begin():
        await mark_call_after_dead_job(session, call_id, JobKind.EVALUATE)
        assert await session.scalar(text("SELECT status FROM speech_jobs")) == "dead"
        assert (
            await session.scalar(
                text("SELECT processing_status FROM speech_calls WHERE id = :id"), {"id": call_id}
            )
            == "done"
        )
