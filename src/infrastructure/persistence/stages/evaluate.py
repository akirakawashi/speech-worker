"""Запись оценки в транзакции исполнителя с защитой от изменения расшифровки."""

from sqlalchemy.ext.asyncio import AsyncSession

from src.application.dto.calls import JobErrorClass
from src.application.dto.quality_execution import QualityWork
from src.application.pipeline.stages import StageError
from src.application.ports.queue import ClaimedJob
from src.application.quality.evaluation import QualityError
from src.infrastructure.persistence.evaluation import (
    _check_input,
    _save_roles,
    store_evaluation,
    store_failure_result,
)
from src.infrastructure.persistence.quality_transcript import DROP_EVALUATIONS_SQL


async def store(session: AsyncSession, job: ClaimedJob, work: QualityWork) -> None:
    """Проверить снимок, записать роли и исход; коммит выполняет очередь.

    После потери аренды вся транзакция откатывается. Изменившийся вход
    отклоняется как повторяемый отказ; новая попытка прочитает свежий текст.
    """
    try:
        if work.evaluation is not None:
            await store_evaluation(
                session,
                job.call_id,
                work.evaluation,
                evaluated_at=work.snapshot.read_at,
                source_lines=work.snapshot.lines,
                totals=work.totals,
            )
            return
        role_method = await _check_input(
            session, job.call_id, work.snapshot.lines, work.snapshot.read_at
        )
        if work.roles:
            await _save_roles(session, job.call_id, work.roles, role_method)
            await session.execute(DROP_EVALUATIONS_SQL, {"call_id": job.call_id})
        assert work.failure_reason is not None
        await store_failure_result(
            session,
            job.call_id,
            evaluated_at=work.snapshot.read_at,
            model_name=work.model_name,
            script_version=work.script_version,
            reason=work.failure_reason,
        )
    except QualityError as exc:
        raise StageError(JobErrorClass.PROVIDER_ERROR, "quality input changed") from exc
