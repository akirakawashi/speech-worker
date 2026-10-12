"""Постановка старых звонков на анализ с продолжением после занятых звонков."""

from datetime import datetime

from src.application.dto.calls import JobLane
from src.application.dto.reanalyze import ReanalyzeReport, ReanalyzeSelection
from src.application.ports.queue import ReprocessingCallNotFound, ReprocessingConflict
from src.application.ports.reanalyze import ReanalyzeRepository


async def reanalyze(
    repository: ReanalyzeRepository,
    *,
    date_from: datetime,
    date_to: datetime,
    apply: bool,
    limit: int | None = None,
    lane: JobLane = JobLane.BACKFILL,
    missing: str = "path",
) -> ReanalyzeReport:
    """Найти звонки, при apply повторно поставить анализ; вернуть сводку пропусков."""
    selection = ReanalyzeSelection(
        date_from=date_from, date_to=date_to, missing=missing, limit=limit
    )
    call_ids = await repository.candidates(selection)
    if not apply:
        return ReanalyzeReport(candidates=len(call_ids), missing=missing)

    queued = busy = gone = 0
    for call_id in call_ids:
        # Своя транзакция на звонок: занятый конвейер одного звонка не откатывает
        # остальные, а прерванная команда оставляет поставленное поставленным.
        try:
            await repository.enqueue(call_id, lane)
        except ReprocessingConflict:
            busy += 1
        except ReprocessingCallNotFound:
            gone += 1
        else:
            queued += 1
    return ReanalyzeReport(
        candidates=len(call_ids),
        queued=queued,
        busy=busy,
        gone=gone,
        applied=True,
        missing=missing,
    )
