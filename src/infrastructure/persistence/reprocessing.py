"""Transactional pipeline replay. The caller owns commit, rollback and audit."""

from __future__ import annotations

import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.application.dto.calls import JobKind, JobLane
from src.application.ports.queue import NewJob, ReprocessingConflict
from src.infrastructure.persistence.settings_store import read_setting
from src.infrastructure.queue.postgres import PostgresJobQueue

#: `topic_id` и `ai_topic_*` — разметка прежним справочником тем, до SP-310. Её
#: больше никто не пишет и не читает, и переобработка её не стирает: это история
#: разметки, а новая ляжет в `ai_node_id`.
_RESET_CALL_SQL = text(
    """
    UPDATE speech_calls
       SET processing_status = 'queued', completed_at = NULL, analysis_status = 'pending',
           sentiment = NULL, summary = NULL,
           ai_node_id = NULL, ai_suggestion = NULL, ai_suggestion_node_id = NULL,
           call_reason = NULL, call_reason_code = NULL,
           segment = CASE WHEN segment_source = 'project' THEN segment ELSE NULL END,
           segment_source = CASE WHEN segment_source = 'project' THEN 'project' ELSE 'unknown' END,
           product = CASE WHEN product_source = 'project' THEN product ELSE NULL END,
           product_source = CASE WHEN product_source = 'project' THEN 'project' ELSE 'unknown' END,
           updated_at = now()
     WHERE id = :call_id
    """
)

_CLEAR_LLM_SPEAKERS_SQL = text(
    """
    UPDATE speech_transcript_segments SET speaker = 'unknown'
     WHERE call_id = :call_id AND transcript_id IN (
         SELECT id FROM speech_transcripts
          WHERE call_id = :call_id AND role_method = 'llm_attribution'
     )
    """
)
_CLEAR_LLM_METHOD_SQL = text(
    "UPDATE speech_transcripts SET role_method = 'none' "
    "WHERE call_id = :call_id AND role_method = 'llm_attribution'"
)
#: A script evaluation was made on those guesses, and the transcript is not
#: rewritten when only roles change: without this it would outlive them. Evaluations made on
#: measured roles stay. The rule is in `src.infrastructure.persistence.quality_transcript`.
_DROP_LLM_EVALUATIONS_SQL = text(
    """
    DELETE FROM speech_call_evaluations
     WHERE call_id = :call_id AND EXISTS (
         SELECT 1 FROM speech_transcripts
          WHERE call_id = :call_id AND role_method = 'llm_attribution'
     )
    """
)


async def reprocess(
    session: AsyncSession,
    call_id: uuid.UUID,
    kind: JobKind,
    *,
    retry_job_id: uuid.UUID | None = None,
    lane: JobLane = JobLane.DAILY,
) -> uuid.UUID:
    """Restart without publishing a job before stale outputs have been cleared.

    Lock/check the entire pipeline before touching any result. The queue method
    uses only this session; the factory is needed by its other port operations.
    No inner commit: the route's audit must succeed in this transaction too.

    `lane`: the admin's button replays in `daily`; the bulk re-analysis of
    history (`src.presentation.cli.reanalyze`) goes to `backfill`, behind the daily batch.
    """
    attempts = await read_setting(session, "max_attempts")
    queue = PostgresJobQueue(async_sessionmaker(bind=session.bind))
    job_id = await queue.reprocess(
        session,
        NewJob(
            call_id=call_id,
            kind=kind,
            lane=lane,
            max_attempts=int(attempts[kind.value]),
            lease_seconds=900 if kind is JobKind.TRANSCRIBE else 300,
        ),
        retry_job_id=retry_job_id,
    )
    params = {"call_id": call_id}
    await session.execute(_RESET_CALL_SQL, params)
    if kind in (JobKind.INGEST, JobKind.TRANSCRIBE):
        # A new transcription replaces all segments anyway. Remove the obsolete
        # text now rather than exposing it until a successful store.
        await session.execute(
            text("DELETE FROM speech_transcripts WHERE call_id = :call_id"), params
        )
    else:
        # Model guesses may be reassigned; channel measurements must survive.
        await session.execute(_CLEAR_LLM_SPEAKERS_SQL, params)
        # Both read `role_method`, so both run before it is cleared.
        await session.execute(_DROP_LLM_EVALUATIONS_SQL, params)
        await session.execute(_CLEAR_LLM_METHOD_SQL, params)
    return job_id


async def retry(session: AsyncSession, job_id: uuid.UUID) -> tuple[uuid.UUID, JobKind]:
    """Resolve the call without locking the job first (consistent lock order)."""
    row = (
        (
            await session.execute(
                text("SELECT call_id, kind FROM speech_jobs WHERE id = :job_id"),
                {"job_id": job_id},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise ReprocessingConflict("job_is_not_failed_or_dead")
    call_id, kind = uuid.UUID(str(row["call_id"])), JobKind(row["kind"])
    # Eligibility and the original ID are checked again under the pipeline locks.
    await reprocess(session, call_id, kind, retry_job_id=job_id)
    return call_id, kind
