"""Буфер доставки в существующем speech_jobs.payload; аудио и итоговые таблицы не пишет."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.application.dto.transcription_delivery import TranscriptionDeliveryRequest

_SAVE_SQL = text(
    """
    UPDATE speech_jobs AS job
       SET payload = jsonb_set(job.payload, '{_transcription_delivery}', CAST(:command AS jsonb))
      FROM speech_calls AS call
     WHERE job.id = :job_id AND job.call_id = :call_id AND call.id = job.call_id
       AND job.kind = 'transcribe' AND job.status = 'running'
       AND job.locked_by = :worker_id AND job.attempts = :attempt
       AND job.payload->>'_claim_token' = :claim_token
       AND job.locked_at + make_interval(secs => job.lease_seconds) > clock_timestamp()
       AND call.audio_status = 'stored' AND call.audio_object_key = :object_key
       AND call.audio_channels IS NOT DISTINCT FROM CAST(:channels AS integer)
       AND (
           NOT (job.payload ? '_transcription_delivery')
           OR job.payload->'_transcription_delivery'->'claim'->>'claim_token' <> :claim_token
           OR job.payload->'_transcription_delivery' = CAST(:command AS jsonb)
       )
    RETURNING job.id
    """
)


class PostgresTranscriptionDeliveryBuffer:
    """Хранить максимум один неподтверждённый DTO на задание до commit бэкенда."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        """Внедрить фабрику коротких транзакций в event loop воркера."""
        self._sessions = sessions

    async def save(self, command: TranscriptionDeliveryRequest) -> bool:
        """Зафиксировать DTO до HTTP; False при потере аренды, смене аудио или подмене повтора."""
        claim = command.claim
        async with self._sessions() as session, session.begin():
            saved = await session.scalar(
                _SAVE_SQL,
                {
                    "job_id": claim.job_id,
                    "call_id": claim.call_id,
                    "worker_id": claim.worker_id,
                    "attempt": claim.attempt,
                    "claim_token": str(claim.claim_token),
                    "object_key": command.source.object_key,
                    "channels": command.source.channels,
                    "command": command.model_dump_json(),
                },
            )
        return saved is not None
