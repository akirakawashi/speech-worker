"""SQL чтения и записи результата этапа конвейера."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.dto.pipeline import TranscribeWork
from src.application.dto.pipeline_input import (
    CallForTranscription,
)
from src.application.ports.queue import ClaimedJob

_LOAD_CALL_SQL = text(
    """
    SELECT session_id, audio_status, audio_object_key, audio_channels, duration_s
      FROM speech_calls
     WHERE id = :call_id
    """
)


_DELETE_TRANSCRIPT_SQL = text("DELETE FROM speech_transcripts WHERE call_id = :call_id")


_INSERT_TRANSCRIPT_SQL = text(
    """
    INSERT INTO speech_transcripts (
        id, call_id, engine, model_name, model_digest, language, role_method,
        duration_s, segments_count, created_at
    )
    VALUES (
        gen_random_uuid(), :call_id, :engine, :model_name, :model_digest, :language,
        :role_method, :duration_s, :segments_count, now()
    )
    RETURNING id
    """
)


_INSERT_SEGMENT_SQL = text(
    """
    INSERT INTO speech_transcript_segments (
        transcript_id, call_id, seq, speaker, start_ms, end_ms, text_raw
    )
    VALUES (
        :transcript_id, :call_id, :seq, :speaker, :start_ms, :end_ms, :text_raw
    )
    """
)


class TranscribeStatements:
    """Сохранение результата в транзакции вызывающего."""

    async def store(self, session: AsyncSession, job: ClaimedJob, work: TranscribeWork) -> None:
        """Сохранить результат этапа в переданной транзакции без самостоятельного commit."""
        await session.execute(_DELETE_TRANSCRIPT_SQL, {"call_id": job.call_id})
        transcript_id = (
            await session.execute(
                _INSERT_TRANSCRIPT_SQL,
                {
                    "call_id": job.call_id,
                    "engine": work.engine,
                    "model_name": work.model_name,
                    "model_digest": None,
                    "language": work.language,
                    "role_method": work.role_method.value,
                    "duration_s": work.duration_s,
                    "segments_count": len(work.replies),
                },
            )
        ).scalar_one()

        for reply in work.replies:
            await session.execute(
                _INSERT_SEGMENT_SQL,
                {
                    "transcript_id": transcript_id,
                    "call_id": job.call_id,
                    "seq": reply.seq,
                    "speaker": reply.speaker.value,
                    "start_ms": reply.start_ms,
                    "end_ms": reply.end_ms,
                    "text_raw": reply.text,
                },
            )

        # Реплики записаны — звонок сразу находится поиском: индекс построен по
        # тексту расшифровки. Дальше только разметка темы и тональности.
        return None


async def load(session: AsyncSession, call_id: UUID) -> CallForTranscription | None:
    """Прочитать метаданные записи и вернуть DTO; None означает удалённый звонок."""
    row = (await session.execute(_LOAD_CALL_SQL, {"call_id": call_id})).mappings().first()
    return CallForTranscription.model_validate(dict(row)) if row is not None else None
