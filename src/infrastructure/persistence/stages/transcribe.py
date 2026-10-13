"""Прочитать метаданные сохранённого аудио; расшифровку записывает бэкенд."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.dto.pipeline_input import (
    CallForTranscription,
)

_LOAD_CALL_SQL = text(
    """
    SELECT session_id, audio_status, audio_object_key, audio_channels, duration_s
      FROM speech_calls
     WHERE id = :call_id
    """
)


async def load(session: AsyncSession, call_id: UUID) -> CallForTranscription | None:
    """Прочитать метаданные записи и вернуть DTO; None означает удалённый звонок."""
    row = (await session.execute(_LOAD_CALL_SQL, {"call_id": call_id})).mappings().first()
    return CallForTranscription.model_validate(dict(row)) if row is not None else None
