"""SQL чтения и записи результата этапа конвейера."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.dto.calls import AudioStatus, DimensionSource
from src.application.dto.pipeline import IngestWork
from src.application.dto.pipeline_input import (
    CallForIngest,
)
from src.application.ports.queue import ClaimedJob

_LOAD_CALL_SQL = text(
    """
    SELECT c.session_id, c.duration_s, c.started_at, c.audio_status,
           c.audio_object_key, c.source_snapshot, c.source, c.source_audio_url,
           p.segment AS project_segment, p.product AS project_product
      FROM speech_calls AS c
      LEFT JOIN speech_ncc_projects AS p ON p.id = c.project_id
     WHERE c.id = :call_id
    """
)


_STORE_AUDIO_SQL = text(
    """
    UPDATE speech_calls
       SET audio_status = :audio_status,
           audio_object_key = :object_key,
           audio_bytes = COALESCE(:audio_bytes, audio_bytes),
           audio_channels = COALESCE(:channels, audio_channels),
           audio_stored_at = COALESCE(audio_stored_at, now()),
           duration_s = COALESCE(:duration_s, duration_s),
           updated_at = now()
     WHERE id = :call_id
    """
)


_FORGET_AUDIO_URL_SQL = text(
    """
    UPDATE speech_calls
       SET source_audio_url = NULL
     WHERE id = :call_id
       AND source_audio_url IS NOT NULL
    """
)


_MARK_NO_AUDIO_SQL = text(
    """
    UPDATE speech_calls
       SET audio_status = :audio_status,
           updated_at = now()
     WHERE id = :call_id
    """
)


_STORE_DIMENSIONS_SQL = text(
    """
    UPDATE speech_calls
       SET segment = :segment,
           segment_source = :segment_source,
           product = :product,
           product_source = :product_source,
           updated_at = now()
     WHERE id = :call_id
    """
)


class IngestStatements:
    """Сохранение результата в транзакции вызывающего."""

    @staticmethod
    async def _store_dimensions(session: AsyncSession, job: ClaimedJob, work: IngestWork) -> None:
        """Записать сегмент и продукт, если их знает проект.

        Источник ставится `project` только вместе со значением. Пометить
        источником пустоту значило бы утверждать, что проект сказал «неизвестно»,
        тогда как он не сказал ничего, — и этап анализа, увидев `project`,
        не стал бы выводить значение из разговора.
        """
        if work.segment is None and work.product is None:
            return
        await session.execute(
            _STORE_DIMENSIONS_SQL,
            {
                "call_id": job.call_id,
                "segment": work.segment,
                "segment_source": (
                    DimensionSource.PROJECT.value if work.segment else DimensionSource.UNKNOWN.value
                ),
                "product": work.product,
                "product_source": (
                    DimensionSource.PROJECT.value if work.product else DimensionSource.UNKNOWN.value
                ),
            },
        )

    async def store(self, session: AsyncSession, job: ClaimedJob, work: IngestWork) -> None:
        """Сохранить результат этапа в переданной транзакции без самостоятельного commit."""
        await session.execute(_FORGET_AUDIO_URL_SQL, {"call_id": job.call_id})
        await self._store_dimensions(session, job, work)
        if work.outcome == IngestWork.SKIPPED_SHORT:
            return None

        if work.outcome == IngestWork.NO_AUDIO:
            await session.execute(
                _MARK_NO_AUDIO_SQL,
                {
                    "call_id": job.call_id,
                    "audio_status": AudioStatus.UNAVAILABLE_AT_SOURCE.value,
                },
            )
            return None

        await session.execute(
            _STORE_AUDIO_SQL,
            {
                "call_id": job.call_id,
                "audio_status": AudioStatus.STORED.value,
                "object_key": work.object_key,
                "audio_bytes": work.audio_bytes,
                "channels": work.channels,
                "duration_s": work.duration_s,
            },
        )
        return None


async def load(session: AsyncSession, call_id: UUID) -> CallForIngest | None:
    """Прочитать метаданные записи и вернуть DTO; None означает удалённый звонок."""
    row = (await session.execute(_LOAD_CALL_SQL, {"call_id": call_id})).mappings().first()
    return CallForIngest.model_validate(dict(row)) if row is not None else None
