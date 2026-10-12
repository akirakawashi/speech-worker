"""SQL-адаптер детерминированных выборок сравнения моделей."""

import uuid
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.application.dto.calls import AnalysisStatus, CallSource
from src.application.dto.classifier import NodePaths as PathsSnapshot
from src.application.dto.comparison import Sampled
from src.application.dto.pipeline_input import AnalysisSegment
from src.application.dto.reasons import CallReason
from src.infrastructure.persistence.classifier import NodePaths

_SAMPLE_SQL = text(
    """
    SELECT c.id, c.operator_node_id, c.ai_node_id, c.sentiment, c.call_reason_code
      FROM speech_calls AS c
      JOIN speech_transcripts AS t ON t.call_id = c.id
     WHERE c.started_at >= :date_from
       AND c.started_at <  :date_to
       AND c.analysis_status = :done
       AND c.operator_node_id IS NOT NULL
       AND c.ai_node_id IS NOT NULL
     ORDER BY md5(CAST(c.id AS text))
     LIMIT :limit
    """
)

_REASON_SAMPLE_SQL = text(
    """
    SELECT c.id, c.call_reason_code
      FROM speech_calls AS c
      JOIN speech_transcripts AS t ON t.call_id = c.id
     WHERE c.started_at >= :date_from
       AND c.started_at <  :date_to
       AND c.source = :source
       AND c.call_reason_code IS NOT NULL
       AND EXISTS (SELECT 1 FROM speech_transcript_segments AS s WHERE s.call_id = c.id)
     ORDER BY md5(CAST(c.id AS text))
     LIMIT :limit
    """
)

_REASON_CODES_SQL = text(
    "SELECT code, label, hint FROM speech_call_reasons WHERE is_active ORDER BY sort_order, code"
)

_REASON_SEGMENTS_SQL = text(
    "SELECT seq, speaker, text_raw FROM speech_transcript_segments "
    "WHERE call_id = :call_id ORDER BY seq"
)


async def sample_calls(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    date_from: datetime,
    date_to: datetime,
    limit: int,
) -> list[Sampled]:
    """Прочитать детерминированную выборку звонков с готовым анализом."""
    async with sessionmaker() as session:
        rows = (
            await session.execute(
                _SAMPLE_SQL,
                {
                    "date_from": date_from,
                    "date_to": date_to,
                    "done": AnalysisStatus.DONE.value,
                    "limit": limit,
                },
            )
        ).mappings()
        return [
            Sampled(
                call_id=uuid.UUID(str(row["id"])),
                operator_node_id=uuid.UUID(str(row["operator_node_id"])),
                ai_node_id=uuid.UUID(str(row["ai_node_id"])),
                sentiment=str(row["sentiment"]) if row["sentiment"] is not None else None,
                reason_code=(
                    str(row["call_reason_code"]) if row["call_reason_code"] is not None else None
                ),
            )
            for row in rows
        ]


async def sample_reason_calls(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    date_from: datetime,
    date_to: datetime,
    source: CallSource,
    limit: int,
) -> list[tuple[uuid.UUID, str]]:
    """Прочитать детерминированную выборку звонков с заполненным кодом причины."""
    async with sessionmaker() as session:
        rows = (
            await session.execute(
                _REASON_SAMPLE_SQL,
                {
                    "date_from": date_from,
                    "date_to": date_to,
                    "source": source.value,
                    "limit": limit,
                },
            )
        ).all()
    return [(uuid.UUID(str(row[0])), str(row[1])) for row in rows]


class PostgresComparisonRepository:
    """Читать исходные данные через короткие сессии, без изменения звонков."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        """Принять фабрику сессий базы речи."""
        self._sessions = sessions

    async def sample_calls(
        self, *, date_from: datetime, date_to: datetime, limit: int
    ) -> list[Sampled]:
        """Прочитать детерминированную выборку готового анализа."""
        return await sample_calls(self._sessions, date_from=date_from, date_to=date_to, limit=limit)

    async def sample_reason_calls(
        self, *, date_from: datetime, date_to: datetime, source: CallSource, limit: int
    ) -> list[tuple[uuid.UUID, str]]:
        """Прочитать детерминированную выборку заполненных причин."""
        return await sample_reason_calls(
            self._sessions, date_from=date_from, date_to=date_to, source=source, limit=limit
        )

    async def reasons(self) -> tuple[CallReason, ...]:
        """Прочитать включённый справочник причин в DTO."""
        async with self._sessions() as session:
            rows = (await session.execute(_REASON_CODES_SQL)).mappings()
            return tuple(CallReason.model_validate(dict(row)) for row in rows)

    async def segments(self, call_id: uuid.UUID) -> tuple[AnalysisSegment, ...]:
        """Прочитать реплики в DTO и закрыть сессию."""
        async with self._sessions() as session:
            rows = (await session.execute(_REASON_SEGMENTS_SQL, {"call_id": call_id})).mappings()
            return tuple(AnalysisSegment.model_validate(dict(row)) for row in rows)

    async def paths(self) -> PathsSnapshot:
        """Прочитать независимый от сессии снимок дерева."""
        async with self._sessions() as session:
            return await NodePaths.load(session)
