"""PostgreSQL-адаптер пакетного заполнения причин без долгих транзакций."""

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.application.dto.pipeline_input import AnalysisSegment
from src.application.dto.reasons import CallReason, ReasonAnswer, ReasonCandidates, ReasonSelection

_CANDIDATES_SQL = text(
    """
    SELECT c.id
      FROM speech_calls AS c
      JOIN speech_transcripts AS t ON t.call_id = c.id
     WHERE c.started_at >= :date_from
       AND c.started_at <  :date_to
       AND c.call_reason_code IS NULL
       AND EXISTS (SELECT 1 FROM speech_transcript_segments AS s WHERE s.call_id = c.id)
     ORDER BY c.started_at DESC, c.id DESC
     LIMIT :limit
    """
)

_RECENTLY_TRANSCRIBED_SQL = text(
    """
    SELECT c.id
      FROM speech_calls AS c
      JOIN speech_transcripts AS t ON t.call_id = c.id
     WHERE t.created_at >= :transcribed_since
       AND c.call_reason_code IS NULL
       AND EXISTS (SELECT 1 FROM speech_transcript_segments AS s WHERE s.call_id = c.id)
     ORDER BY c.started_at DESC, c.id DESC
     LIMIT :limit
    """
)

_REASONS_SQL = text(
    "SELECT code, label, hint FROM speech_call_reasons WHERE is_active ORDER BY sort_order, code"
)

_SEGMENTS_SQL = text(
    "SELECT seq, speaker, text_raw FROM speech_transcript_segments "
    "WHERE call_id = :call_id ORDER BY seq"
)

_STORE_SQL = text(
    """
    UPDATE speech_calls
       SET call_reason = :call_reason, call_reason_code = :call_reason_code, updated_at = now()
     WHERE id = :call_id AND call_reason_code IS NULL
    """
)


class PostgresReasonRepository:
    """Открывать отдельную сессию на чтение или запись, без ожидания модели в транзакции."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        """Принять фабрику сессий базы речи."""
        self._sessions = sessions

    async def candidates(self, selection: ReasonSelection) -> ReasonCandidates:
        """Выполнить безопасный отбор и вернуть DTO звонков и справочника."""
        if selection.transcribed_since is not None:
            query = _RECENTLY_TRANSCRIBED_SQL
            params = {"transcribed_since": selection.transcribed_since, "limit": selection.limit}
        else:
            query = _CANDIDATES_SQL
            params = {
                "date_from": selection.date_from,
                "date_to": selection.date_to,
                "limit": selection.limit,
            }
        async with self._sessions() as session:
            ids = tuple(UUID(str(row[0])) for row in (await session.execute(query, params)).all())
            reasons = tuple(
                CallReason.model_validate(dict(row))
                for row in (await session.execute(_REASONS_SQL)).mappings()
            )
        return ReasonCandidates(call_ids=ids, reasons=reasons)

    async def segments(self, call_id: UUID) -> tuple[AnalysisSegment, ...]:
        """Прочитать реплики и закрыть сессию до вызова модели."""
        async with self._sessions() as session:
            rows = (await session.execute(_SEGMENTS_SQL, {"call_id": call_id})).mappings()
            return tuple(AnalysisSegment.model_validate(dict(row)) for row in rows)

    async def save(self, call_id: UUID, answer: ReasonAnswer) -> None:
        """Атомарно заполнить пустую причину; существующую оставить без изменения."""
        async with self._sessions() as session, session.begin():
            await session.execute(
                _STORE_SQL,
                {"call_id": call_id, "call_reason": answer.text, "call_reason_code": answer.code},
            )
