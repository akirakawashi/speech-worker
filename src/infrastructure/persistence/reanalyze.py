"""SQL-отбор и атомарная повторная постановка звонков на анализ."""

from typing import Final
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.application.dto.calls import JobKind, JobLane, JobStatus
from src.application.dto.reanalyze import ReanalyzeSelection
from src.infrastructure.persistence.reprocessing import reprocess

MISSING: Final[dict[str, str]] = {
    "path": "c.ai_node_id IS NULL",
    "reason": "c.analysis_status = 'done' AND c.call_reason_code IS NULL",
}

_CANDIDATES_SQL = """
    SELECT c.id
      FROM speech_calls AS c
      JOIN speech_transcripts AS t ON t.call_id = c.id
     WHERE c.started_at >= :date_from
       AND c.started_at <  :date_to
       AND {missing}
       AND NOT EXISTS (
            SELECT 1 FROM speech_jobs AS j
             WHERE j.call_id = c.id AND j.status = ANY(CAST(:active AS text[]))
       )
     ORDER BY c.started_at DESC, c.id DESC
     LIMIT :limit
"""


class PostgresReanalyzeRepository:
    """Управлять короткими сессиями повторного анализа."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        """Принять фабрику сессий базы речи."""
        self._sessions = sessions

    async def candidates(self, selection: ReanalyzeSelection) -> tuple[UUID, ...]:
        """Подставить только разрешённое SQL-условие и вернуть UUID кандидатов."""
        params = selection.model_dump()
        params["active"] = [JobStatus.QUEUED.value, JobStatus.RUNNING.value]
        async with self._sessions() as session:
            found = await session.execute(
                text(_CANDIDATES_SQL.format(missing=MISSING[selection.missing])), params
            )
            return tuple(UUID(str(row[0])) for row in found.all())

    async def enqueue(self, call_id: UUID, lane: JobLane) -> None:
        """Атомарно сбросить анализ и поставить задание, сохранив конфликтную проверку."""
        async with self._sessions() as session, session.begin():
            await reprocess(session, call_id, JobKind.ANALYZE, lane=lane)
