"""SQL-адаптер очистки и атомарного аудита удалённых записей."""

from collections.abc import Sequence
from typing import Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.application.dto.calls import AudioStatus
from src.application.dto.retention import AudioRetentionStats, ExpiredRecording, RetentionPolicy
from src.infrastructure.persistence.audit import AuditActor, record_action
from src.infrastructure.persistence.settings_store import read_positive_int
from src.infrastructure.retention.cleanup import count_expired, prune

PRUNE_ACTOR: Final[AuditActor] = AuditActor(sub="system:speech-prune", name="Удаление по сроку")

_EXPIRED_AUDIO_SQL = text(
    """
    SELECT id, session_id, started_at, audio_object_key, audio_bytes
      FROM speech_calls
     WHERE audio_status = :stored
       AND started_at < now() - make_interval(days => :days)
     ORDER BY started_at
     LIMIT :limit
    """
)

_EXPIRED_AUDIO_STATS_SQL = text(
    """
    SELECT count(*) AS calls,
           coalesce(sum(audio_bytes), 0) AS bytes,
           min(started_at) AS oldest
      FROM speech_calls
     WHERE audio_status = :stored
       AND started_at < now() - make_interval(days => :days)
    """
)

_MARK_DELETED_SQL = text(
    """
    UPDATE speech_calls
       SET audio_status = :deleted,
           audio_object_key = NULL,
           audio_deleted_at = now(),
           updated_at = now()
     WHERE id = ANY(CAST(:ids AS uuid[]))
    """
)


async def audio_stats(session: AsyncSession, *, days: int) -> AudioRetentionStats:
    """Посчитать просроченные записи и их объём; вернуть DTO статистики."""
    row = (
        (
            await session.execute(
                _EXPIRED_AUDIO_STATS_SQL, {"stored": AudioStatus.STORED.value, "days": days}
            )
        )
        .mappings()
        .one()
    )
    return AudioRetentionStats.model_validate(dict(row))


async def expired_audio(
    session: AsyncSession, *, days: int, limit: int
) -> tuple[ExpiredRecording, ...]:
    """Прочитать старейшие просроченные записи заданной порцией; вернуть DTO."""
    rows = await session.execute(
        _EXPIRED_AUDIO_SQL,
        {"stored": AudioStatus.STORED.value, "days": days, "limit": limit},
    )
    return tuple(ExpiredRecording.model_validate(dict(row)) for row in rows.mappings())


class PostgresRetentionPersistence:
    """Записать статусы и аудит на переданной сессии после удаления объекта."""

    def __init__(self, session: AsyncSession) -> None:
        """Принять сессию для атомарной записи подтверждений удаления."""
        self._session = session

    async def mark_deleted(self, rows: Sequence[ExpiredRecording]) -> None:
        """Пометить звонки, записать события и завершить общую транзакцию."""
        if not rows:
            return
        await self._session.execute(
            _MARK_DELETED_SQL,
            {"deleted": AudioStatus.DELETED_BY_RETENTION.value, "ids": [row.id for row in rows]},
        )
        for row in rows:
            await record_action(
                self._session,
                PRUNE_ACTOR,
                "audio.retention_delete",
                call_id=row.id,
                payload={
                    "session_id": row.session_id,
                    "object_key": row.audio_object_key,
                    "bytes": int(row.audio_bytes or 0),
                },
            )
        await self._session.commit()


class PostgresRetentionRepository:
    """Открывать короткие сессии для чтения и записи очистки."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        """Принять фабрику сессий базы речи."""
        self._sessions = sessions

    async def retention_days(self) -> int:
        """Прочитать срок хранения из настроек базы."""
        async with self._sessions() as session:
            return await read_positive_int(session, "retention_days")

    async def audio_stats(self, *, days: int) -> AudioRetentionStats:
        """Посчитать просроченные звонки и объём их записей."""
        async with self._sessions() as session:
            return await audio_stats(session, days=days)

    async def expired_audio(self, *, days: int, limit: int) -> tuple[ExpiredRecording, ...]:
        """Вернуть старейшие записи ограниченной порцией."""
        async with self._sessions() as session:
            return await expired_audio(session, days=days, limit=limit)

    async def mark_deleted(self, rows: Sequence[ExpiredRecording]) -> None:
        """Создать сессию и атомарно записать статусы вместе с аудитом."""
        async with self._sessions() as session:
            await PostgresRetentionPersistence(session).mark_deleted(rows)

    async def count_expired(self, policy: RetentionPolicy) -> int:
        """Посчитать завершённые строки за пределами политики хранения."""
        async with self._sessions() as session:
            return await count_expired(session, policy)

    async def prune(self, policy: RetentionPolicy, *, batch_size: int) -> int:
        """Удалить строки порциями с отдельным commit каждой порции."""
        async with self._sessions() as session:
            return await prune(session, policy, batch_size=batch_size)
