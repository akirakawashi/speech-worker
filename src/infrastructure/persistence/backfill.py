"""SQL состояния исторического приёма; бизнес-переходами управляют сценарии."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.application.dto.backfill import BackfillNotStarted, BackfillState
from src.infrastructure.persistence.settings_store import read_runtime_settings

CURSOR_SOURCE = "ncc_backfill"

_SELECT_STATE = text(
    """
    SELECT backfill_from, backfill_until, backfill_cursor, backfill_paused, backfill_done
      FROM speech_ingest_cursors
     WHERE source = :source
    """
)

_START = text(
    """
    INSERT INTO speech_ingest_cursors (
        source, backfill_from, backfill_until, backfill_cursor,
        backfill_paused, backfill_done, updated_at
    )
    VALUES (:source, :from_ts, :until_ts, :until_ts, FALSE, FALSE, now())
    ON CONFLICT (source) DO UPDATE
       SET backfill_from = EXCLUDED.backfill_from,
           backfill_until = EXCLUDED.backfill_until,
           backfill_cursor = EXCLUDED.backfill_cursor,
           backfill_paused = FALSE,
           backfill_done = FALSE,
           updated_at = now()
    """
)

_SET_PAUSED = text(
    """
    UPDATE speech_ingest_cursors
       SET backfill_paused = :paused, updated_at = now()
     WHERE source = :source
    """
)

_ADVANCE = text(
    """
    UPDATE speech_ingest_cursors
       SET backfill_cursor = :cursor,
           backfill_done = :done,
           updated_at = now()
     WHERE source = :source
    """
)


async def read_state_in(session: AsyncSession, *, cursor: str = CURSOR_SOURCE) -> BackfillState:
    """Состояние в уже открытом сеансе.

    Ядро, которым пользуется ручка сервиса: она держит свой сеанс и свою
    транзакцию, а второй движок внутри запроса привязался бы к другому циклу
    событий — он кэширован по URL, и закрепляется за первым же циклом.
    """
    row = (await session.execute(_SELECT_STATE, {"source": cursor})).mappings().first()
    if row is None or row["backfill_cursor"] is None:
        raise BackfillNotStarted(
            "разбор истории не запускали: нет окна, нечего продолжать. "
            "Запустите `python -m src.presentation.cli.backfill start`"
        )
    return BackfillState(
        window_from=row["backfill_from"],
        window_until=row["backfill_until"],
        cursor=row["backfill_cursor"],
        paused=bool(row["backfill_paused"]),
        done=bool(row["backfill_done"]),
    )


async def read_state(
    sessionmaker: async_sessionmaker[AsyncSession], *, cursor: str = CURSOR_SOURCE
) -> BackfillState:
    """То же, но со своим сеансом — для командной строки."""
    async with sessionmaker() as session:
        return await read_state_in(session, cursor=cursor)


async def set_paused_in(session: AsyncSession, *, paused: bool) -> BackfillState:
    """Переключить перечисление дат в уже открытом сеансе.

    Транзакцией не управляет: её ведёт вызывающий, и в ручке сервиса это та же
    транзакция, в которую пишется событие журнала. Записав их порознь, мы
    получили бы паузу без следа в журнале ровно тогда, когда кто-то решил
    выяснить, кто её нажал.
    """
    state = await read_state_in(session)
    if state.paused == paused:
        return state
    await session.execute(_SET_PAUSED, {"source": CURSOR_SOURCE, "paused": paused})
    return BackfillState(
        window_from=state.window_from,
        window_until=state.window_until,
        cursor=state.cursor,
        paused=paused,
        done=state.done,
    )


class PostgresBackfillRepository:
    """Выполнять короткие операции курсора вне долгого обращения к источнику."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        """Принять фабрику сессий выделенной базы речи."""
        self._sessions = sessions

    async def read_state(self, cursor: str) -> BackfillState:
        """Прочитать курсор и сразу закрыть сессию."""
        return await read_state(self._sessions, cursor=cursor)

    async def open_window(self, cursor: str, first: datetime, until: datetime) -> BackfillState:
        """Записать уже проверенное окно и вернуть сохранённое состояние."""
        async with self._sessions() as session, session.begin():
            await session.execute(_START, {"source": cursor, "from_ts": first, "until_ts": until})
        return await self.read_state(cursor)

    async def pause(self, paused: bool) -> BackfillState:
        """Обновить паузу перечисления одной транзакцией."""
        async with self._sessions() as session, session.begin():
            return await set_paused_in(session, paused=paused)

    async def advance(self, cursor: str, to: datetime, done: bool) -> BackfillState:
        """Записать переданные сценариями курсор и признак завершения."""
        async with self._sessions() as session, session.begin():
            await session.execute(_ADVANCE, {"source": cursor, "cursor": to, "done": done})
        return await self.read_state(cursor)

    async def default_window_days(self) -> int:
        """Прочитать проверенную глубину истории из runtime-настроек."""
        async with self._sessions() as session:
            settings = await read_runtime_settings(session)
        return int(settings["backfill_window_days"])
