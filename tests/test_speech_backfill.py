"""Разбор истории на настоящем Postgres.

Проверяется то, ради чего бэкфилл вообще отличается от суточного прогона:
курсор, окно, пауза и «не потерять и не продублировать». Всё это живёт в
строке таблицы, и мок на её месте проверял бы, что код зовёт `UPDATE`, а не
что после падения посередине ни одни сутки не пропали.

Источник подставной: он отдаёт по звонку на дату и запоминает, о каких датах
его спрашивали. Порядок этих дат — главное утверждение половины тестов.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.application.dto.backfill import BackfillNotStarted
from src.application.ingestion.backfill import CURSOR_SOURCE
from src.di.ingestion import run_portion, set_paused, start
from src.domain.calls.contracts import JobLane
from src.infrastructure.persistence.backfill import read_state
from src.infrastructure.queue.postgres import PostgresJobQueue
from src.infrastructure.sources.ncc.source import CallRef
from tests.speech_db_support import speech_test_database_url

_TEST_DATABASE_URL = speech_test_database_url()
pytestmark = pytest.mark.skipif(
    not _TEST_DATABASE_URL,
    reason="SPEECH_POSTGRES_TEST_URL is required: курсор живёт в таблице",
)

#: Полдень, чтобы выравнивание окна по полуночи было видно, а не совпало
#: случайно.
NOW = datetime(2026, 9, 11, 12, 30, tzinfo=UTC)


class _Source:
    """Источник, отдающий один звонок на дату и помнящий, о чём спрашивали."""

    def __init__(self, *, empty_days: set[date] | None = None) -> None:
        self.asked: list[date] = []
        self._empty = empty_days or set()

    @property
    def name(self) -> str:
        return "fake"

    async def list_calls(self, day: date) -> Sequence[CallRef]:
        self.asked.append(day)
        if day in self._empty:
            return ()
        stamp = int(datetime.combine(day, datetime.min.time(), tzinfo=UTC).timestamp())
        return (
            CallRef(
                session_id=f"node_1_domain_0_nauss_0_{stamp}_1",
                audio_locator=f"calls/{day}/1.mp3",
                day=day,
            ),
        )

    async def open_audio(self, ref: CallRef) -> AsyncIterator[bytes]:  # pragma: no cover
        raise AssertionError("бэкфилл не качает аудио — это делает этап приёма")
        yield b""


@pytest.fixture
async def stand() -> AsyncIterator[tuple[async_sessionmaker[AsyncSession], PostgresJobQueue]]:
    engine = create_async_engine(_TEST_DATABASE_URL or "")
    maker = async_sessionmaker(engine, expire_on_commit=False)

    async def _clean() -> None:
        async with maker() as session, session.begin():
            await session.execute(text("DELETE FROM speech_jobs"))
            await session.execute(text("DELETE FROM speech_calls"))
            await session.execute(
                text("DELETE FROM speech_ingest_cursors WHERE source = :s"),
                {"s": CURSOR_SOURCE},
            )

    await _clean()
    yield maker, PostgresJobQueue(maker, engine=engine)
    await _clean()
    await engine.dispose()


async def _lanes(maker: async_sessionmaker[AsyncSession]) -> dict[str, int]:
    async with maker() as session:
        rows = (
            await session.execute(text("SELECT lane, count(*) FROM speech_jobs GROUP BY lane"))
        ).all()
    return {str(lane): int(count) for lane, count in rows}


async def _calls(maker: async_sessionmaker[AsyncSession]) -> int:
    async with maker() as session:
        return int(await session.scalar(text("SELECT count(*) FROM speech_calls")) or 0)


# --------------------------------------------------------------------------- #
# Окно и курсор
# --------------------------------------------------------------------------- #


async def test_nothing_to_continue_before_a_window_is_opened(stand: Any) -> None:
    """«Не начинали» и «всё сделано» — разные ответы.

    Пустое состояние на оба администратору ничего не говорит: в первом случае
    надо нажать «начать», во втором — ничего не надо.
    """
    maker, _ = stand

    with pytest.raises(BackfillNotStarted):
        await read_state(maker)


async def test_the_window_is_aligned_to_midnight(stand: Any) -> None:
    """Источник умеет только сутки: `list_calls` принимает дату.

    Курсор в середине дня означал бы, что половина суток обработана, а
    половина нет, — и различить это нечем.
    """
    maker, _ = stand

    state = await start(maker, window_days=3, now=NOW)

    assert state.window_until == datetime(2026, 9, 11, tzinfo=UTC)
    assert state.window_from == datetime(2026, 9, 8, tzinfo=UTC)
    assert state.cursor == state.window_until
    assert state.done is False


async def test_the_first_portion_takes_yesterday_not_today(stand: Any) -> None:
    """Сегодняшний день ещё идёт, и звонки в нём продолжают появляться.

    Взяв его, бэкфилл записал бы неполные сутки и пометил их пройденными.
    """
    maker, queue = stand
    await start(maker, window_days=3, now=NOW)
    source = _Source()

    await run_portion(source=source, sessionmaker=maker, queue=queue, days=1)

    assert source.asked == [date(2026, 9, 10)]


async def test_the_cursor_walks_from_fresh_to_old(stand: Any) -> None:
    """За неделю до сегодня искать станут раньше, чем за позапрошлый год.

    Остановленный на середине бэкфилл должен останавливаться на менее нужном.
    """
    maker, queue = stand
    await start(maker, window_days=5, now=NOW)
    source = _Source()

    await run_portion(source=source, sessionmaker=maker, queue=queue, days=3)

    assert source.asked == [date(2026, 9, 10), date(2026, 9, 9), date(2026, 9, 8)]


async def test_reaching_the_window_edge_finishes_the_backfill(stand: Any) -> None:
    maker, queue = stand
    await start(maker, window_days=2, now=NOW)
    source = _Source()

    report = await run_portion(source=source, sessionmaker=maker, queue=queue, days=10)

    assert source.asked == [date(2026, 9, 10), date(2026, 9, 9)]
    assert report.state is not None
    assert report.state.done is True
    assert report.state.days_left == 0
    assert "завершён" in report.state.describe()


async def test_a_finished_backfill_asks_for_nothing_more(stand: Any) -> None:
    """Иначе ночной запуск по расписанию ходил бы в источник вечно."""
    maker, queue = stand
    await start(maker, window_days=1, now=NOW)
    await run_portion(source=_Source(), sessionmaker=maker, queue=queue, days=5)

    source = _Source()
    report = await run_portion(source=source, sessionmaker=maker, queue=queue, days=5)

    assert source.asked == []
    assert report.days == ()


# --------------------------------------------------------------------------- #
# Полоса и приоритет
# --------------------------------------------------------------------------- #


async def test_the_jobs_land_in_the_backfill_lane(stand: Any) -> None:
    """Вчерашние звонки важнее позапрошлогодних — разделение полосами.

    Попади история в суточную полосу, она встала бы перед вчерашним пакетом и
    отодвинула его на часы.
    """
    maker, queue = stand
    await start(maker, window_days=3, now=NOW)

    await run_portion(source=_Source(), sessionmaker=maker, queue=queue, days=3)

    assert await _lanes(maker) == {JobLane.BACKFILL.value: 3}


# --------------------------------------------------------------------------- #
# Пауза
# --------------------------------------------------------------------------- #


async def test_a_paused_backfill_asks_the_source_for_nothing(stand: Any) -> None:
    maker, queue = stand
    await start(maker, window_days=5, now=NOW)
    await set_paused(maker, paused=True)
    source = _Source()

    report = await run_portion(source=source, sessionmaker=maker, queue=queue, days=3)

    assert source.asked == []
    assert report.days == ()
    assert report.stopped_because is not None and "паузе" in report.stopped_because


async def test_pausing_and_resuming_loses_nothing_and_repeats_nothing(stand: Any) -> None:
    """Главное требование задачи, и проверяется оно по датам, а не по счётчику.

    Счётчик заданий совпал бы и при пропуске суток с повтором другой.
    """
    maker, queue = stand
    await start(maker, window_days=6, now=NOW)

    first = _Source()
    await run_portion(source=first, sessionmaker=maker, queue=queue, days=2)
    await set_paused(maker, paused=True)
    during_pause = _Source()
    await run_portion(source=during_pause, sessionmaker=maker, queue=queue, days=2)
    await set_paused(maker, paused=False)
    after = _Source()
    await run_portion(source=after, sessionmaker=maker, queue=queue, days=4)

    assert during_pause.asked == []
    walked = first.asked + after.asked
    expected = [date(2026, 9, 10) - timedelta(days=offset) for offset in range(6)]
    assert walked == expected, "даты пропущены или пройдены дважды"
    assert await _calls(maker) == 6


async def test_pausing_twice_is_not_an_error(stand: Any) -> None:
    """Кнопка нажимается людьми, и второе нажатие не должно ничего ломать."""
    maker, _ = stand
    await start(maker, window_days=2, now=NOW)

    await set_paused(maker, paused=True)
    state = await set_paused(maker, paused=True)

    assert state.paused is True


# --------------------------------------------------------------------------- #
# Повторный запуск
# --------------------------------------------------------------------------- #


async def test_starting_over_an_unfinished_window_is_refused(stand: Any) -> None:
    """Разбор истории идёт неделями.

    «Запустить ещё раз», не заметив, что он уже идёт, означало бы пройти то же
    самое второй раз: дубликатов не будет — их держат ключи, — но время будет
    потрачено, а прогресс потерян.
    """
    maker, queue = stand
    await start(maker, window_days=5, now=NOW)
    await run_portion(source=_Source(), sessionmaker=maker, queue=queue, days=2)

    with pytest.raises(RuntimeError, match="уже идёт"):
        await start(maker, window_days=5, now=NOW)

    # Две порции сдвинули курсор на двое суток: 11-е → 10-е → 9-е. Существенно
    # здесь не само число, а что отклонённый запуск его не тронул.
    assert (await read_state(maker)).cursor == datetime(2026, 9, 9, tzinfo=UTC)


async def test_restart_is_possible_when_asked_for_explicitly(stand: Any) -> None:
    maker, queue = stand
    await start(maker, window_days=5, now=NOW)
    await run_portion(source=_Source(), sessionmaker=maker, queue=queue, days=2)

    state = await start(maker, window_days=5, now=NOW, restart=True)

    assert state.cursor == state.window_until
    assert state.done is False


async def test_a_finished_window_may_be_reopened_without_a_flag(stand: Any) -> None:
    """Законченный бэкфилл не «идёт», и просить подтверждения не за что."""
    maker, queue = stand
    await start(maker, window_days=1, now=NOW)
    await run_portion(source=_Source(), sessionmaker=maker, queue=queue, days=5)

    state = await start(maker, window_days=30, now=NOW)

    assert state.done is False
    assert state.window_from == datetime(2026, 8, 12, tzinfo=UTC)


# --------------------------------------------------------------------------- #
# Устойчивость
# --------------------------------------------------------------------------- #


async def test_a_repeated_day_creates_neither_a_second_call_nor_a_second_job(
    stand: Any,
) -> None:
    """Упав между обработкой суток и сдвигом курсора, прогон повторит дату.

    Повтор обязан быть безопасным — на этом и построен порядок «сначала
    обработать, потом сдвинуть».
    """
    maker, queue = stand
    await start(maker, window_days=3, now=NOW)
    await run_portion(source=_Source(), sessionmaker=maker, queue=queue, days=1)

    # Откат курсора руками — ровно то, что видит прогон после падения.
    async with maker() as session, session.begin():
        await session.execute(
            text(
                "UPDATE speech_ingest_cursors SET backfill_cursor = backfill_until "
                "WHERE source = :s"
            ),
            {"s": CURSOR_SOURCE},
        )

    await run_portion(source=_Source(), sessionmaker=maker, queue=queue, days=1)

    assert await _calls(maker) == 1
    assert await _lanes(maker) == {JobLane.BACKFILL.value: 1}


async def test_an_empty_day_still_moves_the_cursor(stand: Any) -> None:
    """Выходной — валидный ответ источника, а не отказ.

    Остановись курсор на нём, бэкфилл встал бы на первых же новогодних днях.
    """
    maker, queue = stand
    await start(maker, window_days=3, now=NOW)
    source = _Source(empty_days={date(2026, 9, 10)})

    await run_portion(source=source, sessionmaker=maker, queue=queue, days=2)

    assert source.asked == [date(2026, 9, 10), date(2026, 9, 9)]
    assert await _calls(maker) == 1


async def test_a_concurrent_run_stops_the_portion_without_moving_the_cursor(
    stand: Any,
) -> None:
    """Прогон, не получивший блокировку, не обработал сутки.

    Сдвинув курсор, мы пропустили бы день из-за чужого прогона — и не узнали бы
    об этом: поиск по тем суткам просто ничего не найдёт.
    """
    maker, queue = stand
    await start(maker, window_days=3, now=NOW)
    engine = create_async_engine(_TEST_DATABASE_URL or "")
    holder = async_sessionmaker(engine, expire_on_commit=False)

    async with holder() as session, session.begin():
        from src.infrastructure.persistence.daily_ingest import DAILY_LOCK_NAME  # noqa: PLC0415
        from src.infrastructure.queue.postgres import acquire_singleton  # noqa: PLC0415

        assert await acquire_singleton(session, DAILY_LOCK_NAME)
        report = await run_portion(source=_Source(), sessionmaker=maker, queue=queue, days=2)

    await engine.dispose()

    assert report.days == ()
    assert report.stopped_because is not None
    assert (await read_state(maker)).cursor == datetime(2026, 9, 11, tzinfo=UTC)


async def test_a_window_of_zero_days_is_a_mistake_not_an_empty_run(stand: Any) -> None:
    maker, _ = stand

    with pytest.raises(ValueError, match="window_days"):
        await start(maker, window_days=0, now=NOW)


def test_the_state_reads_differently_when_paused_and_when_done() -> None:
    """Администратор читает строку, а не поля: она обязана их различать."""
    from src.application.dto.backfill import BackfillState  # noqa: PLC0415

    window_from = datetime(2026, 9, 1, tzinfo=UTC)
    window_until = datetime(2026, 9, 11, tzinfo=UTC)
    paused = BackfillState(
        window_from=window_from,
        window_until=window_until,
        cursor=datetime(2026, 9, 5, tzinfo=UTC),
        paused=True,
        done=False,
    )
    done = BackfillState(
        window_from=window_from,
        window_until=window_until,
        cursor=window_from,
        paused=False,
        done=True,
    )

    assert "паузе" in paused.describe()
    assert paused.days_left == 4
    assert paused.next_day is None, "на паузе следующей даты нет"
    assert "завершён" in done.describe()
    assert done.next_day is None


async def test_uniqueness_is_what_makes_a_repeat_safe(stand: Any) -> None:
    """Идемпотентность держится не на курсоре, а на ключах.

    Курсор лишь избавляет от лишней работы; безопасным повтор суток делает
    уникальность `session_id` и пары `(call_id, kind)`. Проверка стоит здесь,
    чтобы правка, снимающая уникальность, роняла и тест бэкфилла: без неё
    падение между обработкой и сдвигом курсора начало бы плодить дубликаты.
    """
    maker, _ = stand

    async with maker() as session:
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT conrelid::regclass::text AS onto, "
                        "pg_get_constraintdef(oid) AS rule "
                        "FROM pg_constraint "
                        "WHERE conrelid IN ('speech_calls'::regclass, 'speech_jobs'::regclass) "
                        "AND contype IN ('u', 'p')"
                    )
                )
            )
            .mappings()
            .all()
        )

    rules = {(row["onto"], row["rule"]) for row in rows}
    assert any(onto == "speech_calls" and "session_id" in rule for onto, rule in rules), (
        "уникальность session_id снята — повтор суток начнёт плодить звонки"
    )
    assert any(
        onto == "speech_jobs" and "call_id" in rule and "kind" in rule for onto, rule in rules
    ), "уникальность (call_id, kind) снята — повтор суток начнёт плодить задания"
