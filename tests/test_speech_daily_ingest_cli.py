"""Аргументы суточного прогона — без базы.

Сам прогон с ограничением проверяется на настоящем Postgres в
`test_speech_worker_and_daily_run.py`; здесь — только то, что опечатка в
`--limit` останавливает запуск, а не превращается в «без ограничения».

Дневная доля: когда начинается ночь и какие сочетания аргументов приём не
принимает. Сам жребий и отсрочка — там же, на Postgres.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import pytest

from src.application.dto.daily_ingest import MAX_RUN_PRIORITY, DailyRunReport, DayShare
from src.presentation.cli.daily_ingest import build_parser, day_share_of


def test_the_limit_is_optional_and_parsed() -> None:
    parser = build_parser()

    assert parser.parse_args(["--date", "2026-09-14", "--limit", "20"]).limit == 20
    assert parser.parse_args(["--yesterday"]).limit is None


@pytest.mark.parametrize("value", ["0", "-5", "двадцать"])
def test_a_limit_that_is_not_a_positive_number_stops_the_run(value: str) -> None:
    """Ноль или мусор не должны тихо означать «все звонки суток»."""
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--date", "2026-09-14", "--limit", value])


def test_a_limited_run_says_that_the_day_stays_open() -> None:
    report = DailyRunReport(
        day=date(2026, 9, 14), discovered=1827, enqueued=20, already_known=0, limited_to=20
    )

    described = report.describe()

    assert "найдено 1827" in described
    assert "первых 20" in described
    assert "не отмечены" in described


def test_a_full_run_does_not_mention_a_limit() -> None:
    report = DailyRunReport(day=date(2026, 9, 14), discovered=2, enqueued=2, already_known=0)

    assert "первых" not in report.describe()


def test_calls_left_out_by_a_switched_off_project_are_named() -> None:
    """«Найдено 10, поставлено 7» без причины читалось бы как потеря звонков."""
    report = DailyRunReport(
        day=date(2026, 9, 14), discovered=10, enqueued=7, already_known=0, collection_off=3
    )

    assert "не заведено 3 — сбор проекта выключен" in report.describe()


def test_the_switch_is_not_mentioned_when_it_left_nothing_out() -> None:
    report = DailyRunReport(day=date(2026, 9, 14), discovered=2, enqueued=2, already_known=0)

    assert "сбор проекта" not in report.describe()


# --------------------------------------------------------------------------- #
# Дневная доля
# --------------------------------------------------------------------------- #

MSK = ZoneInfo("Europe/Moscow")
NIGHT = DayShare(share=0.5, night_from=19, night_until=7)


@pytest.mark.parametrize(
    ("clock", "expected"),
    [
        # День: ждать сегодняшних 19:00.
        ("2026-10-08 07:00", "2026-10-08 19:00"),
        ("2026-10-08 12:30", "2026-10-08 19:00"),
        ("2026-10-08 18:59", "2026-10-08 19:00"),
        # Ночь — и вечером, и после полуночи: откладывать некуда.
        ("2026-10-08 19:00", None),
        ("2026-10-08 23:40", None),
        ("2026-10-09 03:15", None),
        ("2026-10-09 06:59", None),
    ],
)
def test_the_night_starts_at_its_hour_in_the_call_centre_time(
    clock: str, expected: str | None
) -> None:
    now = datetime.strptime(clock, "%Y-%m-%d %H:%M").replace(tzinfo=MSK)
    wanted = datetime.strptime(expected, "%Y-%m-%d %H:%M").replace(tzinfo=MSK) if expected else None

    assert NIGHT.night_start(now) == wanted
    # Часы узла — UTC: тот же момент, названный в другом поясе, даёт тот же ответ.
    assert NIGHT.night_start(now.astimezone(UTC)) == wanted


def test_a_night_inside_one_day_waits_for_the_next_one_after_it_ends() -> None:
    short = DayShare(share=0.5, night_from=1, night_until=6)

    def at(clock: str) -> datetime:
        return datetime.strptime(clock, "%Y-%m-%d %H:%M").replace(tzinfo=MSK)

    assert short.night_start(at("2026-10-08 00:30")) == at("2026-10-08 01:00")
    assert short.night_start(at("2026-10-08 03:00")) is None
    assert short.night_start(at("2026-10-08 06:00")) == at("2026-10-09 01:00")


def _share_args(*argv: str) -> DayShare | None:
    return day_share_of(build_parser().parse_args(["--yesterday", *argv]), "Europe/Moscow")


def test_the_share_comes_with_the_hours_of_the_night() -> None:
    share = _share_args("--immediate-share", "0.5", "--night-from", "19", "--night-until", "7")

    assert share == DayShare(share=0.5, night_from=19, night_until=7, timezone="Europe/Moscow")
    # Без доли приём работает как раньше: всё идёт в обработку сразу.
    assert _share_args() is None


@pytest.mark.parametrize(
    "argv",
    [
        # Доля без часов ночи: непонятно, до какого часа откладывать.
        ["--immediate-share", "0.5"],
        ["--immediate-share", "0.5", "--night-from", "19"],
        # Часы без доли — опечатка в юните, а не «без ограничения».
        ["--night-from", "19", "--night-until", "7"],
        ["--immediate-share", "0.5", "--night-from", "19", "--night-until", "19"],
    ],
)
def test_a_share_that_cannot_be_applied_stops_the_run(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        _share_args(*argv)


@pytest.mark.parametrize(
    "argv",
    [
        ["--immediate-share", "1.5"],
        ["--immediate-share", "половина"],
        ["--night-from", "24"],
        ["--priority", "-1"],
        # Выше потолка отложенное сравнялось бы с историей, которая уступает всему.
        ["--priority", str(MAX_RUN_PRIORITY + 1)],
    ],
)
def test_a_value_out_of_range_is_refused_by_the_parser(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--yesterday", *argv])


def test_the_run_says_how_many_calls_wait_for_the_night() -> None:
    report = DailyRunReport(
        day=date(2026, 9, 14), discovered=40, enqueued=40, already_known=0, deferred=21
    )

    assert "до ночи отложено 21" in report.describe()
    quiet = DailyRunReport(day=date(2026, 9, 14), discovered=2, enqueued=2, already_known=0)
    assert "отложено" not in quiet.describe()
