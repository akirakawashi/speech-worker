"""Точка входа команды daily_ingest: разобрать транспорт и вызвать сборку."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from src.application.dto.calls import CallSource, JobLane
from src.application.dto.daily_ingest import MAX_RUN_PRIORITY, DayShare


def resolve_day(args: argparse.Namespace, timezone: str = "Europe/Moscow") -> date:
    """Разрешить дату CLI или вчерашний день в часовом поясе колл-центра."""
    if args.yesterday:
        return datetime.now(ZoneInfo(timezone)).date() - timedelta(days=1)
    if not args.date:
        raise SystemExit("give --date YYYY-MM-DD or --yesterday")
    try:
        return date.fromisoformat(args.date)
    except ValueError:
        raise SystemExit(f"not a date: {args.date!r}; expected YYYY-MM-DD") from None


def _positive_int(raw: str) -> int:
    """Прочитать положительное целое CLI; ошибку вернуть как ArgumentTypeError."""
    try:
        value = int(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a number: {raw!r}") from None
    if value < 1:
        raise argparse.ArgumentTypeError("must be 1 or more")
    return value


def _share(raw: str) -> float:
    """Прочитать долю CLI от 0 до 1; ошибку вернуть как ArgumentTypeError."""
    try:
        value = float(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a number: {raw!r}") from None
    if not 0.0 <= value <= 1.0:
        raise argparse.ArgumentTypeError("must be between 0 and 1")
    return value


def _hour(raw: str) -> int:
    """Прочитать час CLI от 0 до 23; ошибку вернуть как ArgumentTypeError."""
    try:
        value = int(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not an hour: {raw!r}") from None
    if not 0 <= value <= 23:
        raise argparse.ArgumentTypeError("must be an hour, 0 to 23")
    return value


def _run_priority(raw: str) -> int:
    """Прочитать допустимый приоритет CLI; ошибку вернуть как ArgumentTypeError."""
    try:
        value = int(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a number: {raw!r}") from None
    if not 0 <= value <= MAX_RUN_PRIORITY:
        raise argparse.ArgumentTypeError(f"must be between 0 and {MAX_RUN_PRIORITY}")
    return value


def day_share_of(args: argparse.Namespace, timezone: str) -> DayShare | None:
    """Дневная доля из аргументов. Отказ — до базы и до источника.

    Доля без часов ночи не значит ничего: непонятно, до какого часа откладывать.
    Часы без доли — опечатка в юните, и молча принять её значило бы оставить
    приём без ограничения, которое в юните написано.
    """
    hours = (args.night_from, args.night_until)
    if args.immediate_share is None:
        if any(hour is not None for hour in hours):
            raise SystemExit("--night-from and --night-until need --immediate-share")
        return None
    if args.night_from is None or args.night_until is None:
        raise SystemExit("--immediate-share needs both --night-from and --night-until")
    if args.night_from == args.night_until:
        raise SystemExit("--night-from and --night-until must differ")
    return DayShare(
        share=args.immediate_share,
        night_from=args.night_from,
        night_until=args.night_until,
        timezone=timezone,
    )


def build_parser() -> argparse.ArgumentParser:
    """Описать аргументы команды без создания внешних ресурсов."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="дата в формате YYYY-MM-DD")
    parser.add_argument("--yesterday", action="store_true", help="за вчера")
    parser.add_argument(
        "--lane",
        default=JobLane.DAILY.value,
        choices=[lane.value for lane in JobLane],
        help="полоса очереди; backfill — для истории",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=None,
        help="взять только первые N звонков суток (пробный прогон; сутки не отмечаются)",
    )
    parser.add_argument(
        "--source",
        default=CallSource.NCC.value,
        choices=[source.value for source in CallSource],
        help="источник звонков: ncc — входящие NCC, telphin — исходящие ШПД (фича 14)",
    )
    parser.add_argument(
        "--catch-up",
        action="store_true",
        help=(
            "вчера и сегодня, затем порция истории источника — для таймера; у "
            "Телфина история с SPEECH_TELPHIN_HISTORY_FROM"
        ),
    )
    parser.add_argument(
        "--immediate-share",
        type=_share,
        default=None,
        help=(
            "дневная доля, от 0 до 1: такая часть новых звонков идёт в "
            "обработку сразу, остальные — с начала ночи; только вместе с "
            "--night-from и --night-until"
        ),
    )
    parser.add_argument(
        "--night-from", type=_hour, default=None, help="час начала ночи в поясе колл-центра"
    )
    parser.add_argument(
        "--night-until", type=_hour, default=None, help="час конца ночи в поясе колл-центра"
    )
    parser.add_argument(
        "--priority",
        type=_run_priority,
        default=None,
        help=(
            "приоритет заданий прогона, от 0 до "
            f"{MAX_RUN_PRIORITY}"
            ": меньше — раньше; по умолчанию — приоритет полосы"
        ),
    )
    return parser


def _history_from(raw: str) -> date | None:
    """Прочитать дату начала истории из настройки; пустая строка отключает догон."""
    if not raw.strip():
        return None
    try:
        return date.fromisoformat(raw.strip())
    except ValueError:
        raise SystemExit(f"SPEECH_TELPHIN_HISTORY_FROM={raw!r} is not a date YYYY-MM-DD") from None


def main(argv: Sequence[str] | None = None) -> int:
    """Передать аргументы команде, собранной DI; вернуть код её завершения."""
    args = build_parser().parse_args(argv)
    from src.di.daily_ingest import run_daily_command

    return run_daily_command(args)


if __name__ == "__main__":
    raise SystemExit(main())
