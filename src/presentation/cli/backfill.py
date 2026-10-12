"""Аргументы и точка входа команды backfill."""

import argparse
from collections.abc import Sequence


def build_parser() -> argparse.ArgumentParser:
    """Описать флаги команды; создание парсера не открывает внешние ресурсы."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    started = commands.add_parser("start", help="открыть окно и начать разбор")
    started.add_argument(
        "--days",
        type=int,
        default=None,
        help="глубина окна в сутках; по умолчанию — настройка backfill_window_days",
    )
    started.add_argument(
        "--restart",
        action="store_true",
        help="начать окно заново, даже если предыдущее не пройдено",
    )
    commands.add_parser("pause", help="приостановить перечисление дат")
    commands.add_parser("resume", help="продолжить")
    commands.add_parser("status", help="где сейчас курсор")
    portion = commands.add_parser("run", help="обработать порцию суток")
    portion.add_argument("--days", type=int, default=1, help="сколько суток за раз")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Разобрать аргументы CLI, вызвать сборку сценария и вернуть код выхода."""
    args = build_parser().parse_args(argv)
    from src.di.backfill import run_backfill_command

    return run_backfill_command(args)


if __name__ == "__main__":
    raise SystemExit(main())
