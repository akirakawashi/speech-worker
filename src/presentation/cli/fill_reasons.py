"""Аргументы и точка входа команды fill_reasons."""

import argparse
from collections.abc import Sequence


def build_parser() -> argparse.ArgumentParser:
    """Описать флаги команды; создание парсера не открывает внешние ресурсы."""
    parser = argparse.ArgumentParser(description="Заполнить причины обращений")
    parser.add_argument("--from", dest="first", help="первый день, YYYY-MM-DD")
    parser.add_argument("--to", dest="last", help="последний день, YYYY-MM-DD")
    parser.add_argument("--yesterday", action="store_true", help="вчерашние сутки")
    parser.add_argument(
        "--transcribed-within-hours",
        type=int,
        default=None,
        help="звонки, расшифрованные за последние N часов, любой даты — для таймера",
    )
    parser.add_argument("--limit", type=int, default=None, help="не больше стольких звонков")
    parser.add_argument(
        "--concurrency", type=int, default=4, help="сколько звонков в модели одновременно"
    )
    parser.add_argument(
        "--apply", action="store_true", help="спросить модель и записать; без него — только счёт"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Разобрать аргументы CLI, вызвать сборку сценария и вернуть код выхода."""
    args = build_parser().parse_args(argv)
    from src.di.reasons import run_reason_command

    return run_reason_command(args)


if __name__ == "__main__":
    raise SystemExit(main())
