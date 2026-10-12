"""Аргументы и точка входа команды classifier_backfill."""

import argparse
from collections.abc import Sequence


def build_parser() -> argparse.ArgumentParser:
    """Описать флаги команды; создание парсера не открывает внешние ресурсы."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="first", required=True, help="первый день, YYYY-MM-DD")
    parser.add_argument("--to", dest="last", required=True, help="последний день, YYYY-MM-DD")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="записать узлы и связи; без него показывается только сводка",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Разобрать аргументы CLI, вызвать сборку сценария и вернуть код выхода."""
    args = build_parser().parse_args(argv)
    from src.di.classifier_backfill import run_classifier_backfill_command

    return run_classifier_backfill_command(args)


if __name__ == "__main__":
    raise SystemExit(main())
