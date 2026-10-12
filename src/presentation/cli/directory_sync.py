"""Команда синхронизации сотрудников и известных проектов NCC."""

import argparse
from collections.abc import Sequence


def build_parser() -> argparse.ArgumentParser:
    """Вернуть парсер выбора справочника; отсутствие флагов означает оба справочника."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--employees", action="store_true", help="справочник сотрудников")
    parser.add_argument("--projects", action="store_true", help="известные проекты")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Разобрать аргументы, обратиться к DI и вывести результат синхронизации."""
    args = build_parser().parse_args(argv)
    from src.di.directory_sync import run_directory_sync_command

    reports = run_directory_sync_command(employees=args.employees, projects=args.projects)
    for report in reports:
        print(report.describe())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
