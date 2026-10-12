"""Аргументы и точка входа команды compare_models."""

import argparse
from collections.abc import Sequence

from src.application.dto.calls import CallSource


def build_parser() -> argparse.ArgumentParser:
    """Описать флаги команды; создание парсера не открывает внешние ресурсы."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="first", required=True, help="первый день, YYYY-MM-DD")
    parser.add_argument("--to", dest="last", required=True, help="последний день, YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=100, help="сколько звонков в выборке")
    parser.add_argument(
        "--concurrency", type=int, default=2, help="сколько звонков разбирается одновременно"
    )
    parser.add_argument(
        "--control", action="store_true", help="разобрать те же звонки и боевым приложением"
    )
    parser.add_argument(
        "--output", help="CSV по звонку; `-` — в стандартный вывод; без него — только сводка"
    )
    parser.add_argument(
        "--apply", action="store_true", help="спросить модели; без него — только размер выборки"
    )
    parser.add_argument(
        "--app",
        choices=("analysis", "reasons"),
        default="analysis",
        help="что сверять: разбор звонка или приложение причин обращения",
    )
    parser.add_argument(
        "--source",
        choices=[source.value for source in CallSource],
        default=CallSource.NCC.value,
        help="источник звонков для --app reasons",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Разобрать аргументы CLI, вызвать сборку сценария и вернуть код выхода."""
    args = build_parser().parse_args(argv)
    from src.di.comparison import run_comparison_command

    return run_comparison_command(args)


if __name__ == "__main__":
    raise SystemExit(main())
