"""Аргументы и точка входа команды reanalyze."""

import argparse
from collections.abc import Sequence


def build_parser() -> argparse.ArgumentParser:
    """Описать флаги команды; создание парсера не открывает внешние ресурсы."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="first", required=True, help="первый день, YYYY-MM-DD")
    parser.add_argument("--to", dest="last", required=True, help="последний день, YYYY-MM-DD")
    parser.add_argument(
        "--limit", type=int, default=None, help="не больше стольких звонков, свежие первыми"
    )
    parser.add_argument(
        "--missing",
        choices=["path", "reason"],
        default="path",
        help="чего не хватает: path — пути ИИ по дереву (SP-310), reason — причины обращения",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="поставить задания; без него показывается только счёт",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Разобрать аргументы CLI, вызвать сборку сценария и вернуть код выхода."""
    args = build_parser().parse_args(argv)
    from src.di.reanalyze import run_reanalyze_command

    return run_reanalyze_command(args)


if __name__ == "__main__":
    raise SystemExit(main())
