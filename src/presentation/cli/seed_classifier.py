"""Загрузить справочник; без --apply показать план изменений."""

import argparse
import asyncio
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    """Разобрать аргументы CLI, запустить сценарий и вывести сводку; вернуть код выхода."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="записать изменения")
    parser.add_argument("--file", type=Path)
    args = parser.parse_args(argv)
    from src.di.catalogues import catalogue_path, run_classifier

    report = asyncio.run(
        run_classifier(args.file or catalogue_path("classifier.json"), apply=args.apply)
    )
    print(report.describe())
    if not args.apply:
        print("это была проверка; чтобы записать, добавьте --apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
