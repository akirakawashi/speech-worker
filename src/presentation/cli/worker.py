"""Аргументы и точка входа команды worker."""

import argparse
from collections.abc import Sequence

from pydantic import ValidationError

from src.application.dto.worker import WorkerOptions


def build_parser() -> argparse.ArgumentParser:
    """Описать флаги команды; создание парсера не открывает внешние ресурсы."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lanes", default="daily,backfill", help="полосы через запятую: daily,backfill"
    )
    parser.add_argument("--once", action="store_true", help="обработать одно задание и выйти")
    parser.add_argument(
        "--stages",
        default="",
        help=(
            "этапы через запятую: ingest,transcribe. Пусто — оба. Анализ и оценку исполняет бэкенд"
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Проверить аргументы как DTO, запустить воркер через DI и вернуть код выхода.

    Args:
        argv: Аргументы без имени команды; None читает их из процесса.

    Returns:
        Ноль после штатной остановки или выполнения одного задания.

    Raises:
        SystemExit: Помощь CLI или неверные параметры до сборки внешних ресурсов.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        options = WorkerOptions.model_validate(vars(args))
    except ValidationError as exc:
        parser.error(str(exc))
    from src.di.worker import run_worker

    return run_worker(options)


if __name__ == "__main__":
    raise SystemExit(main())
