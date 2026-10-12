"""Удаление записей и технических строк по сроку; --apply включает запись."""

import argparse
import asyncio
import logging
from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """Разобрать аргументы команды, выполнить сценарий и вернуть код завершения."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="действительно удалить")
    parser.add_argument("--batch-size", type=int, default=200)
    args = parser.parse_args(argv)
    if args.batch_size < 1:
        parser.error("--batch-size должен быть положительным")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    from src.di.retention import main_async

    return asyncio.run(main_async(apply=args.apply, batch_size=args.batch_size))


if __name__ == "__main__":
    raise SystemExit(main())
