"""Команда оценки: разобрать аргументы и вывести результат сценария."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence
from datetime import date

from src.application.dto.quality_batch import RunEvaluation
from src.application.errors.quality import QualityModelError, QualityModelNotConfigured
from src.application.quality.batch import validate_selection


def main(argv: Sequence[str] | None = None) -> int:
    """По аргументам CLI вызвать сценарий оценки и вывести безопасный отчёт."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--from", dest="first", help="первый день, YYYY-MM-DD")
    parser.add_argument("--to", dest="last", help="последний день, YYYY-MM-DD")
    parser.add_argument("--call", help="один звонок: идентификатор сессии или UUID")
    parser.add_argument(
        "--all",
        dest="everything",
        action="store_true",
        help="без периода: все неоценённые звонки; только вместе с --limit",
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="не больше стольких звонков, свежие первыми"
    )
    parser.add_argument(
        "--force", action="store_true", help="оценить заново и звонки, у которых оценка уже есть"
    )
    parser.add_argument(
        "--skip-failed", action="store_true", help="не брать звонки, оценка которых уже не удалась"
    )
    parser.add_argument(
        "--drop-stale",
        action="store_true",
        help="без модели: счёт оценок, разошедшихся с расшифровкой; с --apply — удалить их",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="вызвать модель и записать оценки; без него показывается только счёт",
    )
    args = parser.parse_args(argv)
    from src.di.evaluation import build_evaluation

    try:
        query = RunEvaluation(
            first=date.fromisoformat(args.first) if args.first else None,
            last=date.fromisoformat(args.last) if args.last else None,
            call=args.call,
            everything=args.everything,
            limit=args.limit,
            force=args.force,
            skip_failed=args.skip_failed,
            drop_stale=args.drop_stale,
            apply=args.apply,
        )
        validate_selection(query)
        result = asyncio.run(build_evaluation().run(query, on_line=print))
    except (ValueError, LookupError, QualityModelNotConfigured) as exc:
        raise SystemExit(str(exc)) from exc
    except QualityModelError as exc:
        raise SystemExit(
            "the model refused the request ("
            f"{exc.reason}"
            "): check SPEECH_QUALITY_LLM_BASE_URL, SPEECH_QUALITY_LLM_MODEL "
            "and the key"
        ) from exc
    if result.stale is not None:
        print(
            f"оценок, разошедшихся с расшифровкой: {result.stale}"
            + ("; удалены" if args.apply else "")
        )
        if not args.apply:
            print("это была проверка; чтобы удалить, добавьте --apply")
    elif result.report is not None:
        print(result.report.describe())
        if not args.apply:
            print("это была проверка; чтобы оценить, добавьте --apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
