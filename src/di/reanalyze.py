"""Сборка повторного анализа старых звонков."""

import argparse
import asyncio
from datetime import date

from src.application.analysis.reanalyze import reanalyze
from src.domain.calls.period import period_bounds
from src.infrastructure.persistence.reanalyze import PostgresReanalyzeRepository


def run_reanalyze_command(args: argparse.Namespace) -> int:
    """Собрать репозиторий и выполнить повторную постановку звонков на анализ."""
    first, last = date.fromisoformat(args.first), date.fromisoformat(args.last)
    if last < first:
        raise SystemExit("--to is earlier than --from")
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit must be positive")

    from src.infrastructure.persistence.session import get_speech_ms_sessionmaker
    from src.setting.config import get_settings

    settings = get_settings()
    start, end = period_bounds(first, last, settings.speech_display_timezone)
    report = asyncio.run(
        reanalyze(
            PostgresReanalyzeRepository(get_speech_ms_sessionmaker(settings)),
            date_from=start,
            date_to=end,
            apply=args.apply,
            limit=args.limit,
            missing=args.missing,
        )
    )
    # `print`, а не `logging`: в образах логирование не сконфигурировано.
    print(report.describe())
    if not args.apply:
        print("это была проверка; чтобы поставить задания, добавьте --apply")
    return 0
