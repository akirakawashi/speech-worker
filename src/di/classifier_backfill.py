"""Сборка пополнения классификатора из отчётных представлений."""

import argparse
import asyncio
from datetime import date

from src.application.classifier.backfill import backfill_classifier, days_between
from src.infrastructure.persistence.classifier_backfill import PostgresClassifierBackfillRepository


def run_classifier_backfill_command(args: argparse.Namespace) -> int:
    """Собрать источник классификаций и выполнить пополнение дерева."""
    try:
        days = days_between(date.fromisoformat(args.first), date.fromisoformat(args.last))
    except ValueError as exc:
        raise SystemExit(str(exc)) from None

    from src.infrastructure.persistence.session import get_speech_ms_sessionmaker
    from src.infrastructure.sources.ncc.reporting import ReportingViewSource
    from src.infrastructure.sources.ncc.source import build_daily_batch_source
    from src.setting.config import get_settings

    settings = get_settings()
    source = build_daily_batch_source(settings)
    if not isinstance(source, ReportingViewSource):
        raise SystemExit(
            "classifier backfill reads the reporting views: SPEECH_NCC_SOURCE_KIND must be "
            f"reporting_view, not {settings.speech_ncc_source_kind!r}"
        )
    report = asyncio.run(
        backfill_classifier(
            source,
            days,
            repository=PostgresClassifierBackfillRepository(get_speech_ms_sessionmaker(settings)),
            apply=args.apply,
        )
    )
    # `print`, а не `logging`: в образах логирование не сконфигурировано.
    print(report.describe())
    if not args.apply:
        print("это была проверка; чтобы записать, добавьте --apply")
    return 0
