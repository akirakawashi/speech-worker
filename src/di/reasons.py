"""Сборка клиента причин и пакетного сценария."""

import argparse
import asyncio
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from src.application.errors.external import AnalysisNotConfigured
from src.application.errors.reasons import ReasonsNotConfigured
from src.application.reasons.batch import fill_reasons
from src.di.analysis import build_analysis_client
from src.domain.calls.period import period_bounds
from src.infrastructure.clients.analysis import SpeechAnalysisClient
from src.infrastructure.persistence.reasons import PostgresReasonRepository
from src.infrastructure.persistence.session import get_speech_ms_sessionmaker
from src.setting.config import Settings, get_settings


def build_reason_client(settings: Settings) -> SpeechAnalysisClient:
    """Собрать прямой вызов модели для сценария причин с промптом бэкенда."""
    try:
        return build_analysis_client(settings, scenario="reason")
    except AnalysisNotConfigured as exc:
        raise ReasonsNotConfigured(str(exc)) from None


def run_reason_command(args: argparse.Namespace) -> int:
    """Собрать клиента причин и выполнить заполнение заданного охвата."""
    settings = get_settings()
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit must be positive")
    client = build_reason_client(settings) if args.apply else None
    if args.transcribed_within_hours is not None:
        if args.transcribed_within_hours < 1:
            raise SystemExit("--transcribed-within-hours must be positive")
        report = asyncio.run(
            fill_reasons(
                PostgresReasonRepository(get_speech_ms_sessionmaker(settings)),
                client,
                transcribed_since=datetime.now(UTC)
                - timedelta(hours=args.transcribed_within_hours),
                apply=args.apply,
                limit=args.limit,
                concurrency=args.concurrency,
            )
        )
        print(report.describe())
        return 0
    if args.yesterday:
        # Вчера — в поясе колл-центра, как и границы суток.
        today = datetime.now(ZoneInfo(settings.speech_display_timezone)).date()
        first = last = today - timedelta(days=1)
    elif args.first and args.last:
        first, last = date.fromisoformat(args.first), date.fromisoformat(args.last)
    else:
        raise SystemExit("give --from and --to, --yesterday or --transcribed-within-hours")
    if last < first:
        raise SystemExit("--to is earlier than --from")

    start, end = period_bounds(first, last, settings.speech_display_timezone)
    report = asyncio.run(
        fill_reasons(
            PostgresReasonRepository(get_speech_ms_sessionmaker(settings)),
            client,
            date_from=start,
            date_to=end,
            apply=args.apply,
            limit=args.limit,
            concurrency=args.concurrency,
        )
    )
    print(report.describe())
    if not args.apply:
        print("это была проверка; чтобы спросить модель и записать, добавьте --apply")
    return 0
