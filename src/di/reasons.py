"""Сборка клиента причин и пакетного сценария."""

import argparse
import asyncio
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from src.application.errors.reasons import ReasonsNotConfigured
from src.application.reasons.batch import fill_reasons
from src.domain.calls.period import period_bounds
from src.infrastructure.clients.analysis import SpeechAnalysisClient
from src.infrastructure.persistence.reasons import PostgresReasonRepository
from src.infrastructure.persistence.session import get_speech_ms_sessionmaker
from src.setting.config import Settings, get_settings


def build_reason_client(settings: Settings) -> SpeechAnalysisClient:
    """Собрать клиент приложения причин по явным настройкам; пустые отклонить."""
    base_url = str(getattr(settings, "speech_dify_base_url", "") or "").strip().rstrip("/")
    app_key = str(getattr(settings, "speech_dify_reason_app_key", "") or "").strip()
    if not base_url or not app_key:
        raise ReasonsNotConfigured(
            "reasons need SPEECH_DIFY_BASE_URL and SPEECH_DIFY_REASON_APP_KEY "
            "(the «Speech: причина обращения» app)"
        )
    return SpeechAnalysisClient(
        base_url=base_url,
        app_key=app_key,
        timeout_seconds=float(getattr(settings, "speech_dify_timeout_seconds", 120.0)),
    )


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
