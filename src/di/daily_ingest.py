"""Сборка и запуск команды daily_ingest из её аргументов."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from src.application.dto.calls import CallSource, JobLane
from src.di.ingestion import catch_up, run_day
from src.infrastructure.queue.postgres import PostgresJobQueue
from src.infrastructure.runtime.logs import configure_logging
from src.presentation.cli.daily_ingest import _history_from, day_share_of, resolve_day


def run_daily_command(args: argparse.Namespace) -> int:
    """Собрать источник, хранение и очередь для команды приёма звонков."""
    configure_logging()
    from src.di.sources import build_ingest_source
    from src.infrastructure.persistence.session import get_speech_ms_sessionmaker
    from src.setting.config import get_settings

    settings = get_settings()
    day_share = day_share_of(args, settings.speech_display_timezone)
    sessionmaker = get_speech_ms_sessionmaker(settings)
    source = build_ingest_source(args.source, settings)
    pepper = settings.speech_phone_hash_pepper
    if args.catch_up:
        telphin = args.source == CallSource.TELPHIN
        lines = asyncio.run(
            catch_up(
                source=source,
                sessionmaker=sessionmaker,
                queue=PostgresJobQueue(sessionmaker),
                call_source=args.source,
                today=datetime.now(ZoneInfo(settings.speech_display_timezone)).date(),
                history_from=_history_from(settings.speech_telphin_history_from)
                if telphin
                else None,
                history_days=settings.speech_telphin_history_days_per_run,
                phone_pepper=pepper,
                day_share=day_share,
                priority=args.priority,
            )
        )
        for line in lines:
            print(line)
        return 0
    day = resolve_day(args, settings.speech_display_timezone)
    report = asyncio.run(
        run_day(
            day,
            source=source,
            sessionmaker=sessionmaker,
            queue=PostgresJobQueue(sessionmaker),
            lane=JobLane(args.lane),
            limit=args.limit,
            call_source=args.source,
            phone_pepper=pepper,
            day_share=day_share,
            priority=args.priority,
        )
    )
    print(report.describe())
    return 0
