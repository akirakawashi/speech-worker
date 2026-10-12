"""Сборка и запуск команды backfill из её аргументов."""

from __future__ import annotations

import argparse
import asyncio

from src.application.dto.backfill import BackfillNotStarted
from src.di.ingestion import run_portion, set_paused, start
from src.infrastructure.persistence.backfill import read_state
from src.infrastructure.persistence.settings_store import read_runtime_settings
from src.infrastructure.queue.postgres import PostgresJobQueue
from src.infrastructure.runtime.logs import configure_logging


async def _run_backfill(args: argparse.Namespace) -> int:
    """Собрать ресурсы команды истории и выполнить выбранную операцию."""
    configure_logging()
    from src.infrastructure.persistence.session import (
        get_speech_ms_engine,
        get_speech_ms_sessionmaker,
    )
    from src.infrastructure.sources.ncc.source import build_daily_batch_source
    from src.setting.config import get_settings

    settings = get_settings()
    sessionmaker = get_speech_ms_sessionmaker(settings)

    async def _window_days() -> int:
        """Прочитать проверенную глубину окна истории в днях."""
        async with sessionmaker() as session:
            configured = await read_runtime_settings(session)
        return int(configured["backfill_window_days"])

    try:
        if args.command == "start":
            days = args.days if args.days is not None else await _window_days()
            state = await start(sessionmaker, window_days=days, restart=args.restart)
        elif args.command == "pause":
            state = await set_paused(sessionmaker, paused=True)
        elif args.command == "resume":
            state = await set_paused(sessionmaker, paused=False)
        elif args.command == "status":
            state = await read_state(sessionmaker)
        else:
            report = await run_portion(
                source=build_daily_batch_source(settings),
                sessionmaker=sessionmaker,
                queue=PostgresJobQueue(sessionmaker, engine=get_speech_ms_engine(settings)),
                days=args.days,
            )
            print(report.describe())
            return 0
    except (BackfillNotStarted, RuntimeError) as exc:
        print(str(exc))
        return 1
    print(state.describe())
    return 0


def run_backfill_command(args: argparse.Namespace) -> int:
    """Выполнить команду истории на одном event loop; вернуть код выхода."""
    return asyncio.run(_run_backfill(args))
