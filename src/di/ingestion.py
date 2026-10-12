"""Сборка свежего и исторического приёма; единая фабрика сессий и очередь."""

from __future__ import annotations

import random
from datetime import date, datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.application.dto.backfill import BackfillState, PortionReport
from src.application.dto.calls import JobLane
from src.application.dto.daily_ingest import DailyRunReport, DayShare
from src.application.ingestion import backfill, daily
from src.application.ports.sources import DailyBatchSource
from src.infrastructure.persistence.backfill import PostgresBackfillRepository
from src.infrastructure.persistence.daily_ingest import PostgresDailyIngestRepository
from src.infrastructure.queue.postgres import PostgresJobQueue


async def run_day(
    day: date,
    *,
    source: DailyBatchSource,
    sessionmaker: async_sessionmaker[AsyncSession],
    queue: PostgresJobQueue,
    lane: JobLane = JobLane.DAILY,
    limit: int | None = None,
    call_source: str = "ncc",
    phone_pepper: str = "",
    day_share: DayShare | None = None,
    priority: int | None = None,
    now: datetime | None = None,
    rng: random.Random | None = None,
) -> DailyRunReport:
    """Собрать порты дня из переданных ресурсов и вызвать сценарий регистрации."""
    return await daily.run_day(
        day,
        source=source,
        repository=PostgresDailyIngestRepository(sessionmaker, queue, phone_pepper=phone_pepper),
        lane=lane,
        limit=limit,
        call_source=call_source,
        day_share=day_share,
        priority=priority,
        now=now,
        rng=rng,
    )


async def catch_up(
    *,
    source: DailyBatchSource,
    sessionmaker: async_sessionmaker[AsyncSession],
    queue: PostgresJobQueue,
    call_source: str,
    today: date,
    history_from: date | None,
    history_days: int,
    phone_pepper: str = "",
    day_share: DayShare | None = None,
    priority: int | None = None,
) -> list[str]:
    """Собрать независимые порты дня и истории и передать их сценарию догона."""
    return await daily.catch_up(
        source=source,
        repository=PostgresDailyIngestRepository(sessionmaker, queue, phone_pepper=phone_pepper),
        history=PostgresBackfillRepository(sessionmaker),
        call_source=call_source,
        today=today,
        history_from=history_from,
        history_days=history_days,
        day_share=day_share,
        priority=priority,
    )


async def start(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    window_days: int,
    now: datetime | None = None,
    restart: bool = False,
    cursor: str = "ncc_backfill",
) -> BackfillState:
    """Собрать хранение курсора и открыть окно прикладным сценарием."""
    return await backfill.start(
        PostgresBackfillRepository(sessionmaker),
        window_days=window_days,
        now=now,
        restart=restart,
        cursor=cursor,
    )


async def set_paused(
    sessionmaker: async_sessionmaker[AsyncSession], *, paused: bool
) -> BackfillState:
    """Собрать хранение и изменить паузу перечисления дат сценарием."""
    return await backfill.set_paused(PostgresBackfillRepository(sessionmaker), paused=paused)


async def run_portion(
    *,
    source: DailyBatchSource,
    sessionmaker: async_sessionmaker[AsyncSession],
    queue: PostgresJobQueue,
    days: int = 1,
    cursor: str = "ncc_backfill",
    call_source: str = "ncc",
    phone_pepper: str = "",
) -> PortionReport:
    """Собрать хранение прогресса и регистрации дня для обработки порции истории."""
    return await backfill.run_portion(
        source=source,
        repository=PostgresBackfillRepository(sessionmaker),
        ingest_repository=PostgresDailyIngestRepository(
            sessionmaker, queue, phone_pepper=phone_pepper
        ),
        days=days,
        cursor=cursor,
        call_source=call_source,
    )
