"""Блокировка одного прогона очистки на выделенном соединении PostgreSQL."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from src.infrastructure.queue.postgres import ADVISORY_NAMESPACE, stable_advisory_key

LOCK_NAME = "speech:prune"


@asynccontextmanager
async def _singleton(engine: AsyncEngine) -> AsyncIterator[bool]:
    """Блокировка на весь прогон, а не на транзакцию.

    `acquire_singleton` из очереди здесь не годится: она держится до конца
    транзакции, а прогон — это много транзакций подряд, между которыми идут
    запросы к хранилищу. Поэтому блокировка уровня сессии на выделенном
    соединении, снимаемая явно.
    """
    key = stable_advisory_key(LOCK_NAME)
    async with engine.connect() as connection:
        got = await connection.scalar(
            text("SELECT pg_try_advisory_lock(:ns, :key)"),
            {"ns": ADVISORY_NAMESPACE, "key": key},
        )
        try:
            yield bool(got)
        finally:
            if got:
                await connection.exec_driver_sql(
                    f"SELECT pg_advisory_unlock({ADVISORY_NAMESPACE}, {key})"
                )
