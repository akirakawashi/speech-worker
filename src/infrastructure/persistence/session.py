"""Пул PostgreSQL воркера и проверка схемы, которой владеет speech-backend."""

import asyncio
from functools import lru_cache

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from src.infrastructure.persistence.write_trace import install_write_trace
from src.setting.config import Settings

# Схема общая с speech-backend; воркер проверяет её и не запускает миграции.
SPEECH_REQUIRED_SCHEMA_REVISION: str = "sp_0015_drop_masking"
_SPEECH_REQUIRED_TABLES = (
    "speech_calls",
    "speech_call_reasons",
    "speech_classifier_nodes",
    "speech_jobs",
    "speech_topics",
    "speech_transcripts",
    "speech_transcript_segments",
    "speech_settings",
    "speech_call_evaluations",
)
_SPEECH_REQUIRED_TABLE_NAMES_SQL = ", ".join(f"'{name}'" for name in _SPEECH_REQUIRED_TABLES)
_SPEECH_SCHEMA_SQL = text(
    f"""
    SELECT
        (
            SELECT count(*) = 1
            FROM alembic_version
            WHERE version_num = '{SPEECH_REQUIRED_SCHEMA_REVISION}'
        )
        AND (
            SELECT count(*) = {len(_SPEECH_REQUIRED_TABLES)}
            FROM pg_catalog.pg_class AS relation
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'public'
              AND relation.relkind IN ('r', 'p')
              AND relation.relname IN ({_SPEECH_REQUIRED_TABLE_NAMES_SQL})
        )
    """
)


async def speech_schema_is_ready(session: AsyncSession) -> bool:
    """Проверить ревизию и таблицы воркера; вернуть False при несовместимой схеме."""
    try:
        result = await session.scalar(_SPEECH_SCHEMA_SQL)
        return bool(result)
    except Exception:
        return False


def _pool_kwargs(settings: Settings) -> dict[str, object]:
    """The connection budget the engine of this process is held to.

    Declared rather than defaulted: the library default is 15 per engine, and
    the cluster is shared.
    """
    return {
        "pool_pre_ping": True,
        "pool_size": settings.db_pool_size,
        "max_overflow": settings.db_max_overflow,
        "pool_timeout": settings.db_pool_timeout_seconds,
        "pool_recycle": settings.db_pool_recycle_seconds,
    }


@lru_cache(maxsize=8)
def _speech_ms_engine(
    database_url: str,
    pool_size: int,
    max_overflow: int,
    pool_timeout: float,
    pool_recycle: int,
    write_trace: bool,
    event_loop: asyncio.AbstractEventLoop | None = None,
) -> AsyncEngine:
    """Создать один пул на комбинацию адреса и параметров; окружение не читает."""
    engine = create_async_engine(
        database_url,
        hide_parameters=True,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_recycle=pool_recycle,
    )
    if write_trace:
        install_write_trace(engine, label="speech-ms")
    return engine


def get_speech_ms_sessionmaker(
    settings: Settings,
) -> async_sessionmaker[AsyncSession]:
    """Сессии процессов Speech Analytics: сервиса, воркеров и поллера.

    Бюджет соединений. Движок держит `db_pool_size` (5) и добирает до
    `db_max_overflow` (3). Процессов у Speech Analytics четыре вида: сервис,
    поллер и два вида воркеров, — но движок в каждом ровно один, потому что
    каждый из них соединяется только со своей базой. Пик Speech Analytics при
    одном сервисе, одном поллере и четырёх воркерах — 6 × 8 = 48 в теории и
    ~17 в установившемся режиме. Кластер общий с LLM и его `max_connections`
    поднимается перед выкаткой; арифметика зафиксирована в
    `aicistent-documentation/speech-analytics/02-architecture-design.md` §9.
    """
    return async_sessionmaker(get_speech_ms_engine(settings), expire_on_commit=False)


def get_speech_ms_engine(settings: Settings) -> AsyncEngine:
    """Тот же движок, что стоит за сессиями Speech Analytics.

    Нужен там, где сессии недостаточно. Слот расшифровки — advisory-блокировка
    **уровня сессии**, и держать её обязано одно физическое соединение;
    `async_sessionmaker` этого не гарантирует и берёт соединение из пула на
    каждую сессию. Очередь без движка отказывается выдавать слот, и до этой
    функции боевые точки входа собирали её именно так — то есть расшифровка
    отказала бы при первом обращении к слоту.

    Движок кэширован по URL (`_speech_ms_engine`), поэтому вызов отсюда и из
    `get_speech_ms_sessionmaker` даёт один и тот же объект, а не второй пул.

    Пустой адрес — отказ, а не откат. В монорепозитории здесь стояло
    `speech_ms_database_url or database_url`: без своей настройки речь молча
    шла в базу основного приложения. Здесь той базы нет, и подставлять вместо
    неё что-либо другое значило бы повторить ту же ошибку с другим адресом.
    Миграции отказывают так же и по той же причине — `alembic/env.py`.
    """
    url = (settings.speech_ms_database_url or "").strip()
    if not url:
        raise RuntimeError(
            "SPEECH_MS_DATABASE_URL is not set. Speech Analytics has its own "
            "database and there is deliberately no fallback."
        )
    try:
        event_loop = asyncio.get_running_loop()
    except RuntimeError:
        event_loop = None
    return _speech_ms_engine(
        url,
        settings.db_pool_size,
        settings.db_max_overflow,
        settings.db_pool_timeout_seconds,
        settings.db_pool_recycle_seconds,
        settings.db_write_trace_enabled,
        event_loop,
    )
