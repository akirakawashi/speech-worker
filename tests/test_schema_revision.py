"""Совместимость отдельного воркера с новой общей схемой без собственных миграций."""

from collections.abc import AsyncIterator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

from src.infrastructure.persistence.session import (
    SPEECH_REQUIRED_SCHEMA_REVISION,
    speech_schema_is_ready,
)
from tests.speech_db_support import speech_test_database_url

DATABASE_URL = speech_test_database_url()


@pytest.fixture
async def connection() -> AsyncIterator[AsyncConnection]:
    """Откатить изменения отметки схемы после теста; рабочие базы не допускаются."""
    if not DATABASE_URL:
        pytest.skip("SPEECH_POSTGRES_TEST_URL is required for worker schema proofs")
    engine = create_async_engine(DATABASE_URL, hide_parameters=True)
    try:
        async with engine.connect() as value, value.begin():
            try:
                yield value
            finally:
                await value.rollback()
    finally:
        await engine.dispose()


async def test_worker_accepts_the_new_backend_migration(connection) -> None:
    """Новая сборка воркера запускается после последней миграции бэкенда."""
    assert SPEECH_REQUIRED_SCHEMA_REVISION == "sp_0017_analysis_storage"
    async with AsyncSession(bind=connection) as session:
        assert await speech_schema_is_ready(session)


@pytest.mark.parametrize("revision", ["sp_0016_evaluation_jobs", "sp_unknown_future"])
async def test_worker_refuses_unsupported_revisions(connection, revision) -> None:
    """Добавочная миграция не заменяет проверку совместимости произвольным диапазоном."""
    await connection.execute(
        text("UPDATE alembic_version SET version_num = :revision"), {"revision": revision}
    )
    async with AsyncSession(bind=connection) as session:
        assert not await speech_schema_is_ready(session)
