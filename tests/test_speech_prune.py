"""Удаление по сроку хранения.

Единственная необратимая задача проекта, поэтому проверяется не «удалилось ли»,
а четыре вещи, каждая из которых уже однажды была причиной неверного отчёта об
уничтожении персональных данных:

* строка не помечается удалённой раньше, чем хранилище подтвердило удаление;
* отказ хранилища останавливает прогон, а не пропускается;
* прогон повторяем — прерванный доводится следующим до конца;
* режим проверки не трогает ничего.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.application.dto.retention import ExpiredRecording
from src.application.errors.retention import PruneAborted
from src.application.retention.service import (
    delete_audio_batch,
    prune_audio,
    speech_policies,
    verify_bucket,
)
from src.di import retention as retention_di
from src.domain.calls.contracts import SPEECH_DEFAULT_SETTINGS, AudioStatus
from src.domain.retention.policy import SPEECH_DELETE_ORDER
from src.infrastructure.persistence.retention import (
    PRUNE_ACTOR,
    PostgresRetentionPersistence,
    PostgresRetentionRepository,
)
from src.infrastructure.persistence.settings_store import (
    UnknownSetting,
    read_positive_int,
    read_setting,
)
from src.infrastructure.storage.recordings import StorageError
from tests.speech_db_support import speech_test_database_url

_TEST_DATABASE_URL = speech_test_database_url()
OLD = datetime.now(UTC) - timedelta(days=400)
RECENT = datetime.now(UTC) - timedelta(days=3)


class FakeStorage:
    """Хранилище, отвечающее так, как отвечает настоящее.

    Отсутствующий объект — `True` (это `204`), пропавшая корзина — исключение.
    Настоящее хранилище проверяется отдельно, на живом MinIO; здесь важно, что
    задача **делает** с каждым из двух ответов.
    """

    def __init__(self, *, fails_on: str | None = None, bucket_missing: bool = False) -> None:
        self.deleted: list[str] = []
        self.fails_on = fails_on
        self.bucket_missing = bucket_missing

    async def delete(self, key: str) -> bool:
        if self.bucket_missing:
            raise StorageError("storage has no bucket 'speech-recordinsg'")
        if key == self.fails_on:
            raise StorageError(f"storage refused the delete of {key}: HTTP 500")
        self.deleted.append(key)
        return True


class RecordingSession:
    """Сессия, которая помнит, о чём её просили, и ничего не делает."""

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.commits = 0

    async def execute(self, statement: Any, params: Any = None) -> None:
        self.statements.append(str(statement))

    async def commit(self) -> None:
        self.commits += 1


def _row(key: str | None, *, session_id: str = "node_1") -> dict[str, Any]:
    return {
        "id": uuid.uuid4(),
        "session_id": session_id,
        "started_at": OLD,
        "audio_object_key": key,
        "audio_bytes": 1024,
    }


# --------------------------------------------------------------------------- #
# Без базы
# --------------------------------------------------------------------------- #


async def test_a_missing_bucket_stops_the_run_before_anything_is_deleted() -> None:
    """Опечатка в имени корзины однажды дала отчёт об удалении, которого не было."""
    with pytest.raises(PruneAborted) as exc:
        await verify_bucket(FakeStorage(bucket_missing=True))  # type: ignore[arg-type]

    assert "корзина недоступна" in str(exc.value)


async def test_the_probe_deletes_nothing_real() -> None:
    """Проба — `DELETE` по ключу, которого в корзине быть не может."""
    storage = FakeStorage()

    await verify_bucket(storage)  # type: ignore[arg-type]

    assert len(storage.deleted) == 1
    assert "retention-probe" in storage.deleted[0]


async def test_a_storage_refusal_stops_the_batch_and_marks_nothing() -> None:
    """Пропустив отказ, мы получили бы строку «удалено» над лежащим файлом."""
    session = RecordingSession()
    rows = [_row("calls/a.mp3"), _row("calls/b.mp3")]
    storage = FakeStorage(fails_on="calls/b.mp3")

    with pytest.raises(StorageError):
        await delete_audio_batch(
            PostgresRetentionPersistence(session),
            storage,
            [ExpiredRecording.model_validate(row) for row in rows],
        )  # type: ignore[arg-type]

    assert storage.deleted == ["calls/a.mp3"], "первый удалён — это факт, его не отменить"
    assert session.statements == [], "но ни одна строка не помечена"
    assert session.commits == 0


async def test_a_row_claiming_stored_audio_without_a_key_is_reported_not_marked() -> None:
    """Пометить её удалённой значило бы записать в журнал удаление, которого не было."""
    session = RecordingSession()
    storage = FakeStorage()

    deleted, freed, inconsistent = await delete_audio_batch(
        PostgresRetentionPersistence(session),
        storage,
        [ExpiredRecording.model_validate(row) for row in [_row(None, session_id="node_broken")]],
    )

    assert deleted == []
    assert freed == 0
    assert inconsistent == ["node_broken"]
    assert storage.deleted == []
    assert session.commits == 0


async def test_the_job_is_recorded_as_a_job_not_as_an_administrator() -> None:
    """Журнал, где ночная задача неотличима от человека, ответа не даёт."""
    assert PRUNE_ACTOR.sub == "system:speech-prune"
    assert PRUNE_ACTOR.roles == ()


def test_rows_are_deleted_children_before_parents() -> None:
    assert [policy.table for policy in speech_policies()] == list(SPEECH_DELETE_ORDER)


def test_the_audit_window_is_a_year_and_marked_as_compliance() -> None:
    """Срок журнала — не про место на диске; сократить его молча нельзя."""
    audit = next(p for p in speech_policies() if p.table == "speech_audit_events")

    assert audit.days == 365
    assert audit.basis == "compliance"


def test_a_job_without_a_finish_time_is_never_pruned() -> None:
    """Незавершённое задание — это работа, а не старьё."""
    jobs = next(p for p in speech_policies() if p.table == "speech_jobs")

    assert jobs.completion_column == "finished_at"


# --------------------------------------------------------------------------- #
# Настройки
# --------------------------------------------------------------------------- #


class _SettingSession:
    def __init__(self, stored: Any) -> None:
        self.stored = stored

    async def execute(self, *_a: Any, **_k: Any) -> Any:
        stored = self.stored

        class _Result:
            def scalar_one_or_none(self) -> Any:
                return stored

        return _Result()


async def test_an_undeclared_setting_is_refused() -> None:
    """Опечатка в имени иначе вернула бы умолчание несуществующей настройки."""
    with pytest.raises(UnknownSetting):
        await read_setting(_SettingSession(None), "retenion_days")  # type: ignore[arg-type]


async def test_a_missing_row_gives_the_declared_default() -> None:
    value = await read_setting(_SettingSession(None), "retention_days")  # type: ignore[arg-type]

    assert value == SPEECH_DEFAULT_SETTINGS["retention_days"] == 180


@pytest.mark.parametrize("broken", ["180", 0, -5, None, True])
async def test_a_retention_window_that_is_not_a_positive_number_falls_back(broken: Any) -> None:
    """Срок `0` — это «удалить всё сегодня». Принять его из базы молча нельзя."""
    value = await read_positive_int(_SettingSession(broken), "retention_days")  # type: ignore[arg-type]

    assert value == 180


async def test_an_administrator_value_is_honoured() -> None:
    value = await read_positive_int(_SettingSession(30), "retention_days")  # type: ignore[arg-type]

    assert value == 30


# --------------------------------------------------------------------------- #
# На живой базе
# --------------------------------------------------------------------------- #


@pytest.fixture()
async def db() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    if not _TEST_DATABASE_URL:
        pytest.skip("SPEECH_POSTGRES_TEST_URL is required for the retention proof")
    engine = create_async_engine(_TEST_DATABASE_URL)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await _clean(sessions)
    yield sessions
    await _clean(sessions)
    await engine.dispose()


async def _clean(sessions: async_sessionmaker[AsyncSession]) -> None:
    async with sessions() as session, session.begin():
        await session.execute(text("DELETE FROM speech_audit_events"))
        await session.execute(text("DELETE FROM speech_calls"))
        await session.execute(text("DELETE FROM speech_settings WHERE key = 'retention_days'"))


async def _seed(sessions: async_sessionmaker[AsyncSession]) -> dict[str, uuid.UUID]:
    ids = {"old": uuid.uuid4(), "recent": uuid.uuid4(), "gone": uuid.uuid4()}
    async with sessions() as session, session.begin():
        for name, when, status, key in (
            ("old", OLD, AudioStatus.STORED.value, "calls/2025/07/old.mp3"),
            ("recent", RECENT, AudioStatus.STORED.value, "calls/2026/09/recent.mp3"),
            ("gone", OLD, AudioStatus.UNAVAILABLE_AT_SOURCE.value, None),
        ):
            await session.execute(
                text(
                    "INSERT INTO speech_calls (id, session_id, direction, started_at, ended_at, "
                    "duration_s, audio_status, audio_object_key, audio_bytes, processing_status, "
                    "analysis_status, source_snapshot, first_seen_at, created_at, updated_at) "
                    "VALUES (:id, :sid, 'inbound', :s, :s, 100, :st, :key, 2048, 'done', 'done', "
                    "'{}', :s, :s, :s)"
                ),
                {
                    "id": ids[name],
                    "sid": f"node_1_prune_{name}",
                    "s": when,
                    "st": status,
                    "key": key,
                },
            )
    return ids


async def _call(sessions: async_sessionmaker[AsyncSession], call_id: uuid.UUID) -> dict[str, Any]:
    async with sessions() as session:
        row = (
            (
                await session.execute(
                    text(
                        "SELECT audio_status, audio_object_key, audio_deleted_at "
                        "FROM speech_calls WHERE id = :id"
                    ),
                    {"id": call_id},
                )
            )
            .mappings()
            .one()
        )
    return dict(row)


async def _events(sessions: async_sessionmaker[AsyncSession]) -> list[dict[str, Any]]:
    async with sessions() as session:
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT event_type, actor_sub, call_id, payload FROM speech_audit_events "
                        "ORDER BY occurred_at"
                    )
                )
            )
            .mappings()
            .all()
        )
    return [dict(row) for row in rows]


class _Settings:
    speech_storage_base_url = "http://minio.invalid:9000"
    speech_storage_bucket = "speech-recordings"


async def test_the_dry_run_touches_nothing(db) -> None:
    ids = await _seed(db)
    storage = FakeStorage()

    removed, freed, bad = await prune_audio(
        PostgresRetentionRepository(db),
        lambda: retention_di.build_storage(_Settings()),
        apply=False,
        batch_size=10,  # type: ignore[arg-type]
    )

    assert (removed, freed, bad) == (0, 0, [])
    assert storage.deleted == []
    assert (await _call(db, ids["old"]))["audio_status"] == AudioStatus.STORED.value
    assert await _events(db) == []


async def test_apply_deletes_the_object_first_then_marks_the_row(db, monkeypatch) -> None:
    ids = await _seed(db)
    storage = FakeStorage()
    monkeypatch.setattr("src.di.retention.build_storage", lambda _s: storage)

    removed, freed, bad = await prune_audio(
        PostgresRetentionRepository(db),
        lambda: retention_di.build_storage(_Settings()),
        apply=True,
        batch_size=10,  # type: ignore[arg-type]
    )

    assert removed == 1 and bad == []
    assert freed == 2048
    assert "calls/2025/07/old.mp3" in storage.deleted

    old = await _call(db, ids["old"])
    assert old["audio_status"] == AudioStatus.DELETED_BY_RETENTION.value
    assert old["audio_object_key"] is None
    assert old["audio_deleted_at"] is not None

    recent = await _call(db, ids["recent"])
    assert recent["audio_status"] == AudioStatus.STORED.value, "срок ещё не вышел"


async def test_the_deletion_is_recorded_in_the_journal(db, monkeypatch) -> None:
    ids = await _seed(db)
    monkeypatch.setattr("src.di.retention.build_storage", lambda _s: FakeStorage())

    await prune_audio(
        PostgresRetentionRepository(db),
        lambda: retention_di.build_storage(_Settings()),
        apply=True,
        batch_size=10,
    )  # type: ignore[arg-type]

    events = await _events(db)
    assert [e["event_type"] for e in events] == ["audio.retention_delete"]
    assert events[0]["call_id"] == ids["old"]
    assert events[0]["actor_sub"] == "system:speech-prune"
    assert events[0]["payload"]["object_key"] == "calls/2025/07/old.mp3"


async def test_a_second_run_finds_nothing_left(db, monkeypatch) -> None:
    """Прерванный прогон доводится следующим, а повторный ничего не портит."""
    await _seed(db)
    monkeypatch.setattr("src.di.retention.build_storage", lambda _s: FakeStorage())
    await prune_audio(
        PostgresRetentionRepository(db),
        lambda: retention_di.build_storage(_Settings()),
        apply=True,
        batch_size=10,
    )  # type: ignore[arg-type]

    removed, freed, bad = await prune_audio(
        PostgresRetentionRepository(db),
        lambda: retention_di.build_storage(_Settings()),
        apply=True,
        batch_size=10,  # type: ignore[arg-type]
    )

    assert (removed, freed, bad) == (0, 0, [])
    assert len(await _events(db)) == 1, "второй прогон не пишет второе событие"


async def test_a_missing_bucket_leaves_every_row_alone(db, monkeypatch) -> None:
    """Главная проверка: отчёт об удалении не выдаётся, если корзина не та."""
    ids = await _seed(db)
    monkeypatch.setattr(
        "src.di.retention.build_storage", lambda _s: FakeStorage(bucket_missing=True)
    )

    with pytest.raises(PruneAborted):
        await prune_audio(
            PostgresRetentionRepository(db),
            lambda: retention_di.build_storage(_Settings()),
            apply=True,
            batch_size=10,
        )  # type: ignore[arg-type]

    assert (await _call(db, ids["old"]))["audio_status"] == AudioStatus.STORED.value
    assert await _events(db) == [], "не удалили — не записали"


async def test_an_administrator_shortened_window_is_obeyed(db, monkeypatch) -> None:
    """Срок читается из `speech_settings`, а не из умолчания в коде."""
    ids = await _seed(db)
    async with db() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO speech_settings (key, value, updated_at) "
                "VALUES ('retention_days', '1'::jsonb, now())"
            )
        )
    monkeypatch.setattr("src.di.retention.build_storage", lambda _s: FakeStorage())

    removed, _freed, _bad = await prune_audio(
        PostgresRetentionRepository(db),
        lambda: retention_di.build_storage(_Settings()),
        apply=True,
        batch_size=10,  # type: ignore[arg-type]
    )

    assert removed == 2, "теперь просрочен и трёхдневный звонок"
    assert (await _call(db, ids["recent"]))["audio_status"] == (
        AudioStatus.DELETED_BY_RETENTION.value
    )
