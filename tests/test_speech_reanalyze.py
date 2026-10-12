"""Переанализ разобранных звонков на настоящем Postgres (SP-310, этап 3).

Проверяется то, ради чего команда: звонок без пути ИИ получает задание анализа в
полосе `backfill` и теряет прежнюю разметку, а звонки с путём ИИ, без
расшифровки, вне периода и с заданием в работе не тронуты.

Нужен `SPEECH_POSTGRES_TEST_URL` с применённой схемой.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.application.analysis.reanalyze import reanalyze
from src.application.ports.queue import ReprocessingConflict
from src.infrastructure.persistence import reanalyze as reanalyze_module
from src.infrastructure.persistence.reanalyze import PostgresReanalyzeRepository
from tests.speech_db_support import speech_test_database_url

_TEST_DATABASE_URL = speech_test_database_url()
pytestmark = pytest.mark.skipif(
    not _TEST_DATABASE_URL,
    reason="SPEECH_POSTGRES_TEST_URL is required",
)

START = datetime(2026, 9, 14, tzinfo=UTC)
END = START + timedelta(days=1)
NODE = uuid.uuid4()

Sessions = async_sessionmaker[AsyncSession]


async def _clean(sessions: Sessions) -> None:
    async with sessions() as session, session.begin():
        for table in (
            "speech_jobs",
            "speech_transcript_segments",
            "speech_transcripts",
            "speech_calls",
            "speech_call_reasons",
        ):
            await session.execute(text(f"DELETE FROM {table}"))  # noqa: S608
        for level in range(5, 0, -1):
            await session.execute(
                text("DELETE FROM speech_classifier_nodes WHERE level = :level"), {"level": level}
            )


@pytest.fixture()
async def sessions() -> AsyncIterator[Sessions]:
    assert _TEST_DATABASE_URL is not None
    engine = create_async_engine(_TEST_DATABASE_URL)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    await _clean(factory)
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO speech_classifier_nodes (id, parent_id, level, label, key, source, "
                "sort_order, is_active, first_seen_at, created_at, updated_at) "
                "VALUES (:id, NULL, 1, 'Физ.лицо', 'физ.лицо', 'file', 0, true, "
                "now(), now(), now())"
            ),
            {"id": NODE},
        )
        await session.execute(
            text(
                "INSERT INTO speech_call_reasons (code, label, hint, sort_order, is_active, "
                "created_at, updated_at) VALUES ('K05', 'Не работает домашний интернет', '', 0, "
                "true, now(), now())"
            )
        )
    yield factory
    await _clean(factory)
    await engine.dispose()


async def _call(
    sessions: Sessions,
    name: str,
    *,
    started: datetime = START + timedelta(hours=10),
    transcribed: bool = True,
    classified: bool = False,
    job: str | None = None,
    reason: str | None = None,
    analysis: str = "done",
) -> uuid.UUID:
    call_id = uuid.uuid4()
    async with sessions() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO speech_calls (id, session_id, direction, started_at, ended_at, "
                "duration_s, audio_status, processing_status, analysis_status, sentiment, "
                "summary, segment, segment_source, ai_node_id, call_reason_code, source_snapshot, "
                "first_seen_at, created_at, updated_at) "
                "VALUES (:id, :sid, 'inbound', :s, :s, 100, 'stored', 'done', :analysis, "
                "'negative', 'Старое резюме', 'b2b', 'llm', :node, :reason, '{}', :s, :s, :s)"
            ),
            {
                "id": call_id,
                "sid": f"node_1_domain_1_nauss_1_1_{name}",
                "s": started,
                "node": NODE if classified else None,
                "reason": reason,
                "analysis": analysis,
            },
        )
        if transcribed:
            await session.execute(
                text(
                    "INSERT INTO speech_transcripts (id, call_id, engine, model_name, language, "
                    "role_method, segments_count, created_at) "
                    "VALUES (gen_random_uuid(), :c, 'test', 'large-v3', 'ru', 'stereo_energy', "
                    "0, now())"
                ),
                {"c": call_id},
            )
        if job:
            await session.execute(
                text(
                    "INSERT INTO speech_jobs (id, call_id, kind, lane, status, priority, attempts, "
                    "max_attempts, run_after, lease_seconds, payload, created_at) "
                    "VALUES (gen_random_uuid(), :c, 'analyze', 'daily', :status, 0, 0, 3, now(), "
                    "300, '{}', now())"
                ),
                {"c": call_id, "status": job},
            )
    return call_id


async def _jobs(sessions: Sessions) -> list[tuple[str, str, str, str]]:
    async with sessions() as session:
        rows = await session.execute(
            text(
                "SELECT c.session_id, j.kind, j.lane, j.status FROM speech_jobs AS j "
                "JOIN speech_calls AS c ON c.id = j.call_id ORDER BY c.session_id"
            )
        )
        return [
            (str(session_id).rsplit("_", 1)[-1], kind, lane, status)
            for session_id, kind, lane, status in rows
        ]


async def test_a_dry_run_counts_only_calls_without_an_ai_path(sessions: Sessions) -> None:
    await _call(sessions, "old")
    await _call(sessions, "classified", classified=True)
    await _call(sessions, "untranscribed", transcribed=False)
    await _call(sessions, "busy", job="queued")
    await _call(sessions, "outside", started=START - timedelta(hours=1))

    report = await reanalyze(
        PostgresReanalyzeRepository(sessions), date_from=START, date_to=END, apply=False
    )

    assert (report.candidates, report.queued, report.applied) == (1, 0, False)
    assert await _jobs(sessions) == [("busy", "analyze", "daily", "queued")], "ничего не записано"


async def test_the_call_goes_to_the_backfill_lane_without_its_old_analysis(
    sessions: Sessions,
) -> None:
    old = await _call(sessions, "old")
    await _call(sessions, "busy", job="queued")

    report = await reanalyze(
        PostgresReanalyzeRepository(sessions), date_from=START, date_to=END, apply=True
    )

    assert (report.candidates, report.queued, report.busy, report.gone) == (1, 1, 0, 0)
    assert "поставлено на анализ: 1" in report.describe()
    assert await _jobs(sessions) == [
        ("busy", "analyze", "daily", "queued"),
        ("old", "analyze", "backfill", "queued"),
    ]
    async with sessions() as session:
        call = (
            (
                await session.execute(
                    text(
                        "SELECT processing_status, analysis_status, sentiment, summary, segment, "
                        "segment_source FROM speech_calls WHERE id = :id"
                    ),
                    {"id": old},
                )
            )
            .mappings()
            .one()
        )
    assert dict(call) == {
        "processing_status": "queued",
        "analysis_status": "pending",
        "sentiment": None,
        "summary": None,
        "segment": None,
        "segment_source": "unknown",
    }


async def test_the_freshest_calls_go_first_and_the_limit_holds(sessions: Sessions) -> None:
    await _call(sessions, "earlier", started=START + timedelta(hours=1))
    await _call(sessions, "later", started=START + timedelta(hours=20))

    report = await reanalyze(
        PostgresReanalyzeRepository(sessions), date_from=START, date_to=END, apply=True, limit=1
    )

    assert (report.candidates, report.queued) == (1, 1)
    assert [job[0] for job in await _jobs(sessions)] == ["later"]


async def test_a_call_taken_by_the_pipeline_meanwhile_is_skipped(
    sessions: Sessions, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _call(sessions, "old")

    async def _busy(*_args: Any, **_kwargs: Any) -> uuid.UUID:
        raise ReprocessingConflict("call_has_queued_or_running_jobs")

    monkeypatch.setattr(reanalyze_module, "reprocess", _busy)

    report = await reanalyze(
        PostgresReanalyzeRepository(sessions), date_from=START, date_to=END, apply=True
    )

    assert (report.candidates, report.queued, report.busy) == (1, 0, 1)
    assert await _jobs(sessions) == []


async def test_calls_analysed_before_the_reasons_are_found_by_the_missing_reason(
    sessions: Sessions,
) -> None:
    """S-32: `--missing reason` берёт разобранные звонки без кода причины."""
    await _call(sessions, "noreason", classified=True)
    await _call(sessions, "reasoned", classified=True, reason="K05")
    await _call(sessions, "pending", classified=True, analysis="pending")

    dry = await reanalyze(
        PostgresReanalyzeRepository(sessions),
        date_from=START,
        date_to=END,
        apply=False,
        missing="reason",
    )
    report = await reanalyze(
        PostgresReanalyzeRepository(sessions),
        date_from=START,
        date_to=END,
        apply=True,
        missing="reason",
    )

    assert dry.candidates == 1
    assert "без причины обращения: 1" in dry.describe()
    assert (report.candidates, report.queued) == (1, 1)
    assert await _jobs(sessions) == [("noreason", "analyze", "backfill", "queued")]
