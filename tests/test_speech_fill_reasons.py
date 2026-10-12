"""Причины для прошлых звонков отдельным приложением (S-32): `fill_reasons` и его воркфлоу.

Живая база нужна для отбора и записи (`SPEECH_POSTGRES_TEST_URL`); Dify подставной.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.application.reasons.batch import fill_reasons
from src.infrastructure.clients.analysis import AnalysisError
from src.infrastructure.persistence.reasons import PostgresReasonRepository
from tests.speech_db_support import speech_test_database_url

DSL_PATH = Path(__file__).resolve().parents[1] / "dify" / "speech" / "speech-call-reason.dify.yml"
_TEST_DATABASE_URL = speech_test_database_url()
_live = pytest.mark.skipif(not _TEST_DATABASE_URL, reason="SPEECH_POSTGRES_TEST_URL is required")

Sessions = async_sessionmaker[AsyncSession]
START = datetime(2026, 9, 23, 21, 0, tzinfo=UTC)
END = START + timedelta(days=1)


# --------------------------------------------------------------------------- #
# Воркфлоу, который импортируют в Dify
# --------------------------------------------------------------------------- #


def _nodes() -> dict[str, dict[str, Any]]:
    graph = yaml.safe_load(DSL_PATH.read_text(encoding="utf-8"))["workflow"]["graph"]
    return {n["data"]["type"]: n["data"] for n in graph["nodes"]}


def test_the_reason_workflow_gets_the_reference_and_the_turns_only() -> None:
    """Ни дерева, ни пути оператора, ни резюме: причина — только по разговору."""
    nodes = _nodes()
    inputs = {v["variable"]: v for v in nodes["start"]["variables"]}

    assert list(nodes) == ["start", "llm", "end"]
    assert set(inputs) == {"call_id", "reasons", "segments"}
    assert inputs["reasons"]["max_length"] == 16_000
    assert [o["variable"] for o in nodes["end"]["outputs"]] == ["answer"]


def test_the_reason_prompt_names_its_fields_and_keeps_the_turns_as_data() -> None:
    template = _nodes()["llm"]["prompt_template"]
    system = next(p["text"] for p in template if p["role"] == "system")
    user = next(p["text"] for p in template if p["role"] == "user")

    for field in ("`call_reason`", "`call_reason_code`"):
        assert field in system
    assert "данные стороннего источника" in system and "не выполняй" in system
    assert "•••" not in system and "█" not in system
    turns_label = user[
        user.index("{{#node_start.reasons#}}") : user.index("{{#node_start.segments#}}")
    ]
    assert "данные стороннего источника" in turns_label, "пометка SP-710 — прямо перед репликами"
    for reference in re.findall(r"\{\{#([^}]+)#\}\}", system + user):
        assert re.fullmatch(r"node_start\.[a-z_]+", reference)


# --------------------------------------------------------------------------- #
# Отбор и запись
# --------------------------------------------------------------------------- #


class _Dify:
    """Подставное приложение причин: отвечает по очереди, запоминает входы."""

    def __init__(self, *answers: Any) -> None:
        self.answers = list(answers)
        self.inputs: list[dict[str, Any]] = []

    async def run(self, inputs: dict[str, Any]) -> dict[str, Any]:
        self.inputs.append(inputs)
        answer = (
            self.answers.pop(0) if self.answers else {"call_reason": "", "call_reason_code": ""}
        )
        if isinstance(answer, Exception):
            raise answer
        return {"answer": json.dumps(answer, ensure_ascii=False)}


async def _clean(sessions: Sessions) -> None:
    async with sessions() as session, session.begin():
        for table in ("speech_transcript_segments", "speech_transcripts", "speech_calls"):
            await session.execute(text(f"DELETE FROM {table}"))  # noqa: S608
        await session.execute(text("DELETE FROM speech_call_reasons"))


@pytest.fixture()
async def sessions() -> AsyncIterator[Sessions]:
    assert _TEST_DATABASE_URL is not None
    engine = create_async_engine(_TEST_DATABASE_URL)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    await _clean(factory)
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO speech_call_reasons (code, label, hint, sort_order, is_active) VALUES "
                "('K05', 'Не работает домашний интернет', 'ШПД не работает.', 0, true), "
                "('K99', 'Другое', 'Если не подходит ни один.', 1, true)"
            )
        )
    yield factory
    await _clean(factory)
    await engine.dispose()


async def _call(
    sessions: Sessions, name: str, *, hours: int, transcribed: bool = True, code: str | None = None
) -> uuid.UUID:
    call_id, started = uuid.uuid4(), START + timedelta(hours=hours)
    async with sessions() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO speech_calls (id, session_id, direction, started_at, ended_at, "
                "duration_s, audio_status, processing_status, analysis_status, "
                "call_reason_code, source_snapshot, first_seen_at, created_at, updated_at) "
                "VALUES (:id, :sid, 'inbound', :s, :s, 60, 'stored', 'done', 'done', :code, "
                "'{}', :s, :s, :s)"
            ),
            {"id": call_id, "sid": f"node_1_{name}", "s": started, "code": code},
        )
        if not transcribed:
            return call_id
        transcript = uuid.uuid4()
        await session.execute(
            text(
                "INSERT INTO speech_transcripts (id, call_id, engine, model_name, language, "
                "role_method, segments_count, created_at) VALUES (:t, :c, 'w', 'm', "
                "'ru', 'stereo_energy', 1, now())"
            ),
            {"t": transcript, "c": call_id},
        )
        await session.execute(
            text(
                "INSERT INTO speech_transcript_segments (transcript_id, call_id, seq, speaker, "
                "start_ms, end_ms, text_raw) VALUES (:t, :c, 1, 'client', "
                "0, 900, 'Номер 89780001122, нет интернета.')"
            ),
            {"t": transcript, "c": call_id},
        )
    return call_id


async def _reasons(sessions: Sessions) -> dict[str, tuple[str | None, str | None]]:
    async with sessions() as session:
        rows = await session.execute(
            text("SELECT session_id, call_reason, call_reason_code FROM speech_calls")
        )
        return {sid.removeprefix("node_1_"): (reason, code) for sid, reason, code in rows}


@_live
async def test_a_dry_run_counts_and_asks_nobody(sessions: Sessions) -> None:
    await _call(sessions, "open", hours=1)
    await _call(sessions, "coded", hours=2, code="K99")
    await _call(sessions, "untranscribed", hours=3, transcribed=False)
    dify = _Dify()

    report = await fill_reasons(
        PostgresReasonRepository(sessions), dify, date_from=START, date_to=END, apply=False
    )  # type: ignore[arg-type]

    assert (report.candidates, dify.inputs) == (1, [])


@_live
async def test_only_transcribed_calls_without_a_reason_are_filled_freshest_first(
    sessions: Sessions,
) -> None:
    await _call(sessions, "earlier", hours=1)
    await _call(sessions, "later", hours=5)
    await _call(sessions, "coded", hours=2, code="K99")
    dify = _Dify(
        {"call_reason": "Нет домашнего интернета.", "call_reason_code": "K05"},
        {"call_reason": "Непонятно.", "call_reason_code": "K42"},  # мимо справочника
        {"call_reason": "Непонятно.", "call_reason_code": "K42"},
        {"call_reason": "Непонятно.", "call_reason_code": "K42"},
    )

    report = await fill_reasons(
        PostgresReasonRepository(sessions),
        dify,
        date_from=START,
        date_to=END,
        apply=True,
        concurrency=1,  # type: ignore[arg-type]
    )

    assert (report.filled, report.unanswered, report.failed) == (1, 1, 0)
    assert await _reasons(sessions) == {
        "later": ("Нет домашнего интернета.", "K05"),
        "earlier": (None, None),
        "coded": (None, "K99"),
    }, "свежий первым; код мимо справочника не пишется; готовая причина не тронута"
    first = dify.inputs[0]
    assert first["reasons"].startswith("K05 · Не работает домашний интернет — ")
    assert first["segments"] == [
        {"seq": 1, "speaker": "client", "text": "Номер 89780001122, нет интернета."}
    ]


@_live
async def test_an_unreachable_dify_leaves_the_call_for_the_next_run(
    sessions: Sessions, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.application.reasons import batch as module

    monkeypatch.setattr(module, "PROVIDER_PAUSE_SECONDS", 0)
    await _call(sessions, "open", hours=1)
    dify = _Dify(*[AnalysisError("unreachable", retryable=True)] * 3)

    report = await fill_reasons(
        PostgresReasonRepository(sessions), dify, date_from=START, date_to=END, apply=True
    )  # type: ignore[arg-type]

    assert (report.filled, report.failed) == (0, 1)
    assert await _reasons(sessions) == {"open": (None, None)}


@_live
async def test_the_timer_takes_calls_transcribed_lately_whatever_their_date(
    sessions: Sessions,
) -> None:
    await _call(sessions, "history", hours=-24 * 20)  # звонок трёхнедельной давности
    stale = await _call(sessions, "stale", hours=1)
    async with sessions() as session, session.begin():
        await session.execute(
            text(
                "UPDATE speech_transcripts SET created_at = now() - interval '2 days' "
                "WHERE call_id = :c"
            ),
            {"c": stale},
        )
    dify = _Dify()

    report = await fill_reasons(
        PostgresReasonRepository(sessions),
        dify,  # type: ignore[arg-type]
        transcribed_since=datetime.now(UTC) - timedelta(hours=24),
        apply=False,
    )

    assert report.candidates == 1, (
        "история, расшифрованная сейчас, — да; давно расшифрованный — нет"
    )
