"""SQL пакетной оценки и атомарная проверка исходных реплик при записи."""

from __future__ import annotations

import json
import uuid
from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.application.dto.quality import CURRENT_SCRIPT, CallScript
from src.application.dto.quality_batch import (
    CallRef,
    CandidateSelection,
    LineSnapshot,
)
from src.application.dto.quality_execution import QualityTotals
from src.application.quality.evaluation import (
    Evaluation,
    QualityError,
    TranscriptLine,
)
from src.infrastructure.persistence.quality_transcript import DROP_EVALUATIONS_SQL, FINGERPRINT_SQL

_CANDIDATES_SQL = text(
    """
    SELECT c.id, c.session_id
      FROM speech_calls AS c
      JOIN speech_transcripts AS t ON t.call_id = c.id
     WHERE c.source = :source
       AND (CAST(:date_from AS timestamptz) IS NULL OR c.started_at >= :date_from)
       AND (CAST(:date_to   AS timestamptz) IS NULL OR c.started_at <  :date_to)
       AND (CAST(:force AS boolean) OR NOT EXISTS (
            SELECT 1 FROM speech_call_evaluations AS e
             WHERE e.call_id = c.id
               AND e.script_version = :script_version
               AND e.evaluated_at >= t.created_at
               AND (e.failure_reason IS NULL OR CAST(:skip_failed AS boolean))
       ))
     ORDER BY c.started_at DESC, c.id DESC
     LIMIT :limit
    """
)

_CALL_SQL = text(
    """
    SELECT c.id, c.session_id, c.source, t.id AS transcript_id
      FROM speech_calls AS c
      LEFT JOIN speech_transcripts AS t ON t.call_id = c.id
     WHERE c.session_id = :ref OR CAST(c.id AS text) = :ref
    """
)

_SEGMENTS_SQL = text(
    """
    SELECT seq, speaker, start_ms, text_raw
      FROM speech_transcript_segments
     WHERE call_id = :call_id
     ORDER BY seq
    """
)

_READ_AT_SQL = text("SELECT clock_timestamp()")

_LOCK_TRANSCRIPT_SQL = text(
    "SELECT created_at, role_method FROM speech_transcripts WHERE call_id = :call_id FOR UPDATE"
)

_LOCK_SEGMENTS_SQL = text(str(_SEGMENTS_SQL) + " FOR UPDATE")

_UPDATE_ROLE_SQL = text(
    "UPDATE speech_transcript_segments SET speaker = :speaker WHERE "
    "call_id = :call_id AND seq = :seq"
)

_MARK_LLM_ROLES_SQL = text(
    "UPDATE speech_transcripts SET role_method = 'llm_attribution' WHERE call_id = :call_id"
)

_STORE_SQL = text(
    f"""
    INSERT INTO speech_call_evaluations
        (id, call_id, script_version, model_name, script_applies, score, passed_count,
         stage_count, all_passed, outcome, stages, objections, summary, evaluated_at,
         transcript_fingerprint, failure_reason)
    VALUES (gen_random_uuid(), :call_id, :script_version, :model_name, :script_applies,
            :score, :passed_count, :stage_count, :all_passed, :outcome,
            CAST(:stages AS jsonb), CAST(:objections AS jsonb), :summary, :evaluated_at,
            {FINGERPRINT_SQL.format(call_id=":call_id")}, :failure_reason)
    ON CONFLICT (call_id, script_version) DO UPDATE
       SET model_name = EXCLUDED.model_name,
           script_applies = EXCLUDED.script_applies,
           score = EXCLUDED.score,
           passed_count = EXCLUDED.passed_count,
           stage_count = EXCLUDED.stage_count,
           all_passed = EXCLUDED.all_passed,
           outcome = EXCLUDED.outcome,
           stages = EXCLUDED.stages,
           objections = EXCLUDED.objections,
           summary = EXCLUDED.summary,
           evaluated_at = EXCLUDED.evaluated_at,
           transcript_fingerprint = EXCLUDED.transcript_fingerprint,
           failure_reason = EXCLUDED.failure_reason
    """
)

_STALE = f"""
      FROM speech_call_evaluations AS e
     WHERE e.transcript_fingerprint IS DISTINCT FROM
           {FINGERPRINT_SQL.format(call_id="e.call_id")}
"""

_COUNT_STALE_SQL = text(f"SELECT count(*) {_STALE}")

_DROP_STALE_SQL = text(f"DELETE {_STALE} RETURNING e.id")


async def find_candidates(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    date_from: datetime | None,
    date_to: datetime | None,
    force: bool = False,
    skip_failed: bool = False,
    limit: int | None = None,
    script: CallScript = CURRENT_SCRIPT,
) -> list[CallRef]:
    """Звонки, которые команда оценила бы, свежие первыми.

    Граница периода `None` — без границы. `skip_failed` убирает звонки, чья
    оценка по этому же тексту уже не удалась: расписание не должно возвращаться
    к ним каждым запуском.
    """
    async with sessionmaker() as session:
        rows = await session.execute(
            _CANDIDATES_SQL,
            {
                "source": script.source.value,
                "date_from": date_from,
                "date_to": date_to,
                "force": force,
                "skip_failed": skip_failed,
                "script_version": script.version,
                "limit": limit,
            },
        )
        return [
            CallRef(call_id=uuid.UUID(str(row[0])), session_id=str(row[1])) for row in rows.all()
        ]


async def find_call(
    sessionmaker: async_sessionmaker[AsyncSession], ref: str, *, script: CallScript = CURRENT_SCRIPT
) -> CallRef:
    """Один звонок по идентификатору сессии или UUID. `LookupError`, если оценить нечего."""
    async with sessionmaker() as session:
        row = (await session.execute(_CALL_SQL, {"ref": ref.strip()})).mappings().first()
    if row is None:
        raise LookupError(f"call {ref} is not in the database")
    if row["source"] != script.source.value:
        raise LookupError(
            f"call {ref} is a {row['source']} call; the script scores {script.source.value} calls"
        )
    if row["transcript_id"] is None:
        raise LookupError(f"call {ref} has no transcript yet")
    return CallRef(call_id=uuid.UUID(str(row["id"])), session_id=str(row["session_id"]))


async def load_lines(
    sessionmaker: async_sessionmaker[AsyncSession], call_id: uuid.UUID
) -> tuple[list[TranscriptLine], datetime]:
    """Реплики звонка и момент, когда они прочитаны.

    Момент берётся **до** чтения и становится временем оценки. Модель думает
    десятки секунд, и звонок за это время могут переобработать: оценка,
    помеченная временем записи, выглядела бы свежее новых реплик, хотя
    поставлена по старым.
    """
    async with sessionmaker() as session:
        read_at: datetime = (await session.execute(_READ_AT_SQL)).scalar_one()
        rows = (await session.execute(_SEGMENTS_SQL, {"call_id": call_id})).mappings()
        lines = [
            TranscriptLine(
                seq=int(row["seq"]),
                speaker=str(row["speaker"]),
                start_ms=int(row["start_ms"]),
                text=str(row["text_raw"]),
            )
            for row in rows
        ]
    return (lines, read_at)


async def _check_input(
    session: AsyncSession, call_id: uuid.UUID, lines: Sequence[TranscriptLine], read_at: datetime
) -> str:
    """Короткая блокировка при записи: модель работает без открытой транзакции."""
    transcript = (
        (await session.execute(_LOCK_TRANSCRIPT_SQL, {"call_id": call_id})).mappings().first()
    )
    if transcript is None or transcript["created_at"] > read_at:
        raise QualityError("the transcript changed during evaluation", code="transcript_changed")
    rows = (await session.execute(_LOCK_SEGMENTS_SQL, {"call_id": call_id})).mappings()
    current = [
        TranscriptLine(
            seq=int(row["seq"]),
            speaker=str(row["speaker"]),
            start_ms=int(row["start_ms"]),
            text=str(row["text_raw"]),
        )
        for row in rows
    ]
    if current != list(lines):
        raise QualityError("the transcript changed during evaluation", code="transcript_changed")
    return str(transcript["role_method"])


async def _save_roles(
    session: AsyncSession, call_id: uuid.UUID, roles: Sequence[tuple[int, str]], role_method: str
) -> None:
    """Роли, названные моделью, — в расшифровку. Только под блокировкой `_check_input`."""
    if not roles:
        return
    await session.execute(
        _UPDATE_ROLE_SQL,
        [{"call_id": call_id, "seq": seq, "speaker": speaker} for seq, speaker in roles],
    )
    if role_method == "none":
        await session.execute(_MARK_LLM_ROLES_SQL, {"call_id": call_id})


async def store_evaluation(
    session: AsyncSession,
    call_id: uuid.UUID,
    evaluation: Evaluation,
    *,
    evaluated_at: datetime,
    source_lines: Sequence[TranscriptLine],
    totals: QualityTotals | None = None,
) -> None:
    """Атомарно записать оценку в текущей транзакции после проверки её исходных данных."""
    role_method = await _check_input(session, call_id, source_lines, evaluated_at)
    await _save_roles(session, call_id, evaluation.speaker_roles, role_method)
    await session.execute(
        _STORE_SQL,
        {
            "call_id": call_id,
            "script_version": evaluation.script_version,
            "model_name": evaluation.model_name,
            "script_applies": evaluation.script_applies,
            "score": totals.score if totals is not None else evaluation.score,
            "passed_count": totals.passed_count if totals is not None else evaluation.passed_count,
            "stage_count": totals.stage_count if totals is not None else evaluation.stage_count,
            "all_passed": totals.all_passed if totals is not None else evaluation.all_passed,
            "outcome": evaluation.outcome,
            "stages": json.dumps([stage.as_json() for stage in evaluation.stages]),
            "objections": json.dumps([item.as_json() for item in evaluation.objections]),
            "summary": evaluation.summary,
            "evaluated_at": evaluated_at,
            "failure_reason": None,
        },
    )


_STORE_FAILURE_SQL = text(
    str(_STORE_SQL)
    + """
     WHERE speech_call_evaluations.failure_reason IS NOT NULL
        OR speech_call_evaluations.evaluated_at < (
               SELECT created_at FROM speech_transcripts WHERE call_id = EXCLUDED.call_id
           )
    """
)


async def _store_failure(
    sessionmaker: async_sessionmaker[AsyncSession],
    call_id: uuid.UUID,
    lines: Sequence[TranscriptLine],
    read_at: datetime,
    model_name: str,
    script: CallScript,
    reason: str,
) -> None:
    """Сохранить нормализованный отказ оценки без текста разговора."""
    async with sessionmaker() as session, session.begin():
        try:
            await _check_input(session, call_id, lines, read_at)
        except QualityError:
            return
        await store_failure_result(
            session,
            call_id,
            evaluated_at=read_at,
            model_name=model_name,
            script_version=script.version,
            reason=reason,
        )


async def store_failure_result(
    session: AsyncSession,
    call_id: uuid.UUID,
    *,
    evaluated_at: datetime,
    model_name: str,
    script_version: str,
    reason: str,
) -> None:
    """Записать проверенный отказ в транзакции вызывающего, сохранив свежую успешную оценку.

    Вызывающий сначала блокирует и проверяет исходные реплики. Текст разговора
    и ответа модели в причину не включаются; коммит принадлежит вызывающему.
    """
    await session.execute(
        _STORE_FAILURE_SQL,
        {
            "call_id": call_id,
            "script_version": script_version,
            "model_name": model_name,
            "script_applies": None,
            "score": None,
            "passed_count": None,
            "stage_count": None,
            "all_passed": None,
            "outcome": None,
            "stages": "[]",
            "objections": "[]",
            "summary": None,
            "evaluated_at": evaluated_at,
            "failure_reason": reason,
        },
    )


async def drop_stale(sessionmaker: async_sessionmaker[AsyncSession], *, apply: bool) -> int:
    """Сколько оценок разошлось с расшифровкой; с `apply` они удаляются.

    Удалённая оценка — не потеря: звонок снова ждёт оценки, и следующий запуск
    команды ставит её по нынешним репликам.
    """
    async with sessionmaker() as session, session.begin():
        if not apply:
            return int((await session.execute(_COUNT_STALE_SQL)).scalar_one())
        return len((await session.execute(_DROP_STALE_SQL)).all())


class PostgresEvaluationRepository:
    """Выполнять короткие транзакции оценки; вызов модели происходит снаружи."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        """Принять фабрику сессий выделенной базы речи."""
        self._sessions = sessions

    async def find_candidates(self, selection: CandidateSelection) -> list[CallRef]:
        """Передать типизированный охват SQL-отбору."""
        return await find_candidates(
            self._sessions,
            date_from=selection.date_from,
            date_to=selection.date_to,
            force=selection.force,
            skip_failed=selection.skip_failed,
            limit=selection.limit,
            script=selection.script,
        )

    async def find_call(self, ref: str, script: CallScript = CURRENT_SCRIPT) -> CallRef:
        """Найти один звонок по ссылке, проверив источник и расшифровку."""
        return await find_call(self._sessions, ref, script=script)

    async def load_lines(self, call_id: uuid.UUID) -> LineSnapshot:
        """Прочитать реплики и серверный момент; сразу закрыть сессию."""
        lines, read_at = await load_lines(self._sessions, call_id)
        return LineSnapshot(lines=tuple(lines), read_at=read_at)

    async def save_roles(
        self, call_id: uuid.UUID, snapshot: LineSnapshot, prepared: Sequence[TranscriptLine]
    ) -> None:
        """Под блокировкой проверить снимок, обновить роли и снять оценку."""
        async with self._sessions() as session, session.begin():
            method = await _check_input(session, call_id, snapshot.lines, snapshot.read_at)
            await _save_roles(
                session, call_id, [(line.seq, line.speaker) for line in prepared], method
            )
            await session.execute(DROP_EVALUATIONS_SQL, {"call_id": call_id})

    async def save_evaluation(
        self, call_id: uuid.UUID, evaluation: Evaluation, snapshot: LineSnapshot
    ) -> None:
        """Проверить снимок и записать оценку одной короткой транзакцией."""
        async with self._sessions() as session, session.begin():
            await store_evaluation(
                session,
                call_id,
                evaluation,
                evaluated_at=snapshot.read_at,
                source_lines=snapshot.lines,
            )

    async def save_failure(
        self,
        call_id: uuid.UUID,
        snapshot: LineSnapshot,
        model_name: str,
        script: CallScript,
        reason: str,
    ) -> None:
        """Сохранить отказ, если его вход ещё действует и успешная оценка не новее."""
        await _store_failure(
            self._sessions, call_id, snapshot.lines, snapshot.read_at, model_name, script, reason
        )

    async def drop_stale(self, *, apply: bool) -> int:
        """Сосчитать либо удалить несовпадающие оценки."""
        return await drop_stale(self._sessions, apply=apply)
