"""SQL чтения и записи результата этапа конвейера."""

from __future__ import annotations

import json
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.dto.calls import AnalysisStatus, RoleMethod
from src.application.dto.pipeline import AnalyzeWork
from src.application.dto.pipeline_input import (
    AnalysisInput,
    AnalysisSegment,
    CallForAnalysis,
    ClassifierRecord,
)
from src.application.dto.reasons import CallReason
from src.application.ports.queue import ClaimedJob
from src.infrastructure.persistence.quality_transcript import DROP_EVALUATIONS_SQL

_LOAD_CALL_SQL = text(
    """
    SELECT c.id, t.role_method
      FROM speech_calls AS c
      LEFT JOIN speech_transcripts AS t ON t.call_id = c.id
     WHERE c.id = :call_id
    """
)


_LOAD_SEGMENTS_SQL = text(
    """
    SELECT seq, speaker, text_raw
      FROM speech_transcript_segments
     WHERE call_id = :call_id
     ORDER BY seq
    """
)


_LOAD_CLASSIFIER_SQL = text(
    "SELECT id, parent_id, level, label, key, sort_order, is_active FROM speech_classifier_nodes"
)


_LOAD_REASONS_SQL = text(
    "SELECT code, label, hint FROM speech_call_reasons WHERE is_active ORDER BY sort_order, code"
)


_STORE_ANALYSIS_SQL = text(
    """
    UPDATE speech_calls
       SET ai_node_id = CAST(:ai_node_id AS uuid),
           sentiment = :sentiment,
           summary = :summary,
           ai_suggestion = CAST(:ai_suggestion AS jsonb),
           ai_suggestion_node_id = CAST(:ai_suggestion_node_id AS uuid),
           call_reason = :call_reason,
           call_reason_code = :call_reason_code,
           analysis_status = :analysis_status,
           segment = CASE WHEN segment_source = 'project' THEN segment ELSE :segment END,
           segment_source = CASE
               WHEN segment_source = 'project' THEN segment_source
               WHEN :segment IS NULL THEN segment_source
               ELSE 'llm'
           END,
           product = CASE WHEN product_source = 'project' THEN product ELSE :product END,
           product_source = CASE
               WHEN product_source = 'project' THEN product_source
               WHEN :product IS NULL THEN product_source
               ELSE 'llm'
           END,
           updated_at = now()
     WHERE id = :call_id
    """
)


_UPDATE_SPEAKER_SQL = text(
    """
    UPDATE speech_transcript_segments
       SET speaker = :speaker
     WHERE call_id = :call_id AND seq = :seq
    """
)


_SET_ROLE_METHOD_SQL = text(
    "UPDATE speech_transcripts SET role_method = :role_method WHERE call_id = :call_id"
)


class AnalyzeStatements:
    """Сохранение результата в транзакции вызывающего."""

    async def store(self, session: AsyncSession, job: ClaimedJob, work: AnalyzeWork) -> None:
        """Сохранить результат этапа в переданной транзакции без самостоятельного commit."""
        await session.execute(
            _STORE_ANALYSIS_SQL,
            {
                "call_id": job.call_id,
                "ai_node_id": work.node_id,
                "sentiment": work.sentiment.value if work.sentiment else None,
                "summary": work.summary,
                "ai_suggestion": (
                    json.dumps(list(work.suggestion), ensure_ascii=False)
                    if work.suggestion
                    else None
                ),
                "ai_suggestion_node_id": work.suggestion_node_id,
                "call_reason": work.call_reason,
                "call_reason_code": work.call_reason_code,
                "analysis_status": AnalysisStatus.DONE.value,
                "segment": work.segment,
                "product": work.product,
            },
        )
        for seq, speaker in work.roles:
            await session.execute(
                _UPDATE_SPEAKER_SQL,
                {"call_id": job.call_id, "seq": seq, "speaker": speaker.value},
            )
        if work.roles:
            # Метод ролей меняется вместе с ролями: иначе транскрипт утверждал
            # бы, что роли получены энергией каналов, которой у моно-записи нет.
            await session.execute(
                _SET_ROLE_METHOD_SQL,
                {"call_id": job.call_id, "role_method": RoleMethod.LLM_ATTRIBUTION.value},
            )
            # Оценку по скрипту могли поставить, пока шёл разбор, — и стояла она
            # на прежних ролях. Расшифровка от смены ролей не обновляется, сама
            # оценка не устареет: её удаляют, и команда оценки ставит новую.
            # Удаление — после записи ролей: команда оценки тоже берёт реплики
            # и расшифровку раньше, чем строку оценки, и встречного ожидания
            # через эту строку не возникает.
            await session.execute(DROP_EVALUATIONS_SQL, {"call_id": job.call_id})
        return None


async def load(session: AsyncSession, call_id: UUID) -> AnalysisInput | None:
    """Прочитать данные звонка, реплики и справочники в переданной сессии."""
    row = (await session.execute(_LOAD_CALL_SQL, {"call_id": call_id})).mappings().first()
    if row is None:
        return None
    segments = tuple(
        AnalysisSegment.model_validate(dict(row))
        for row in (await session.execute(_LOAD_SEGMENTS_SQL, {"call_id": call_id})).mappings()
    )
    nodes = tuple(
        ClassifierRecord.model_validate(dict(row))
        for row in (await session.execute(_LOAD_CLASSIFIER_SQL)).mappings()
    )
    reasons = tuple(
        CallReason(code=str(row["code"]), label=str(row["label"]), hint=str(row["hint"]))
        for row in (await session.execute(_LOAD_REASONS_SQL)).mappings()
    )
    return AnalysisInput(
        call=CallForAnalysis.model_validate(dict(row)),
        segments=segments,
        classifier=nodes,
        reasons=reasons,
    )
