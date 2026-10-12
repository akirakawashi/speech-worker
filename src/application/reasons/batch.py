"""Заполнение причин с ограничением параллельности и проверкой ответов модели."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Sequence
from datetime import datetime

from pydantic import JsonValue

from src.application.dto.pipeline_input import AnalysisSegment
from src.application.dto.reasons import FillReport, ReasonAnswer, ReasonSelection
from src.application.errors.external import AnalysisError
from src.application.errors.reasons import ReasonsNotConfigured
from src.application.pipeline.analyze import (
    MAX_OUTPUT_ATTEMPTS,
    describe_raw_problem,
    normalise_outputs,
)
from src.application.ports.reasons import ReasonRepository
from src.application.ports.recordings import AnalysisBackend
from src.domain.calls.reasons import REASONS_PROMPT_LIMIT, interpret_reason, prompt_reasons

logger = logging.getLogger(__name__)
PROVIDER_RETRIES = 3
PROVIDER_PAUSE_SECONDS = 15


async def ask_reason(
    client: AnalysisBackend,
    call_id: uuid.UUID,
    reference: str,
    codes: frozenset[str],
    segments: Sequence[AnalysisSegment],
) -> ReasonAnswer | None:
    """Причина одного звонка. `None` — Dify недоступен после повторов."""
    inputs: dict[str, JsonValue] = {
        "call_id": str(call_id),
        "reasons": reference,
        "segments": [
            {"seq": int(s.seq), "speaker": str(s.speaker), "text": s.text_raw} for s in segments
        ],
    }
    answer = ReasonAnswer()
    for _attempt in range(MAX_OUTPUT_ATTEMPTS):
        outputs: dict[str, JsonValue] | None = None
        for retry in range(PROVIDER_RETRIES):
            try:
                outputs = await client.run(inputs)
                break
            except AnalysisError as exc:
                if not exc.retryable or retry == PROVIDER_RETRIES - 1:
                    logger.warning("call=%s: reason workflow failed: %s", call_id, exc.reason)
                    return None
                await asyncio.sleep(PROVIDER_PAUSE_SECONDS)
        problem = describe_raw_problem(outputs)
        answer = interpret_reason(normalise_outputs(outputs), codes)
        if answer.code:
            return answer
        logger.info("call=%s: no reason code in the answer (%s)", call_id, problem or "no code")
    return answer


async def fill_reasons(
    repository: ReasonRepository,
    client: AnalysisBackend | None,
    *,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    transcribed_since: datetime | None = None,
    apply: bool,
    limit: int | None = None,
    concurrency: int = 4,
    progress_every: int = 100,
) -> FillReport:
    """Причины звонков периода — или звонков, расшифрованных после `transcribed_since`."""
    if transcribed_since is None and (date_from is None or date_to is None):
        raise ValueError("give a period or transcribed_since")
    started = time.monotonic()
    candidates = await repository.candidates(
        ReasonSelection(
            date_from=date_from, date_to=date_to, transcribed_since=transcribed_since, limit=limit
        )
    )
    call_ids, reasons = candidates.call_ids, candidates.reasons
    report = FillReport(candidates=len(call_ids), applied=apply)
    if not apply or not call_ids:
        return report
    if client is None:
        raise ReasonsNotConfigured("no reason client")
    reference = prompt_reasons(reasons)
    if not reasons or len(reference) > REASONS_PROMPT_LIMIT:
        raise ReasonsNotConfigured(
            "the reason reference is empty or longer than the workflow accepts: "
            "python -m src.presentation.cli.seed_call_reasons --apply"
        )
    codes = frozenset(reason.code for reason in reasons)
    gate = asyncio.Semaphore(max(1, concurrency))
    done = 0

    async def one(call_id: uuid.UUID) -> None:
        """Обработать один звонок в пределах разрешённой параллельности и учесть результат."""
        nonlocal done
        async with gate:
            segments = await repository.segments(call_id)
            answer = await ask_reason(client, call_id, reference, codes, segments)
            if answer is None:
                report.failed += 1
            elif answer.code is None:
                report.unanswered += 1
            else:
                await repository.save(call_id, answer)
                report.filled += 1
            done += 1
            if progress_every and done % progress_every == 0:
                # `print`, а не `logging`: в образах логирование не сконфигурировано.
                print(
                    f"  {done}/{len(call_ids)}: записано {report.filled}, "
                    f"без ответа {report.unanswered}, сбой {report.failed}, "
                    f"{time.monotonic() - started:.0f} с",
                    flush=True,
                )

    await asyncio.gather(*(one(call_id) for call_id in call_ids))
    report.seconds = time.monotonic() - started
    return report
