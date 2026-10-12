"""Сравнить модель-кандидат с сохранённым результатом и необязательным контролем."""

import asyncio
import time
import uuid
from collections.abc import Callable, Sequence

from src.application.dto.calls import JobKind, JobLane
from src.application.dto.comparison import Outcome, ReasonOutcome, ReasonRow, Row, Sampled
from src.application.dto.pipeline_input import AnalysisSegment
from src.application.pipeline.analyze import AnalyzeHandler
from src.application.pipeline.stages import StageDeps, StageError
from src.application.ports.comparison import ComparisonRepository
from src.application.ports.queue import ClaimedJob
from src.application.ports.recordings import AnalysisBackend
from src.application.reasons.batch import ask_reason
from src.domain.calls.reasons import REASONS_PROMPT_LIMIT, prompt_reasons


async def analyse(handler: AnalyzeHandler, deps: StageDeps, call_id: uuid.UUID) -> Outcome:
    """Разбор кодом этапа, без записи. Отказ — строка с классом ошибки, не исключение."""
    job = ClaimedJob(
        id=uuid.uuid4(),
        call_id=call_id,
        kind=JobKind.ANALYZE,
        lane=JobLane.BACKFILL,
        attempts=1,
        max_attempts=1,
        payload={},
        lease_seconds=0,
    )
    started = time.monotonic()
    try:
        work = await handler.work(job, deps)
    except StageError as exc:
        # Текст разговора в сообщения этапа не попадает: там класс, причина и
        # номер попытки (`analyze._run_until_valid`).
        return Outcome(
            work=None, error=f"{exc.error_class.value}: {exc}", seconds=time.monotonic() - started
        )
    return Outcome(work=work, error=None, seconds=time.monotonic() - started)


async def compare(
    calls: Sequence[Sampled],
    *,
    candidate_deps: StageDeps,
    control_deps: StageDeps | None,
    concurrency: int,
    progress_every: int = 10,
    echo: Callable[[str], None] = print,
) -> list[Row]:
    """Прогнать кандидата и необязательный контроль через этап без записи результатов."""
    handler = AnalyzeHandler()
    gate = asyncio.Semaphore(max(1, concurrency))
    started = time.monotonic()
    done = 0

    async def one(call: Sampled) -> Row:
        """Обработать один звонок в пределах разрешённой параллельности и учесть результат."""
        nonlocal done
        async with gate:
            # Кандидат и контроль — одновременно: они на разных машинах, и
            # время прогона определяет самый медленный из двух, а не их сумма.
            if control_deps is None:
                row = Row(
                    call=call,
                    candidate=await analyse(handler, candidate_deps, call.call_id),
                    control=None,
                )
            else:
                fresh, again = await asyncio.gather(
                    analyse(handler, candidate_deps, call.call_id),
                    analyse(handler, control_deps, call.call_id),
                )
                row = Row(call=call, candidate=fresh, control=again)
        done += 1
        if progress_every and done % progress_every == 0:
            # `print`, а не `logging`: в образах логирование не сконфигурировано.
            echo(f"  {done}/{len(calls)}, {time.monotonic() - started:.0f} с")
        return row

    return list(await asyncio.gather(*(one(call) for call in calls)))


async def _reason_reference(
    repository: ComparisonRepository,
) -> tuple[str, frozenset[str]]:
    """Прочитать и проверить доступный модели справочник причин."""
    reasons = await repository.reasons()
    reference = prompt_reasons(reasons)
    if not reasons or len(reference) > REASONS_PROMPT_LIMIT:
        raise SystemExit("the reason reference is empty or longer than the workflow accepts")
    return reference, frozenset(reason.code for reason in reasons)


async def _ask_timed(
    client: AnalysisBackend,
    call_id: uuid.UUID,
    reference: str,
    codes: frozenset[str],
    segments: Sequence[AnalysisSegment],
) -> ReasonOutcome:
    """Вызвать модель причины и вернуть ответ вместе с длительностью вызова."""
    started = time.monotonic()
    answer = await ask_reason(client, call_id, reference, codes, segments)
    seconds = time.monotonic() - started
    if answer is None:
        return ReasonOutcome(code=None, failed=True, seconds=seconds)
    return ReasonOutcome(code=answer.code, failed=False, seconds=seconds)


async def compare_reasons(
    repository: ComparisonRepository,
    calls: Sequence[tuple[uuid.UUID, str]],
    *,
    candidate: AnalysisBackend,
    control: AnalysisBackend | None,
    concurrency: int,
    progress_every: int = 10,
    echo: Callable[[str], None] = print,
) -> list[ReasonRow]:
    """Сравнить коды причин кандидата и контроля на переданной выборке звонков."""
    reference, codes = await _reason_reference(repository)
    gate = asyncio.Semaphore(max(1, concurrency))
    started = time.monotonic()
    done = 0

    async def one(call_id: uuid.UUID, stored: str) -> ReasonRow:
        """Обработать один звонок в пределах разрешённой параллельности и учесть результат."""
        nonlocal done
        async with gate:
            segments = await repository.segments(call_id)
            if control is None:
                fresh = await _ask_timed(candidate, call_id, reference, codes, segments)
                row = ReasonRow(call_id=call_id, stored=stored, candidate=fresh, control=None)
            else:
                fresh, again = await asyncio.gather(
                    _ask_timed(candidate, call_id, reference, codes, segments),
                    _ask_timed(control, call_id, reference, codes, segments),
                )
                row = ReasonRow(call_id=call_id, stored=stored, candidate=fresh, control=again)
        done += 1
        if progress_every and done % progress_every == 0:
            echo(f"  {done}/{len(calls)}, {time.monotonic() - started:.0f} с")
        return row

    return list(await asyncio.gather(*(one(call_id, stored) for call_id, stored in calls)))
