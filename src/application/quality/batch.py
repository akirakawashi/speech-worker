"""Пакетная оценка: подготовить роли, спросить модель и сохранить каждую попытку."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Sequence
from datetime import datetime

from src.application.dto.quality import CURRENT_SCRIPT, CallScript, Evaluation, TranscriptLine
from src.application.dto.quality_batch import (
    BatchResult,
    CallRef,
    CandidateSelection,
    EvaluateReport,
    LineSnapshot,
    RunEvaluation,
)
from src.application.errors.quality import QualityModelError
from src.application.ports.evaluation import EvaluationRepository
from src.application.quality.evaluation import (
    MAX_OUTPUT_ATTEMPTS,
    QualityError,
    QualityModel,
    evaluate,
)
from src.application.quality.roles import RoleAttributionError, prepare_roles, without_roles
from src.domain.calls.period import period_bounds


def describe_evaluation(evaluation: Evaluation, script: CallScript) -> str:
    """Строка о звонке для журнала команды. Ни реплик, ни пояснений модели в ней нет."""
    if not evaluation.script_applies:
        return "скрипт не применим"
    return (
        f"балл {evaluation.score}%, этапов {evaluation.passed_count}/{evaluation.stage_count}, "
        f"итог: {script.outcome_label(evaluation.outcome)}"
    )


async def _settle_roles(
    repository: EvaluationRepository,
    client: QualityModel,
    call_id: uuid.UUID,
    lines: list[TranscriptLine],
    read_at: datetime,
) -> list[TranscriptLine]:
    """Роли моно-записи — в базу сразу, до оценки. Возвращает реплики с ролями.

    Записанные вместе с оценкой, роли пропадали вместе с ней: ответ модели не
    проходил проверку, разговор оставался на экране без ролей, и следующий
    прогон спрашивал о них модель заново.

    Запись — под той же проверкой, что и у оценки: расшифровку за время ответа
    модели могли переобработать, и роли прежних реплик в новые не пишутся.
    """
    if not without_roles(lines):
        return lines
    try:
        prepared = await prepare_roles(client, lines, max_attempts=MAX_OUTPUT_ATTEMPTS)
    except RoleAttributionError as exc:
        raise QualityError(str(exc), code="roles_undetermined") from exc
    await repository.save_roles(
        call_id, LineSnapshot(lines=tuple(lines), read_at=read_at), prepared
    )
    return list(prepared)


async def evaluate_calls(
    repository: EvaluationRepository,
    client: QualityModel,
    calls: Sequence[CallRef],
    *,
    script: CallScript = CURRENT_SCRIPT,
    on_line: Callable[[str], None] = lambda _line: None,
) -> EvaluateReport:
    """Оценить звонки по одному и записать каждую оценку своей транзакцией.

    Своя транзакция на звонок: отказ модели на одном звонке не откатывает
    остальные, а прерванная команда оставляет оценённое оценённым.

    Отказ настройки — неверный ключ, неизвестная модель — останавливает всю
    команду: он повторился бы на каждом звонке, и отчёт из сотни одинаковых
    отказов скрыл бы единственную причину. Любой другой отказ модели касается
    одного звонка: он считается неоценённым, и команда идёт дальше.
    """
    report = EvaluateReport(candidates=len(calls), applied=True)
    for call in calls:
        snapshot = await repository.load_lines(call.call_id)
        lines, read_at = list(snapshot.lines), snapshot.read_at
        try:
            lines = await _settle_roles(repository, client, call.call_id, lines, read_at)
            evaluation = await evaluate(client, lines, script=script)
            await repository.save_evaluation(
                call.call_id, evaluation, LineSnapshot(lines=tuple(lines), read_at=read_at)
            )
        except QualityModelError as exc:
            if exc.misconfigured:
                raise
            await repository.save_failure(
                call.call_id,
                LineSnapshot(lines=tuple(lines), read_at=read_at),
                client.model,
                script,
                "model_error",
            )
            report.failed += 1
            report.failures.append(f"{call.session_id}: модель не ответила ({exc.reason})")
            on_line(report.failures[-1])
            continue
        except QualityError as exc:
            await repository.save_failure(
                call.call_id,
                LineSnapshot(lines=tuple(lines), read_at=read_at),
                client.model,
                script,
                exc.code,
            )
            report.failed += 1
            report.failures.append(f"{call.session_id}: не оценён ({exc.reason})")
            on_line(report.failures[-1])
            continue
        if evaluation.script_applies:
            report.evaluated += 1
        else:
            report.not_applicable += 1
        on_line(f"{call.session_id}: {describe_evaluation(evaluation, script)}")
    return report


def validate_selection(query: RunEvaluation) -> None:
    """Проверить совместимость параметров охвата до создания соединений или клиента модели."""
    dated = query.first is not None or query.last is not None
    if query.drop_stale:
        if (
            dated
            or query.everything
            or query.call is not None
            or query.limit is not None
            or query.force
            or query.skip_failed
        ):
            raise ValueError(
                "--drop-stale checks every stored evaluation; no other option applies to it"
            )
        return
    if query.everything:
        if query.call is not None or dated:
            raise ValueError("--all takes every call; a period or --call does not apply to it")
        if query.limit is None:
            raise ValueError("--all needs --limit: without it one run takes the whole archive")
    elif query.call is None and (query.first is None or query.last is None):
        raise ValueError("give --from and --to, --call, or --all")
    if query.call is not None and dated:
        raise ValueError("--call evaluates one call; a period does not apply to it")
    if query.limit is not None and query.limit < 1:
        raise ValueError("--limit must be positive")
    if query.skip_failed and (query.force or query.call is not None):
        raise ValueError("--skip-failed does not apply with --force or --call")


class EvaluationService:
    """Выбрать охват команды и применить сценарий оценки через заменяемые порты."""

    def __init__(
        self,
        repository: EvaluationRepository,
        client_factory: Callable[[], QualityModel],
        *,
        timezone: str,
    ) -> None:
        """Принять хранение, отложенную сборку модели и пояс календарных границ."""
        self._repository = repository
        self._client_factory = client_factory
        self._timezone = timezone

    async def run(
        self, query: RunEvaluation, *, on_line: Callable[[str], None] = lambda _line: None
    ) -> BatchResult:
        """Проверить охват, показать кандидатов либо оценить их и вернуть отчёт."""
        validate_selection(query)
        if query.drop_stale:
            return BatchResult(stale=await self._repository.drop_stale(apply=query.apply))
        if query.call is not None:
            calls = [await self._repository.find_call(query.call)]
        else:
            start: datetime | None = None
            end: datetime | None = None
            if query.first is not None and query.last is not None:
                if query.last < query.first:
                    raise ValueError("--to is earlier than --from")
                start, end = period_bounds(query.first, query.last, self._timezone)
            calls = await self._repository.find_candidates(
                CandidateSelection(
                    date_from=start,
                    date_to=end,
                    force=query.force,
                    skip_failed=query.skip_failed,
                    limit=query.limit,
                )
            )
        if not query.apply:
            return BatchResult(report=EvaluateReport(candidates=len(calls)))
        report = await evaluate_calls(
            self._repository, self._client_factory(), calls, on_line=on_line
        )
        return BatchResult(report=report)
