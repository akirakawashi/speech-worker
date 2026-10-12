"""Пополнить дерево путями источника и связать ранее принятые звонки."""

from collections.abc import Sequence
from datetime import date, timedelta
from typing import Final

from src.application.dto.classifier_backfill import ClassifierBackfillReport
from src.application.dto.sources import CallRef
from src.application.ports.classifier_backfill import (
    ClassificationSource,
    ClassifierBackfillRepository,
    ClassifierBackfillTransaction,
)
from src.domain.calls.classifier import PATH_SEPARATOR

MAX_DAYS: Final[int] = 62
_LINK_BATCH: Final[int] = 1000


def disposition_text(ref: CallRef) -> str | None:
    """Вернуть операторский путь одной строкой до 255 символов без пустых уровней."""
    return PATH_SEPARATOR.join(level for level in ref.disposition_path if level)[:255] or None


def days_between(first: date, last: date) -> list[date]:
    """Вернуть включительные дни периода; неверное или чрезмерное окно отклонить."""
    if last < first:
        raise ValueError("--to is earlier than --from")
    span = (last - first).days + 1
    if span > MAX_DAYS:
        raise ValueError(f"the window is {span} days; at most {MAX_DAYS}")
    return [first + timedelta(days=offset) for offset in range(span)]


async def backfill_classifier(
    source: ClassificationSource,
    days: Sequence[date],
    *,
    repository: ClassifierBackfillRepository,
    apply: bool,
) -> ClassifierBackfillReport:
    # Представление читается до транзакции: держать её открытой, пока идут
    # запросы в чужую базу, незачем.
    """Пополнить дерево путями источника и вернуть сводку связывания звонков."""
    totals: dict[tuple[str, ...], int] = {}
    refs: dict[str, CallRef] = {}
    for day in days:
        for levels, calls in (await source.list_classifications(day)).items():
            totals[levels] = totals.get(levels, 0) + calls
        for ref in await source.list_calls(day):
            if ref.disposition_path:
                refs.setdefault(ref.session_id, ref)

    async with repository.transaction(apply=apply) as paths:
        for levels in sorted(totals):
            await paths.node(levels)
        linked = await _link(paths, list(refs.values()), apply=apply)

    return ClassifierBackfillReport(
        days=len(days),
        calls=sum(totals.values()),
        paths=len(totals),
        created=paths.created,
        applied=apply,
        linked=linked,
    )


async def _link(
    paths: ClassifierBackfillTransaction, refs: Sequence[CallRef], *, apply: bool
) -> int:
    """Связать с узлом оператора звонки, которые приняты, но ещё не связаны."""
    linked = 0
    for start in range(0, len(refs), _LINK_BATCH):
        batch = refs[start : start + _LINK_BATCH]
        unlinked = await paths.unlinked([ref.session_id for ref in batch])
        for ref in batch:
            if ref.session_id not in unlinked:
                continue
            node_id = await paths.node(ref.disposition_path)
            if apply:
                await paths.link(ref.session_id, node_id, disposition_text(ref))
            linked += 1
    return linked
