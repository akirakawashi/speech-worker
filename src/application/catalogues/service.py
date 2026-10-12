"""Сценарии загрузки дерева и причин из справочников владельца."""

import uuid

from src.application.dto.calls import ClassifierSource
from src.application.dto.catalogues import (
    CallReason,
    ClassifierNodeWrite,
    ClassifierSeedReport,
    ReasonsSeedReport,
    ReasonWrite,
)
from src.application.ports.catalogues import ClassifierCatalogue, ReasonsCatalogue
from src.domain.calls.classifier import label_key


async def seed_classifier(
    repository: ClassifierCatalogue, paths: list[tuple[str, ...]], *, apply: bool
) -> ClassifierSeedReport:
    """Сравнить пути с деревом; при apply записать изменения и вернуть сводку."""
    existing = {(row.parent_id, row.key): row for row in await repository.nodes()}
    placed: dict[tuple[uuid.UUID | None, str], uuid.UUID] = {}
    created = updated = 0
    source = ClassifierSource.FILE.value

    for levels in paths:
        parent: uuid.UUID | None = None
        for level, label in enumerate(levels, start=1):
            slot = (parent, label_key(label))
            if slot not in placed:
                order = len(placed)
                row = existing.get(slot)
                if row is None:
                    created += 1
                    data = ClassifierNodeWrite(
                        parent_id=parent,
                        level=level,
                        label=label,
                        key=slot[1],
                        source=source,
                        sort_order=order,
                    )
                    node_id = await repository.create(data) if apply else uuid.uuid4()
                else:
                    node_id = row.id
                    wanted = (label, source, order, True)
                    if (row.label, row.source, row.sort_order, row.is_active) != wanted:
                        updated += 1
                        if apply:
                            await repository.update(
                                node_id,
                                ClassifierNodeWrite(
                                    parent_id=parent,
                                    level=level,
                                    label=label,
                                    key=slot[1],
                                    source=source,
                                    sort_order=order,
                                ),
                            )
                placed[slot] = node_id
            parent = placed[slot]

    return ClassifierSeedReport(
        paths=len(paths), nodes=len(placed), created=created, updated=updated
    )


async def seed_call_reasons(
    repository: ReasonsCatalogue, reasons: list[CallReason], *, apply: bool
) -> ReasonsSeedReport:
    """Согласовать справочник с файлом, выключить пропавшие коды и вернуть сводку."""
    existing = {row.code: row for row in await repository.reasons()}
    created = updated = 0
    for order, reason in enumerate(reasons):
        row = existing.get(reason.code)
        wanted = (reason.label, reason.hint, order, True)
        if row is None:
            created += 1
        elif (row.label, row.hint, row.sort_order, row.is_active) != wanted:
            updated += 1
        else:
            continue
        if apply:
            await repository.upsert(
                ReasonWrite(
                    code=reason.code, label=reason.label, hint=reason.hint, sort_order=order
                )
            )
    listed = {reason.code for reason in reasons}
    disabled = tuple(
        sorted(code for code, row in existing.items() if code not in listed and row.is_active)
    )
    if apply:
        for code in disabled:
            await repository.disable(code)
    return ReasonsSeedReport(
        codes=len(reasons), created=created, updated=updated, disabled=disabled
    )
