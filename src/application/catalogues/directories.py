"""Сценарии синхронизации справочников с сохранением исторических связей звонков."""

import logging

from src.application.dto.directories import SyncReport
from src.application.errors.directories import DirectorySourceError
from src.application.ports.directories import DirectoryRepository, DirectorySource
from src.domain.calls.directories import merged_employees

logger = logging.getLogger(__name__)


class DirectorySyncService:
    """Согласовать источник и хранилище; управление транзакцией остаётся за портом записи."""

    def __init__(self, source: DirectorySource, repository: DirectoryRepository) -> None:
        """Получить заменяемые источник и хранилище, не открывая соединения."""
        self._source = source
        self._repository = repository

    async def employees(self) -> SyncReport:
        """Прочитать оба списка, сохранить сотрудников и пометить пропавших; вернуть сводку."""
        active = await self._source.employees(removed=False)
        dismissed = await self._source.employees(removed=True)
        employees = merged_employees(active, dismissed)
        created = updated = unchanged = 0
        for employee in employees:
            change = await self._repository.upsert_operator(employee)
            if change == "created":
                created += 1
            elif change == "updated":
                updated += 1
            else:
                unchanged += 1
        vanished = await self._repository.mark_missing_operators(
            tuple(row.login for row in employees)
        )
        note = (
            f"пропавших из справочника NCC помечено уволенными: {vanished} "
            "(строки сохранены — их логины стоят у звонков)"
            if vanished
            else None
        )
        return SyncReport(
            scope="сотрудники",
            seen=len(employees),
            created=created,
            updated=updated,
            unchanged=unchanged,
            note=note,
        )

    async def projects(self) -> SyncReport:
        """Обновить известные проекты, сохранив отсутствующие; вернуть сводку с отказами."""
        known = await self._repository.known_projects()
        if not known:
            return SyncReport(
                scope="проекты",
                note=(
                    "в справочнике пока нет ни одного проекта. Перечислить их в NCC нечем "
                    "(списочного запроса у REST API нет) — проект появится вместе с первым "
                    "принятым звонком из него"
                ),
            )
        updated = unchanged = failed = gone = 0
        for uuid in known:
            try:
                project = await self._source.project(uuid)
            except DirectorySourceError as exc:
                if exc.reason == "projects_not_found":
                    gone += 1
                else:
                    logger.warning("project %s: %s", uuid, exc.reason)
                failed += 1
                continue
            if await self._repository.update_project(project):
                updated += 1
            else:
                unchanged += 1
        note = (
            f"удалены в NCC, но сохранены у нас: {gone} "
            "(на них ссылаются звонки — без строки они потеряли бы название очереди)"
            if gone
            else None
        )
        return SyncReport(
            scope="проекты",
            seen=len(known),
            updated=updated,
            unchanged=unchanged,
            failed=failed,
            note=note,
        )
