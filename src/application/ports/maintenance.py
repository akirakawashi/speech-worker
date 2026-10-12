"""Порты служебного управления и приёма записей по готовой команде."""

from typing import Protocol

from src.application.dto.maintenance import (
    CatchUpCommand,
    MaintenancePlan,
    MaintenanceRequest,
    QualityQueueReport,
    QualityQueueRequest,
)


class BackendMaintenance(Protocol):
    """Получить расписание и поставить оценку через служебный API бэкенда."""

    async def plan(self, command: MaintenanceRequest) -> MaintenancePlan:
        """Вернуть разрешённый срок и готовую команду выбранного действия."""
        ...

    async def enqueue(self, command: QualityQueueRequest) -> QualityQueueReport:
        """Попросить бэкенд выбрать звонки и дедуплицировать задания оценки."""
        ...


class ScheduledIntake(Protocol):
    """Выполнить приём метаданных; скачивание аудио делает очередь."""

    async def collect(self, command: CatchUpCommand) -> None:
        """Собрать свежие записи и зарегистрировать задания, сохранив курсоры источника."""
        ...
