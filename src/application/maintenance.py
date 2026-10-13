"""Выполнить готовый план приёма записей; оценку планирует бэкенд."""

from src.application.dto.maintenance import MaintenancePlan, MaintenanceRequest
from src.application.ports.maintenance import BackendMaintenance, ScheduledIntake


class MaintenanceService:
    """Передавать команды нужному порту без выбора расписания и правил оценки."""

    def __init__(self, backend: BackendMaintenance, intake: ScheduledIntake) -> None:
        """Принять служебное управление и существующий приём метаданных."""
        self._backend = backend
        self._intake = intake

    async def plan(self, command: MaintenanceRequest) -> MaintenancePlan:
        """Получить проверенный план по действию и моменту предыдущего запуска."""
        return await self._backend.plan(command)

    async def execute(self, plan: MaintenancePlan) -> None:
        """Передать готовую команду источника порту приёма метаданных."""
        await self._intake.collect(plan.ingest)
