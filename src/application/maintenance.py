"""Выполнение служебных команд: получить план, собрать записи, поставить оценку."""

from src.application.dto.maintenance import MaintenancePlan, MaintenanceRequest, QualityQueueRequest
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
        """Собрать метаданные либо поставить оценку; модели здесь не вызываются."""
        if plan.ingest is not None:
            await self._intake.collect(plan.ingest)
        else:
            assert plan.evaluation is not None
            await self._backend.enqueue(QualityQueueRequest(selection=plan.evaluation))
