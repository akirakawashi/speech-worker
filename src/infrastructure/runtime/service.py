"""Один процесс: независимые циклы приёма и последовательная общая очередь."""

import asyncio
import logging
from collections.abc import Sequence
from typing import Protocol

from src.application.dto.maintenance import MaintenanceAction, MaintenanceRequest
from src.application.maintenance import MaintenanceService

logger = logging.getLogger(__name__)


class SequentialWorker(Protocol):
    """Минимальный технический контракт обработчика очереди."""

    async def run_forever(self) -> None:
        """Последовательно выполнять задания до сигнала остановки."""
        ...

    def request_stop(self) -> None:
        """Завершить текущую работу и перестать захватывать следующие задания."""
        ...


class WorkerService:
    """Запустить обслуживание независимо от очереди и согласованно остановить их."""

    def __init__(
        self,
        worker: SequentialWorker,
        maintenance: MaintenanceService,
        actions: Sequence[MaintenanceAction],
        *,
        retry_seconds: float = 60,
    ) -> None:
        """Принять очередь, готовые действия и задержку технического повтора."""
        self._worker = worker
        self._maintenance = maintenance
        self._actions = tuple(dict.fromkeys(actions))
        self._retry_seconds = retry_seconds
        self._stopping = asyncio.Event()
        self._collectors: list[asyncio.Task[None]] = []

    def request_stop(self) -> None:
        """Остановить опрос источников сразу; текущему заданию дать завершиться."""
        self._stopping.set()
        self._worker.request_stop()
        for task in self._collectors:
            task.cancel()

    async def run_forever(self) -> None:
        """Запустить циклы; после остановки дождаться очереди и закрыть все фоновые задачи.

        Отказ приёма изолирован от обработки. Неожиданное завершение очереди
        завершает весь сервис, чтобы внешний supervisor мог его перезапустить.
        """
        if self._stopping.is_set():
            return
        self._collectors = [
            asyncio.create_task(self._collect(action), name=f"speech-collect-{action}")
            for action in self._actions
        ]
        processing = asyncio.create_task(self._worker.run_forever(), name="speech-processing")
        try:
            await asyncio.shield(processing)
        finally:
            self.request_stop()
            await asyncio.gather(*self._collectors, return_exceptions=True)
            await processing

    async def _collect(self, action: MaintenanceAction) -> None:
        """Получать сроки из бэкенда и выполнять непересекающиеся прогоны одного действия."""
        last_started = None
        while not self._stopping.is_set():
            try:
                plan = await self._maintenance.plan(
                    MaintenanceRequest(action=action, last_started=last_started)
                )
                await self._wait(plan.wait_seconds)
                if self._stopping.is_set():
                    return
                last_started = plan.run_at
                await self._maintenance.execute(plan)
                logger.info("scheduled action completed action=%s", action)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "scheduled action failed action=%s type=%s", action, type(exc).__name__
                )
                await self._wait(self._retry_seconds)

    async def _wait(self, seconds: float) -> None:
        """Подождать срок или остановку без блокировки потока обработки очереди."""
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)
        except TimeoutError:
            pass
