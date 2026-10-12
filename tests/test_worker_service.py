"""Приём не задерживает очередь, отказы изолированы, остановка завершает активное задание."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

from src.application.dto.calls import CallSource
from src.application.dto.maintenance import CatchUpCommand, MaintenancePlan
from src.infrastructure.runtime.service import WorkerService


class _Queue:
    """Одна контролируемая активная работа, которая завершается после разрешения теста."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.finish = asyncio.Event()
        self.stopping = asyncio.Event()
        self.completed = False

    async def run_forever(self) -> None:
        self.started.set()
        await self.finish.wait()
        self.completed = True

    def request_stop(self) -> None:
        self.stopping.set()


def _maintenance():
    """Выдать немедленный NCC-прогон без внешнего API."""
    return Mock(
        plan=AsyncMock(
            return_value=MaintenancePlan(
                action="ncc",
                run_at=datetime.now(UTC),
                ingest=CatchUpCommand(source=CallSource.NCC, timezone="Europe/Moscow"),
            )
        ),
        execute=AsyncMock(),
    )


async def test_slow_intake_does_not_block_queue_and_stops_without_cancelling_active_job() -> None:
    """Метаданные ждут сеть, очередь работает; SIGTERM сохраняет активную работу."""
    queue = _Queue()
    maintenance = _maintenance()
    intake_started, intake_cancelled = asyncio.Event(), asyncio.Event()

    async def collect(plan):
        intake_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            intake_cancelled.set()

    maintenance.execute.side_effect = collect
    service = WorkerService(queue, maintenance, ["ncc"])
    running = asyncio.create_task(service.run_forever())
    await asyncio.wait_for(asyncio.gather(queue.started.wait(), intake_started.wait()), timeout=1)
    service.request_stop()
    await asyncio.wait_for(intake_cancelled.wait(), timeout=1)
    assert queue.stopping.is_set() and not queue.completed
    assert not running.done()
    queue.finish.set()
    await asyncio.wait_for(running, timeout=1)
    assert queue.completed


async def test_backend_failure_is_retried_without_stopping_processing(caplog) -> None:
    """Недоступное управление повторяется, а сообщение ошибки с данными не попадает в лог."""
    queue = _Queue()
    maintenance = _maintenance()
    failure_seen = asyncio.Event()

    async def fail(command):
        failure_seen.set()
        raise RuntimeError("private transcript and token")

    maintenance.plan.side_effect = fail
    service = WorkerService(queue, maintenance, ["ncc"], retry_seconds=0.01)
    running = asyncio.create_task(service.run_forever())
    try:
        await asyncio.wait_for(failure_seen.wait(), timeout=1)
        await asyncio.sleep(0.035)
        assert maintenance.plan.await_count >= 2
        assert queue.started.is_set() and not running.done()
        assert "private transcript" not in caplog.text
    finally:
        service.request_stop()
        queue.finish.set()
        await asyncio.wait_for(running, timeout=1)


async def test_sleeping_collectors_stop_immediately_and_do_not_start_another_run() -> None:
    """Не ждать ночного срока после остановки и не оставлять фоновых задач."""
    queue = _Queue()
    maintenance = _maintenance()
    plan = maintenance.plan.return_value
    maintenance.plan.return_value = plan.model_copy(
        update={"run_at": plan.run_at.replace(year=2030), "wait_seconds": 86400}
    )
    service = WorkerService(queue, maintenance, ["ncc"])
    running = asyncio.create_task(service.run_forever())
    await asyncio.wait_for(queue.started.wait(), timeout=1)
    service.request_stop()
    queue.finish.set()
    await asyncio.wait_for(running, timeout=1)
    maintenance.execute.assert_not_awaited()
    assert all(task.done() for task in service._collectors)


async def test_a_source_never_overlaps_its_previous_intake() -> None:
    """Один цикл не начинает следующий прогон, пока предыдущий не завершён."""
    queue = _Queue()
    maintenance = _maintenance()
    started = asyncio.Event()

    async def collect(plan):
        started.set()
        await asyncio.Event().wait()

    maintenance.execute.side_effect = collect
    service = WorkerService(queue, maintenance, ["ncc", "ncc"])
    running = asyncio.create_task(service.run_forever())
    await asyncio.wait_for(started.wait(), timeout=1)
    assert maintenance.execute.await_count == 1
    service.request_stop()
    queue.finish.set()
    await asyncio.wait_for(running, timeout=1)
