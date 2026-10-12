from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict

AdapterCallStatus = Literal["success", "empty", "error"]
AdapterFailureReason = Literal[
    "not_configured",
    "unauthorized",
    "forbidden",
    "timeout",
    "provider_error",
    "unexpected_error",
]


class AdapterExecutionTelemetry(BaseModel):
    """Счётчики попыток и повторов вызова без тела запроса."""

    model_config = ConfigDict(frozen=False)

    adapter: str
    request_attempts: int = 0
    retry_count: int = 0
    max_attempts: int | None = None
    total_timeout_ms: int | None = None

    def bind_policy(self, *, max_attempts: int, total_timeout_seconds: float) -> None:
        """Сохранить ограничения текущего внешнего вызова в телеметрии."""
        if max_attempts < 1:
            raise ValueError("Adapter max_attempts must be positive")
        timeout_ms = max(int(total_timeout_seconds * 1000), 1)
        if self.max_attempts is not None and self.max_attempts != max_attempts:
            raise ValueError("Adapter telemetry cannot change max_attempts")
        if self.total_timeout_ms is not None and self.total_timeout_ms != timeout_ms:
            raise ValueError("Adapter telemetry cannot change total timeout")
        self.max_attempts = max_attempts
        self.total_timeout_ms = timeout_ms

    def record_request_attempt(self) -> None:
        """Увеличить счётчик фактически выполненных запросов."""
        self.request_attempts += 1

    def record_retry(self) -> None:
        """Увеличить счётчик повторов текущего вызова."""
        self.retry_count += 1

    def apply_remote_snapshot(
        self,
        *,
        request_attempts: int,
        retry_count: int,
        max_attempts_per_request: int,
        total_timeout_ms: int,
    ) -> None:
        """Обновить счётчики исполнения данными технического адаптера."""
        if request_attempts < 0 or retry_count < 0:
            raise ValueError("Adapter telemetry counters cannot be negative")
        if retry_count > request_attempts:
            raise ValueError("Adapter retry_count cannot exceed request_attempts")
        if max_attempts_per_request < 1 or total_timeout_ms < 1:
            raise ValueError("Adapter remote policy values must be positive")
        if self.request_attempts or self.retry_count:
            raise ValueError("Adapter telemetry already contains local execution data")
        if self.max_attempts is not None and self.max_attempts != max_attempts_per_request:
            raise ValueError("Adapter remote max_attempts does not match local policy")
        if self.total_timeout_ms is not None and total_timeout_ms > self.total_timeout_ms:
            raise ValueError("Adapter remote timeout exceeds local policy")
        self.request_attempts = request_attempts
        self.retry_count = retry_count
        self.max_attempts = max_attempts_per_request
        self.total_timeout_ms = total_timeout_ms

    def as_payload(self) -> dict[str, int]:
        """Вернуть безопасные счётчики исполнения для журнала."""
        payload = {
            "request_attempts": self.request_attempts,
            "retry_count": self.retry_count,
        }
        if self.max_attempts is not None:
            payload["max_attempts_per_request"] = self.max_attempts
        if self.total_timeout_ms is not None:
            payload["total_timeout_ms"] = self.total_timeout_ms
        return payload


_CURRENT_ADAPTER_TELEMETRY: ContextVar[AdapterExecutionTelemetry | None] = ContextVar(
    "current_adapter_execution_telemetry",
    default=None,
)


@contextmanager
def capture_adapter_execution(telemetry: AdapterExecutionTelemetry) -> Iterator[None]:
    """Создать контекст накопления счётчиков внешнего вызова."""
    token = _CURRENT_ADAPTER_TELEMETRY.set(telemetry)
    try:
        yield
    finally:
        _CURRENT_ADAPTER_TELEMETRY.reset(token)


def current_adapter_execution_telemetry(adapter: str) -> AdapterExecutionTelemetry | None:
    """Вернуть счётчики текущего вызова либо None вне его контекста."""
    telemetry = _CURRENT_ADAPTER_TELEMETRY.get()
    if telemetry is None or telemetry.adapter != adapter:
        return None
    return telemetry


class AdapterOutcome(BaseModel):
    """Safe, normalized metadata for one external adapter invocation."""

    model_config = ConfigDict(frozen=True)

    adapter: str
    status: AdapterCallStatus
    elapsed_ms: int
    reason: AdapterFailureReason | None = None
    result_count: int | None = None
    request_id: str | None = None
    request_attempts: int | None = None
    retry_count: int | None = None
    max_attempts: int | None = None
    total_timeout_ms: int | None = None

    @classmethod
    def from_started(
        cls,
        *,
        adapter: str,
        started_at: float,
        status: AdapterCallStatus,
        reason: AdapterFailureReason | None = None,
        result_count: int | None = None,
        request_id: str | None = None,
        telemetry: AdapterExecutionTelemetry | None = None,
    ) -> AdapterOutcome:
        """Создать телеметрию от переданного момента начала операции."""
        return cls(
            adapter=adapter,
            status=status,
            elapsed_ms=int((time.perf_counter() - started_at) * 1000),
            reason=reason,
            result_count=result_count,
            request_id=request_id,
            request_attempts=telemetry.request_attempts if telemetry is not None else None,
            retry_count=telemetry.retry_count if telemetry is not None else None,
            max_attempts=telemetry.max_attempts if telemetry is not None else None,
            total_timeout_ms=telemetry.total_timeout_ms if telemetry is not None else None,
        )

    def as_payload(self) -> dict[str, str | int]:
        """Вернуть безопасные счётчики исполнения для журнала."""
        payload: dict[str, str | int] = {
            "adapter": self.adapter,
            "status": self.status,
            "elapsed_ms": self.elapsed_ms,
        }
        if self.reason is not None:
            payload["reason"] = self.reason
        if self.result_count is not None:
            payload["result_count"] = self.result_count
        if self.request_id is not None:
            payload["request_id"] = self.request_id
        if self.request_attempts is not None:
            payload["request_attempts"] = self.request_attempts
        if self.retry_count is not None:
            payload["retry_count"] = self.retry_count
        if self.max_attempts is not None:
            payload["max_attempts_per_request"] = self.max_attempts
        if self.total_timeout_ms is not None:
            payload["total_timeout_ms"] = self.total_timeout_ms
        return payload


class AdapterOutcomeSink(Protocol):
    """Накопить итог вызова адаптера для журнала и метрик."""

    def record_adapter_outcome(self, outcome: AdapterOutcome) -> None:
        """Добавить DTO итога вызова в накопитель журнала и метрик; результата не возвращает."""
        ...
