"""Дедлайн, повторы и телеметрия внешнего HTTP-вызова.

Политика Whisper собирается в infrastructure/transcription/execution.py.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from types import TracebackType
from typing import Literal, Self

import httpx
from pydantic import BaseModel, ConfigDict

from src.application.dto.adapter import (
    AdapterExecutionTelemetry,
    current_adapter_execution_telemetry,
)

#: Имена целей, под которыми исполнение пишет телеметрию. В монорепозитории тип
#: собирался в `app/adapters/registry.py` вместе с именами корпоративных
#: адаптеров чата (Jira, Confluence и другие) — реестр остался там, сюда
#: перенесены только две цели конвейера речи.
AdapterExecutionTarget = Literal["speech_transcription", "speech_ncc"]

AdapterRequestFailureReason = Literal[
    "unauthorized",
    "forbidden",
    "timeout",
    "provider_error",
]

DEFAULT_RETRYABLE_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})


class AdapterExecutionPolicy(BaseModel):
    """Ограничения времени, повторов и пула одного внешнего вызова."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    adapter: AdapterExecutionTarget
    operation_type: Literal["read"]
    total_timeout_seconds: float
    max_attempts: int
    retry_backoff_seconds: float
    retryable_status_codes: frozenset[int]


class AdapterRequestFailure(BaseModel):
    """Нормализованный отказ запроса к внешней зависимости."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    reason: AdapterRequestFailureReason
    retryable: bool
    status_code: int | None = None


class AdapterRequestError(RuntimeError):
    """Safe, normalized HTTP failure raised inside an adapter boundary."""

    def __init__(self, failure: AdapterRequestFailure) -> None:
        """Настроить AdapterRequestError из переданных параметров и зависимостей."""
        self.failure = failure
        super().__init__(f"Adapter request failed: {failure.reason}")

    @property
    def reason(self) -> AdapterRequestFailureReason:
        """Вернуть нормализованную причину технического отказа."""
        return self.failure.reason

    @property
    def status_code(self) -> int | None:
        """Вернуть HTTP-код технического отказа, если ответ был получен."""
        return self.failure.status_code


def classify_adapter_http_error(
    error: httpx.HTTPError | TimeoutError,
    *,
    retryable_status_codes: frozenset[int] = DEFAULT_RETRYABLE_STATUS_CODES,
) -> AdapterRequestFailure:
    """Перевести технический отказ HTTP в устойчивую причину порта."""
    if isinstance(error, (httpx.TimeoutException, TimeoutError)):
        return AdapterRequestFailure(reason="timeout", retryable=True)

    if isinstance(error, httpx.HTTPStatusError):
        status_code = error.response.status_code
        if status_code == 401:
            return AdapterRequestFailure(
                reason="unauthorized",
                retryable=False,
                status_code=status_code,
            )
        if status_code == 403:
            return AdapterRequestFailure(
                reason="forbidden",
                retryable=False,
                status_code=status_code,
            )
        return AdapterRequestFailure(
            reason="provider_error",
            retryable=status_code in retryable_status_codes,
            status_code=status_code,
        )

    if isinstance(error, httpx.RequestError):
        return AdapterRequestFailure(reason="provider_error", retryable=True)

    return AdapterRequestFailure(reason="provider_error", retryable=False)


class AdapterExecution:
    """One total deadline shared by every request and retry in an adapter call."""

    def __init__(
        self,
        policy: AdapterExecutionPolicy,
        telemetry: AdapterExecutionTelemetry | None = None,
    ) -> None:
        """Настроить AdapterExecution из переданных параметров и зависимостей."""
        self.policy = policy
        self.telemetry = (
            telemetry
            or current_adapter_execution_telemetry(policy.adapter)
            or AdapterExecutionTelemetry(adapter=policy.adapter)
        )
        self.telemetry.bind_policy(
            max_attempts=policy.max_attempts,
            total_timeout_seconds=policy.total_timeout_seconds,
        )
        self._deadline: float | None = None
        self._timeout_context: asyncio.Timeout | None = None
        self._active = False
        self._used = False

    @property
    def request_attempts(self) -> int:
        """Вернуть число выполненных сетевых попыток текущего вызова."""
        return self.telemetry.request_attempts

    @property
    def retry_count(self) -> int:
        """Вернуть число повторов текущего внешнего вызова."""
        return self.telemetry.retry_count

    @property
    def remaining_seconds(self) -> float:
        """Вернуть остаток общего бюджета времени вызова в секундах."""
        if not self._active or self._deadline is None:
            raise RuntimeError("Adapter execution context is not active")
        return max(self._deadline - time.monotonic(), 0.0)

    async def __aenter__(self) -> Self:
        """Открыть контекст внешнего вызова и вернуть его исполнитель."""
        if self._used:
            raise RuntimeError("Adapter execution context cannot be reused")
        self._used = True
        self._active = True
        self._deadline = time.monotonic() + self.policy.total_timeout_seconds
        self._timeout_context = asyncio.timeout(self.policy.total_timeout_seconds)
        await self._timeout_context.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """Завершить контекст вызова и закрыть принадлежащие ему ресурсы."""
        if self._timeout_context is None:
            return None
        try:
            await self._timeout_context.__aexit__(exc_type, exc_value, traceback)
            return None
        finally:
            self._active = False

    async def request(
        self,
        client: httpx.AsyncClient,
        method: Literal["GET", "POST"],
        url: str | httpx.URL,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        json: Mapping[str, object] | None = None,
        data: Mapping[str, str] | None = None,
        files: Mapping[str, tuple[str, bytes, str]] | None = None,
        retry_safe: bool = True,
    ) -> httpx.Response:
        """Выполнить HTTP-запрос с ограничением бюджета и разрешёнными повторами."""
        last_error: AdapterRequestError | None = None

        for attempt in range(1, self.policy.max_attempts + 1):
            remaining_seconds = self.remaining_seconds
            if remaining_seconds <= 0:
                raise TimeoutError

            self.telemetry.record_request_attempt()
            try:
                response = await client.request(
                    method,
                    url,
                    params=params,
                    headers=headers,
                    json=json,
                    data=data,
                    files=files,
                    timeout=remaining_seconds,
                )
                response.raise_for_status()
                return response
            except (httpx.HTTPError, TimeoutError) as exc:
                failure = classify_adapter_http_error(
                    exc,
                    retryable_status_codes=self.policy.retryable_status_codes,
                )
                last_error = AdapterRequestError(failure)
                if not self._can_retry(failure, attempt=attempt, retry_safe=retry_safe):
                    raise last_error from exc

                delay = self.policy.retry_backoff_seconds * attempt
                if delay >= self.remaining_seconds:
                    raise last_error from exc
                self.telemetry.record_retry()
                if delay > 0:
                    await asyncio.sleep(delay)

        if last_error is not None:
            raise last_error
        raise RuntimeError("Adapter request did not execute")

    async def get(
        self,
        client: httpx.AsyncClient,
        url: str | httpx.URL,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """Вернуть зарегистрированный объект по ключу либо сообщить об отсутствии."""
        return await self.request(
            client,
            "GET",
            url,
            params=params,
            headers=headers,
            retry_safe=True,
        )

    async def post(
        self,
        client: httpx.AsyncClient,
        url: str | httpx.URL,
        *,
        headers: Mapping[str, str] | None = None,
        json: Mapping[str, object] | None = None,
        data: Mapping[str, str] | None = None,
        files: Mapping[str, tuple[str, bytes, str]] | None = None,
        retry_safe: bool = False,
    ) -> httpx.Response:
        """Выполнить POST с политикой времени и повторов текущего исполнителя."""
        return await self.request(
            client,
            "POST",
            url,
            headers=headers,
            json=json,
            data=data,
            files=files,
            retry_safe=retry_safe,
        )

    def _can_retry(
        self,
        failure: AdapterRequestFailure,
        *,
        attempt: int,
        retry_safe: bool,
    ) -> bool:
        """Проверить, допускает ли политика повтор после указанного отказа."""
        return (
            retry_safe
            and self.policy.operation_type == "read"
            and failure.retryable
            and attempt < self.policy.max_attempts
            and self.remaining_seconds > 0
        )
