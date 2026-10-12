"""HTTP-адаптер распознавания: транспорт, срок вызова и метаданные."""

from __future__ import annotations

import time

import httpx

from src.application.dto.adapter import AdapterExecutionTelemetry
from src.application.dto.transcription import (
    TRANSCRIPTION_ADAPTER_NAME,
    TranscriptionError,
    TranscriptionMetadata,
    TranscriptionOutcome,
    TranscriptionRequest,
    TranscriptionResult,
)
from src.infrastructure.http.execution import AdapterExecution, AdapterExecutionPolicy
from src.infrastructure.transcription.registry import (
    build_transcription_provider,
    require_provider_spec,
)
from src.setting.transcription import TranscriptionConfig


def resolve_transcription_execution_policy(config: TranscriptionConfig) -> AdapterExecutionPolicy:
    """Собрать бюджет времени и повторов технического вызова распознавания."""
    return AdapterExecutionPolicy(
        adapter=TRANSCRIPTION_ADAPTER_NAME,
        operation_type="read",
        total_timeout_seconds=max(config.timeout_seconds, 0.1),
        # Transcription never retries automatically: a partially uploaded
        # multipart body cannot be safely replayed, and the provider spec
        # declares retry_mode="none".
        max_attempts=1,
        retry_backoff_seconds=0.0,
        retryable_status_codes=frozenset(),
    )


async def _transcribe_request(
    *,
    config: TranscriptionConfig | None,
    request: TranscriptionRequest,
    request_id: str | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> tuple[TranscriptionResult, TranscriptionOutcome]:
    """Run one logical transcription call.

    Returns `(result, outcome)` on success. On failure, raises
    `TranscriptionError` with `.outcome` populated so the caller can enrich its
    existing audit event without a second network round-trip or a duplicate
    event.
    """

    started_at = time.monotonic()
    telemetry = AdapterExecutionTelemetry(adapter=TRANSCRIPTION_ADAPTER_NAME)

    if config is None:
        outcome = TranscriptionOutcome(
            status="error",
            reason="not_configured",
            elapsed_ms=int((time.monotonic() - started_at) * 1000),
            request_id=request_id,
            telemetry=telemetry,
            config=None,
            request=request,
            result=None,
        )
        raise TranscriptionError("not_configured", outcome=outcome)

    provider = build_transcription_provider(config)
    execution = AdapterExecution(
        resolve_transcription_execution_policy(config),
        telemetry=telemetry,
    )
    owns_client = http_client is None
    client = http_client or httpx.AsyncClient()

    try:
        async with execution:
            result = await provider.transcribe(request, execution=execution, client=client)
    except TranscriptionError as exc:
        outcome = TranscriptionOutcome(
            status="error",
            reason=exc.reason,
            elapsed_ms=int((time.monotonic() - started_at) * 1000),
            request_id=request_id,
            telemetry=telemetry,
            config=_metadata(config),
            request=request,
            result=None,
            status_code=exc.status_code,
            detail_code=exc.detail_code,
        )
        raise TranscriptionError(
            exc.reason,
            status_code=exc.status_code,
            detail_code=exc.detail_code,
            outcome=outcome,
        ) from exc
    except TimeoutError as exc:
        outcome = TranscriptionOutcome(
            status="error",
            reason="timeout",
            elapsed_ms=int((time.monotonic() - started_at) * 1000),
            request_id=request_id,
            telemetry=telemetry,
            config=_metadata(config),
            request=request,
            result=None,
        )
        raise TranscriptionError("timeout", outcome=outcome) from exc
    except Exception as exc:
        outcome = TranscriptionOutcome(
            status="error",
            reason="unexpected_error",
            elapsed_ms=int((time.monotonic() - started_at) * 1000),
            request_id=request_id,
            telemetry=telemetry,
            config=_metadata(config),
            request=request,
            result=None,
        )
        raise TranscriptionError("unexpected_error", outcome=outcome) from exc
    finally:
        if owns_client:
            await client.aclose()

    outcome = TranscriptionOutcome(
        status="success",
        reason=None,
        elapsed_ms=int((time.monotonic() - started_at) * 1000),
        request_id=request_id,
        telemetry=telemetry,
        config=_metadata(config),
        request=request,
        result=result,
    )
    return result, outcome


def _metadata(config: TranscriptionConfig) -> TranscriptionMetadata:
    """Получить метаданные конфигурации, исключив адрес сервера и ключ."""
    spec = require_provider_spec(config.kind)
    return TranscriptionMetadata(
        kind=config.kind,
        model=config.model,
        source=config.source,
        base_url_setting=config.base_url_setting,
        audio_window_seconds=config.audio_window_seconds,
        network_zone=spec.network_zone,
        auth_mode=spec.auth_mode,
        owner=spec.owner,
        version=spec.version,
    )


class HttpTranscriptionBackend:
    """Адаптер порта распознавания с явными настройками и HTTP-клиентом."""

    def __init__(
        self, config: TranscriptionConfig | None, *, http_client: httpx.AsyncClient | None = None
    ) -> None:
        """Принять конфигурацию движка и необязательный клиент вызывающего."""
        self._config = config
        self._http_client = http_client

    async def transcribe(
        self, request: TranscriptionRequest, *, request_id: str | None = None
    ) -> tuple[TranscriptionResult, TranscriptionOutcome]:
        """Отправить DTO записи и вернуть распознавание с метаданными.

        Raises:
            TranscriptionError: Нормализованный отказ с метаданными операции.
        """
        return await _transcribe_request(
            config=self._config,
            request=request,
            request_id=request_id,
            http_client=self._http_client,
        )
