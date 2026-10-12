"""Бюджет времени и повторов HTTP-вызова распознавания."""

from src.application.dto.transcription import TRANSCRIPTION_ADAPTER_NAME
from src.infrastructure.http.execution import AdapterExecutionPolicy
from src.setting.transcription import TranscriptionConfig


def resolve_transcription_execution_policy(config: TranscriptionConfig) -> AdapterExecutionPolicy:
    """Собрать бюджет времени и повторов технического вызова распознавания."""
    return AdapterExecutionPolicy(
        adapter=TRANSCRIPTION_ADAPTER_NAME,
        operation_type="read",
        total_timeout_seconds=max(config.timeout_seconds, 0.1),
        # Частично отправленное multipart-тело не повторяем автоматически.
        max_attempts=1,
        retry_backoff_seconds=0.0,
        retryable_status_codes=frozenset(),
    )
