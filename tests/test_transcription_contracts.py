from __future__ import annotations

from typing import get_args

import pytest

from src.application.dto.adapter import AdapterFailureReason
from src.application.dto.transcription import (
    TRANSCRIPTION_ADAPTER_NAME,
    TRANSCRIPTION_CONTRACT_VERSION,
    TranscriptionError,
    TranscriptionFailureReason,
    TranscriptionRequest,
    TranscriptionResult,
)


def make_request(**overrides: object) -> TranscriptionRequest:
    payload: dict[str, object] = {
        "filename": "call.mp3",
        "content": b"secret-audio-bytes",
        "mime_type": "audio/mpeg",
        "audio_format": "mp3",
        "size_bytes": 18,
        "language": "ru",
    }
    payload.update(overrides)
    return TranscriptionRequest(**payload)  # type: ignore[arg-type]


def test_contract_version_and_adapter_name_are_stable() -> None:
    assert TRANSCRIPTION_CONTRACT_VERSION == "1.0"
    assert TRANSCRIPTION_ADAPTER_NAME == "speech_transcription"


def test_failure_reasons_match_the_adapter_vocabulary() -> None:
    # The normalized audit `reason` field must stay inside the contract the
    # README defines for corporate adapters.
    assert set(get_args(TranscriptionFailureReason)) == set(get_args(AdapterFailureReason))


def test_error_string_carries_only_the_normalized_reason() -> None:
    exc = TranscriptionError(
        "provider_error",
        status_code=500,
        detail_code="malformed_response",
    )

    assert str(exc) == "transcription_failed:provider_error"
    assert "500" not in str(exc)
    assert "malformed_response" not in str(exc)
    assert exc.reason == "provider_error"
    assert exc.status_code == 500
    assert exc.detail_code == "malformed_response"


def test_error_defaults_have_no_provider_detail() -> None:
    exc = TranscriptionError("timeout")

    assert exc.status_code is None
    assert exc.detail_code is None
    assert exc.outcome is None


def test_request_repr_hides_audio_bytes() -> None:
    rendered = repr(make_request())

    assert "secret-audio-bytes" not in rendered
    assert "call.mp3" in rendered


def test_result_repr_hides_transcript_text() -> None:
    result = TranscriptionResult(
        text="совершенно секретная расшифровка",
        provider_kind="openai_compatible",
        model="openai/whisper-large-v3",
    )

    rendered = repr(result)

    assert "секретная" not in rendered
    assert "openai/whisper-large-v3" in rendered


def test_result_defaults_are_conservative() -> None:
    result = TranscriptionResult(
        text="ok",
        provider_kind="openrouter",
        model="openai/whisper-large-v3",
    )

    assert result.language is None
    assert result.confidence is None
    assert result.duration_seconds is None
    assert result.truncation_check == "not_applicable"


@pytest.mark.parametrize("reason", ["not_configured", "unauthorized", "timeout"])
def test_error_is_raisable_and_catchable_by_reason(reason: str) -> None:
    with pytest.raises(TranscriptionError) as exc_info:
        raise TranscriptionError(reason)  # type: ignore[arg-type]

    assert exc_info.value.reason == reason
