from __future__ import annotations

from src.application.dto.transcription import TranscriptionRequest, TranscriptionResult
from src.infrastructure.transcription.http import (
    HttpTranscriptionProvider,
    TranscriptionHttpRequest,
)
from src.setting.transcription import TranscriptionConfig


class _StubProvider(HttpTranscriptionProvider):
    kind = "openai_compatible"

    def build_http_request(self, request: TranscriptionRequest) -> TranscriptionHttpRequest:
        return TranscriptionHttpRequest(
            url=f"{self.config.base_url}/audio/transcriptions",
            headers={},
        )

    def parse_payload(self, payload: object, request: TranscriptionRequest) -> TranscriptionResult:
        raise NotImplementedError


def make_config(**overrides: object) -> TranscriptionConfig:
    payload: dict[str, object] = {
        "kind": "openai_compatible",
        "base_url": "http://vllm:8000/v1",
        "api_key": "",
        "model": "openai/whisper-large-v3",
        "timeout_seconds": 60.0,
        "audio_window_seconds": None,
        "response_format": "verbose_json",
        "source": "transcription",
        "base_url_setting": "transcription_base_url",
    }
    payload.update(overrides)
    return TranscriptionConfig(**payload)  # type: ignore[arg-type]


def test_auth_headers_are_empty_when_api_key_is_blank() -> None:
    provider = _StubProvider(make_config(api_key=""))

    assert provider.auth_headers() == {}


def test_auth_headers_carry_bearer_token_when_api_key_is_set() -> None:
    provider = _StubProvider(make_config(api_key="sk-secret"))

    assert provider.auth_headers() == {"Authorization": "Bearer sk-secret"}


def test_evaluate_truncation_is_not_applicable_without_a_declared_window() -> None:
    provider = _StubProvider(make_config(audio_window_seconds=None))

    assert provider.evaluate_truncation(29.0) == "not_applicable"
    assert provider.evaluate_truncation(None) == "not_applicable"


def test_evaluate_truncation_is_unavailable_when_duration_is_missing() -> None:
    provider = _StubProvider(make_config(audio_window_seconds=30.0))

    assert provider.evaluate_truncation(None) == "unavailable"


def test_evaluate_truncation_is_suspected_near_the_window_boundary() -> None:
    provider = _StubProvider(make_config(audio_window_seconds=30.0))

    assert provider.evaluate_truncation(29.8) == "suspected"
    assert provider.evaluate_truncation(30.0) == "suspected"


def test_evaluate_truncation_is_clear_well_below_the_window() -> None:
    provider = _StubProvider(make_config(audio_window_seconds=30.0))

    assert provider.evaluate_truncation(12.4) == "clear"
