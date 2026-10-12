from __future__ import annotations

import base64
import json

import httpx
import pytest

from src.application.dto.transcription import TranscriptionError, TranscriptionRequest
from src.infrastructure.http.execution import AdapterExecution, AdapterExecutionPolicy
from src.infrastructure.transcription.providers import OpenRouterAudioProvider
from src.setting.transcription import TranscriptionConfig


def make_config(**overrides: object) -> TranscriptionConfig:
    payload: dict[str, object] = {
        "kind": "openrouter",
        "base_url": "https://openrouter.ai/api/v1",
        "api_key": "or-secret",
        "model": "openai/whisper-large-v3",
        "timeout_seconds": 30.0,
        "audio_window_seconds": None,
        "response_format": "verbose_json",
        "source": "transcription",
        "base_url_setting": "transcription_base_url",
    }
    payload.update(overrides)
    return TranscriptionConfig(**payload)  # type: ignore[arg-type]


def make_request(**overrides: object) -> TranscriptionRequest:
    payload: dict[str, object] = {
        "filename": "call.mp3",
        "content": b"voice-bytes",
        "mime_type": "audio/mpeg",
        "audio_format": "mp3",
        "size_bytes": 11,
        "language": "ru",
    }
    payload.update(overrides)
    return TranscriptionRequest(**payload)  # type: ignore[arg-type]


def make_execution(handler: object) -> tuple[AdapterExecution, httpx.AsyncClient]:
    execution = AdapterExecution(
        AdapterExecutionPolicy(
            adapter="speech_transcription",
            operation_type="read",
            total_timeout_seconds=5.0,
            max_attempts=1,
            retry_backoff_seconds=0.0,
            retryable_status_codes=frozenset(),
        )
    )
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))  # type: ignore[arg-type]
    return execution, client


async def test_openrouter_sends_json_base64_payload_and_parses_text() -> None:
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["authorization"] = request.headers.get("authorization")
        captured["body"] = json.loads(await request.aread())
        return httpx.Response(
            200,
            json={"text": "  привет  ", "language": "russian", "confidence": 0.92},
        )

    provider = OpenRouterAudioProvider(make_config())
    execution, client = make_execution(handler)
    async with client:
        async with execution:
            result = await provider.transcribe(make_request(), execution=execution, client=client)

    assert captured["url"] == "https://openrouter.ai/api/v1/audio/transcriptions"
    assert captured["authorization"] == "Bearer or-secret"
    body = captured["body"]
    assert isinstance(body, dict)
    assert body["model"] == "openai/whisper-large-v3"
    assert body["language"] == "ru"
    assert body["input_audio"]["format"] == "mp3"
    assert base64.b64decode(body["input_audio"]["data"]) == b"voice-bytes"

    assert result.text == "привет"
    assert result.language == "russian"
    assert result.confidence == 0.92
    assert result.provider_kind == "openrouter"


async def test_openrouter_accepts_transcript_key_and_falls_back_to_requested_language() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"transcript": "ok"})

    provider = OpenRouterAudioProvider(make_config())
    execution, client = make_execution(handler)
    async with client:
        async with execution:
            result = await provider.transcribe(
                make_request(language="ru"),
                execution=execution,
                client=client,
            )

    assert result.text == "ok"
    assert result.language == "ru"


async def test_openrouter_maps_401_to_unauthorized_without_leaking_body() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "invalid api key or-secret"})

    provider = OpenRouterAudioProvider(make_config())
    execution, client = make_execution(handler)
    async with client:
        with pytest.raises(TranscriptionError) as exc_info:
            async with execution:
                await provider.transcribe(make_request(), execution=execution, client=client)

    assert exc_info.value.reason == "unauthorized"
    assert "or-secret" not in str(exc_info.value)
    assert "invalid api key" not in str(exc_info.value)


async def test_openrouter_maps_403_to_forbidden() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(403)

    provider = OpenRouterAudioProvider(make_config())
    execution, client = make_execution(handler)
    async with client:
        with pytest.raises(TranscriptionError) as exc_info:
            async with execution:
                await provider.transcribe(make_request(), execution=execution, client=client)

    assert exc_info.value.reason == "forbidden"


async def test_openrouter_maps_timeout_to_timeout_reason() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    provider = OpenRouterAudioProvider(make_config())
    execution, client = make_execution(handler)
    async with client:
        with pytest.raises(TranscriptionError) as exc_info:
            async with execution:
                await provider.transcribe(make_request(), execution=execution, client=client)

    assert exc_info.value.reason == "timeout"


async def test_openrouter_maps_malformed_body_to_provider_error() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not json")

    provider = OpenRouterAudioProvider(make_config())
    execution, client = make_execution(handler)
    async with client:
        with pytest.raises(TranscriptionError) as exc_info:
            async with execution:
                await provider.transcribe(make_request(), execution=execution, client=client)

    assert exc_info.value.reason == "provider_error"
    assert exc_info.value.detail_code == "malformed_response"


async def test_openrouter_maps_empty_text_to_provider_error() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"text": "   "})

    provider = OpenRouterAudioProvider(make_config())
    execution, client = make_execution(handler)
    async with client:
        with pytest.raises(TranscriptionError) as exc_info:
            async with execution:
                await provider.transcribe(make_request(), execution=execution, client=client)

    assert exc_info.value.reason == "provider_error"
    assert exc_info.value.detail_code == "empty_transcript"


async def test_openrouter_truncation_check_is_not_applicable_without_window() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"text": "ok"})

    provider = OpenRouterAudioProvider(make_config(audio_window_seconds=None))
    execution, client = make_execution(handler)
    async with client:
        async with execution:
            result = await provider.transcribe(make_request(), execution=execution, client=client)

    assert result.truncation_check == "not_applicable"
