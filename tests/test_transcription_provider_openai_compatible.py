from __future__ import annotations

import httpx
import pytest

from src.application.dto.transcription import TranscriptionError, TranscriptionRequest
from src.infrastructure.http.execution import AdapterExecution, AdapterExecutionPolicy
from src.infrastructure.transcription.providers import OpenAICompatibleAudioProvider
from src.setting.transcription import TranscriptionConfig


def make_config(**overrides: object) -> TranscriptionConfig:
    payload: dict[str, object] = {
        "kind": "openai_compatible",
        "base_url": "http://host.docker.internal:8011/v1",
        "api_key": "",
        "model": "openai/whisper-large-v3",
        "timeout_seconds": 60.0,
        "audio_window_seconds": 30.0,
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


async def test_openai_compatible_sends_multipart_with_file_model_and_language() -> None:
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["content_type"] = request.headers.get("content-type", "")
        body = await request.aread()
        captured["body"] = body
        return httpx.Response(200, json={"text": "ok", "duration": 5.0})

    provider = OpenAICompatibleAudioProvider(make_config())
    execution, client = make_execution(handler)
    async with client:
        async with execution:
            await provider.transcribe(make_request(), execution=execution, client=client)

    content_type = captured["content_type"]
    assert isinstance(content_type, str)
    assert content_type.startswith("multipart/form-data")

    body = captured["body"]
    assert isinstance(body, bytes)
    assert b'name="file"' in body
    assert b'filename="call.mp3"' in body
    assert b'name="model"' in body
    assert b"openai/whisper-large-v3" in body
    assert b'name="language"' in body
    assert b'name="response_format"' in body
    assert b"verbose_json" in body


async def test_openai_compatible_omits_authorization_when_api_key_is_empty() -> None:
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = {k.lower() for k in request.headers.keys()}
        return httpx.Response(200, json={"text": "ok"})

    provider = OpenAICompatibleAudioProvider(make_config(api_key=""))
    execution, client = make_execution(handler)
    async with client:
        async with execution:
            await provider.transcribe(make_request(), execution=execution, client=client)

    assert "authorization" not in captured["headers"]  # type: ignore[operator]


async def test_openai_compatible_sends_authorization_when_api_key_is_set() -> None:
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["authorization"] = request.headers.get("authorization")
        return httpx.Response(200, json={"text": "ok"})

    provider = OpenAICompatibleAudioProvider(make_config(api_key="sk-secret"))
    execution, client = make_execution(handler)
    async with client:
        async with execution:
            await provider.transcribe(make_request(), execution=execution, client=client)

    assert captured["authorization"] == "Bearer sk-secret"


async def test_openai_compatible_parses_text_and_duration() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"text": "ok", "language": "russian", "duration": 12.4})

    provider = OpenAICompatibleAudioProvider(make_config(audio_window_seconds=None))
    execution, client = make_execution(handler)
    async with client:
        async with execution:
            result = await provider.transcribe(make_request(), execution=execution, client=client)

    assert result.text == "ok"
    assert result.language == "russian"
    assert result.duration_seconds == 12.4
    assert result.confidence is None
    assert result.provider_kind == "openai_compatible"


async def test_openai_compatible_reports_truncation_suspected_at_window_boundary() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"text": "ok", "duration": 29.8})

    provider = OpenAICompatibleAudioProvider(make_config(audio_window_seconds=30.0))
    execution, client = make_execution(handler)
    async with client:
        async with execution:
            result = await provider.transcribe(make_request(), execution=execution, client=client)

    assert result.truncation_check == "suspected"


async def test_openai_compatible_reports_truncation_unavailable_without_duration() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"text": "ok"})

    provider = OpenAICompatibleAudioProvider(make_config(audio_window_seconds=30.0))
    execution, client = make_execution(handler)
    async with client:
        async with execution:
            result = await provider.transcribe(make_request(), execution=execution, client=client)

    assert result.truncation_check == "unavailable"


async def test_openai_compatible_maps_empty_text_to_provider_error() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"text": ""})

    provider = OpenAICompatibleAudioProvider(make_config())
    execution, client = make_execution(handler)
    async with client:
        with pytest.raises(TranscriptionError) as exc_info:
            async with execution:
                await provider.transcribe(make_request(), execution=execution, client=client)

    assert exc_info.value.reason == "provider_error"
    assert exc_info.value.detail_code == "empty_transcript"


async def test_openai_compatible_sends_configured_extra_options() -> None:
    # vad_filter is engine-specific (faster-whisper has it, vLLM does not), so
    # it must arrive from configuration rather than be hardcoded in the port.
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = await request.aread()
        return httpx.Response(200, json={"text": "ok"})

    provider = OpenAICompatibleAudioProvider(
        make_config(extra_options={"vad_filter": "true", "temperature": "0.0"})
    )
    execution, client = make_execution(handler)
    async with client:
        async with execution:
            await provider.transcribe(make_request(), execution=execution, client=client)

    body = captured["body"]
    assert isinstance(body, bytes)
    assert b'name="vad_filter"' in body
    assert b"true" in body
    assert b'name="temperature"' in body


async def test_openai_compatible_extra_options_cannot_override_core_fields() -> None:
    # A stray "model" in extra options must not silently retarget the request.
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = await request.aread()
        return httpx.Response(200, json={"text": "ok"})

    provider = OpenAICompatibleAudioProvider(
        make_config(extra_options={"model": "someone-elses-model"})
    )
    execution, client = make_execution(handler)
    async with client:
        async with execution:
            await provider.transcribe(make_request(), execution=execution, client=client)

    body = captured["body"]
    assert isinstance(body, bytes)
    assert b"someone-elses-model" not in body
    assert b"openai/whisper-large-v3" in body
