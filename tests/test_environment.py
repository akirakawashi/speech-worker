"""Контракт шаблона окружения и путь .env → Settings → HTTP Whisper."""

from __future__ import annotations

import os
import re
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from src.infrastructure.clients import whisper
from src.setting.config import Settings

ROOT = Path(__file__).resolve().parents[1]


def _clear_setting_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Удалить внешние значения известных полей на время одного теста."""
    fields = set(Settings.model_fields)
    for name in os.environ:
        if name.lower() in fields:
            monkeypatch.delenv(name)


def test_environment_template_uses_supported_parameters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Проверить имена параметров и загрузить настоящий шаблон через Settings."""
    _clear_setting_environment(monkeypatch)
    template = ROOT / ".env.example"
    declared = set(re.findall(r"^([A-Z][A-Z0-9_]*)=", template.read_text(), re.M))
    supported = {name.upper() for name in Settings.model_fields}
    compose = ROOT / "docker-compose.yml"
    if compose.exists():
        supported.update(re.findall(r"\$\{([A-Z][A-Z0-9_]*)", compose.read_text()))

    assert declared <= supported, sorted(declared - supported)
    Settings(_env_file=template)


async def test_dotenv_settings_reach_the_whisper_http_request(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Прочитать настройки из файла, отправить WAV и вернуть DTO сегментов."""
    _clear_setting_environment(monkeypatch)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "SPEECH_WHISPER_BASE_URL=http://whisper.test/v1\n"
        "SPEECH_WHISPER_API_KEY=test-whisper-token\n"
        "SPEECH_WHISPER_MODEL=configured-whisper\n"
        "SPEECH_WHISPER_LANGUAGE=ru\n"
        "SPEECH_WHISPER_TIMEOUT_SECONDS=60\n"
        "SPEECH_WHISPER_VAD_FILTER=false\n"
        "OPENROUTER_ENABLED=obsolete-invalid-value\n"
        "TRANSCRIPTION_PROVIDER=obsolete-provider\n",
        encoding="utf-8",
    )
    audio = tmp_path / "call.wav"
    audio.write_bytes(b"RIFF-test-audio")
    calls = 0

    async def handle(request: httpx.Request) -> httpx.Response:
        """Проверить multipart-запрос и вернуть ответ Whisper без внешней сети."""
        nonlocal calls
        calls += 1
        assert request.method == "POST"
        assert str(request.url) == "http://whisper.test/v1/audio/transcriptions"
        assert request.headers["Authorization"] == "Bearer test-whisper-token"
        assert request.headers["Content-Type"].startswith("multipart/form-data")
        body = await request.aread()
        assert b'filename="call.wav"' in body
        assert b"RIFF-test-audio" in body
        for name, value in (
            ("model", "configured-whisper"),
            ("language", "ru"),
            ("response_format", "verbose_json"),
            ("vad_filter", "false"),
        ):
            assert f'name="{name}"\r\n\r\n{value}\r\n'.encode() in body
        return httpx.Response(
            200,
            json={
                "text": "Добрый день",
                "language": "ru",
                "duration": 1.0,
                "segments": [{"start": 0.0, "end": 1.0, "text": "Добрый день"}],
            },
        )

    http_client = httpx.AsyncClient
    monkeypatch.setattr(
        whisper,
        "httpx",
        SimpleNamespace(AsyncClient=lambda: http_client(transport=httpx.MockTransport(handle))),
    )
    client = whisper.build_whisper_client(Settings(_env_file=env_file))
    result = await client.transcribe(audio)

    assert calls == 1
    assert result.text == "Добрый день"
    assert result.model == "configured-whisper"
    assert result.language == "ru"
    assert result.duration_seconds == 1.0
    assert [(s.start_s, s.end_s, s.text) for s in result.segments] == [(0.0, 1.0, "Добрый день")]
