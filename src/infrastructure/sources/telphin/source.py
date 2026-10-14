"""Скачивание Телфина по секретной ссылке из команды бэкенда; API списка звонков здесь нет."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx

from src.application.dto.sources import CallRef
from src.application.errors.external import RecordingNotAvailable
from src.setting.config import Settings


class TelphinError(Exception):
    """Технический отказ скачивания без секретного URL или тела ответа."""


class TelphinAudioSource:
    """Читать запись потоком, закрывая HTTP-ответ и собственный клиент при любом исходе."""

    def __init__(self, *, timeout_s: float = 60, download: httpx.AsyncClient | None = None) -> None:
        """Принять таймаут и необязательный внешний транспорт, которым управляет вызывающий."""
        self._timeout_s = timeout_s
        self._download = download

    @asynccontextmanager
    async def _client(self) -> AsyncIterator[httpx.AsyncClient]:
        """Выдать внешний клиент либо открыть и закрыть клиент одной записи."""
        if self._download is not None:
            yield self._download
        else:
            async with httpx.AsyncClient(timeout=self._timeout_s) as client:
                yield client

    async def open_audio(self, ref: CallRef) -> AsyncIterator[bytes]:
        """Вернуть поток; нет URL/404/410 — RecordingNotAvailable, прочее — TelphinError."""
        if not ref.audio_url:
            raise RecordingNotAvailable("telphin recording URL is missing")
        return self._chunks(ref.audio_url)

    async def _chunks(self, url: str) -> AsyncIterator[bytes]:
        """Передать байты с закрытием ответа при успехе, ошибке и отмене; URL не раскрывать."""
        try:
            async with self._client() as client, client.stream("GET", url) as response:
                if response.status_code in (404, 410):
                    raise RecordingNotAvailable("telphin recording is missing")
                if response.status_code != 200:
                    raise TelphinError(f"telphin storage HTTP {response.status_code}")
                async for chunk in response.aiter_bytes(1 << 16):
                    yield chunk
        except httpx.HTTPError:
            raise TelphinError("telphin storage transport failed") from None


def build_telphin_audio(settings: Settings) -> TelphinAudioSource:
    """Собрать скачиватель без App ID, App Secret, фильтров и часового пояса."""
    return TelphinAudioSource(timeout_s=settings.speech_telphin_timeout_seconds)
