"""Клиент Whisper для конвейера.

Транспорт берётся у основного приложения (`app/transcription`), а не пишется
заново: multipart-запрос, заголовок авторизации, повторы с бюджетом времени и
дисциплина «в ошибку не попадает тело ответа провайдера» (BR-13) там уже
решены и проверены.

Своё здесь только две вещи:

* **настройки.** Конвейер и чат могут использовать разные серверы и модели.
    Конвейеру нужен настоящий Whisper на icai2. Общая пара
  настроек означала бы, что выключить одно нельзя, не выключив другое.
* **`verbose_json`.** Конвейеру нужны реплики с таймкодами, а не сплошной
  текст: на них строится и поиск с цитатами, и определение ролей по каналам.

Замер SP-003: скорость 13,2× реального времени (RTF ≈ 0,076), сегменты приходят с
`start`/`end` и признаками качества.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Final

import httpx
from pydantic import BaseModel, ConfigDict

from src.application.dto.transcription import (
    TranscriptionError,
    TranscriptionRequest,
    TranscriptionResult,
)
from src.application.errors.external import (
    WhisperNotConfigured as WhisperNotConfigured,
)
from src.infrastructure.http.execution import AdapterExecution
from src.infrastructure.transcription.providers import OpenAICompatibleAudioProvider
from src.infrastructure.transcription.service import resolve_transcription_execution_policy
from src.setting.config import Settings
from src.setting.transcription import TranscriptionConfig

#: Даунмикс пишется в WAV: он уже подготовлен `ffmpeg`, и перекодировать его в
#: сжатый формат ради передачи значит потерять то, ради чего готовили.
AUDIO_MIME: Final[str] = "audio/wav"
AUDIO_FORMAT: Final[str] = "wav"


class SpeechTranscriptionClient(BaseModel):
    """Одна операция: распознать файл и вернуть реплики."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    config: TranscriptionConfig
    language: str

    async def transcribe(self, path: Path) -> TranscriptionResult:
        """Распознать подготовленный файл.

        Общий TranscriptionRequest принимает bytes; multipart сам по себе
        буферизации не требует. Час PCM 16 кГц, mono, 16 bit — около 115 МБ
        без накладных расходов запроса. Чтение вынесено из event loop.
        """
        content = await asyncio.to_thread(path.read_bytes)
        request = TranscriptionRequest(
            filename=path.name,
            content=content,
            mime_type=AUDIO_MIME,
            audio_format=AUDIO_FORMAT,
            size_bytes=len(content),
            language=self.language,
        )
        provider = OpenAICompatibleAudioProvider(self.config)
        execution = AdapterExecution(resolve_transcription_execution_policy(self.config))
        try:
            async with execution, httpx.AsyncClient() as client:
                return await provider.transcribe(request, execution=execution, client=client)
        except TimeoutError as exc:
            # Общий срок вызова срабатывает при выходе из `execution`, то есть
            # уже снаружи провайдера, и его перевод в класс туда не дотягивается:
            # наружу шёл голый `TimeoutError`, и воркер писал `unexpected_error`.
            # Основное приложение переводит его так же (`transcribe_audio`).
            # Отмену снаружи это не задевает: истёкшим сроком `asyncio.timeout`
            # называет только собственный, остальная отмена выходит как есть.
            raise TranscriptionError("timeout") from exc


def build_whisper_client(settings: Settings) -> SpeechTranscriptionClient:
    """Собрать клиент по настройкам `SPEECH_WHISPER_*`.

    Отказ при пустом адресе, а не молчаливое отключение: воркер, тихо
    решивший, что распознавать нечем, оставил бы суточный пакет в очереди без
    единой ошибки в журнале.
    """
    base_url = str(getattr(settings, "speech_whisper_base_url", "") or "").strip().rstrip("/")
    if not base_url:
        raise WhisperNotConfigured(
            "speech transcription needs SPEECH_WHISPER_BASE_URL; without it the stage would "
            "hold the whole daily batch in the queue and report nothing"
        )
    extra = {
        "vad_filter": "true" if getattr(settings, "speech_whisper_vad_filter", True) else "false"
    }

    return SpeechTranscriptionClient(
        config=TranscriptionConfig(
            kind="openai_compatible",
            base_url=base_url,
            api_key=str(getattr(settings, "speech_whisper_api_key", "") or ""),
            model=str(getattr(settings, "speech_whisper_model", "") or "large-v3"),
            timeout_seconds=float(getattr(settings, "speech_whisper_timeout_seconds", 600.0)),
            # Окно не задано: у сервера на icai2 ограничения длительности нет,
            # и объявив его, мы получили бы «подозрение на обрезку» на каждой
            # длинной записи — предупреждение, за которым ничего не стоит.
            audio_window_seconds=None,
            response_format="verbose_json",
            source="transcription",
            base_url_setting="SPEECH_WHISPER_BASE_URL",
            extra_options=extra,
        ),
        language=str(getattr(settings, "speech_whisper_language", "") or "ru"),
    )


__all__ = [
    "SpeechTranscriptionClient",
    "TranscriptionError",
    "WhisperNotConfigured",
    "build_whisper_client",
]
