"""Multipart-транспорт Whisper и разбор verbose_json."""

from __future__ import annotations

from src.application.dto.transcription import (
    TranscriptionError,
    TranscriptionProviderKind,
    TranscriptionRequest,
    TranscriptionResult,
    TranscriptionSegment,
)
from src.infrastructure.transcription.http import (
    HttpTranscriptionProvider,
    TranscriptionHttpRequest,
)


class OpenAICompatibleAudioProvider(HttpTranscriptionProvider):
    """Multipart transport used by OpenAI and OpenAI-compatible servers (vLLM)."""

    kind: TranscriptionProviderKind = "openai_compatible"

    def build_http_request(self, request: TranscriptionRequest) -> TranscriptionHttpRequest:
        # Engine knobs go in first so the fields this transport owns always win:
        # a stray "model" in configuration must not silently retarget the call.
        """Преобразовать DTO распознавания в технический HTTP-запрос провайдера."""
        data: dict[str, str] = dict(self.config.extra_options)
        data.update(
            {
                "model": self.model,
                "response_format": self.config.response_format,
            }
        )
        if request.language:
            data["language"] = request.language

        return TranscriptionHttpRequest(
            url=f"{self.config.base_url}/audio/transcriptions",
            headers=self.auth_headers(),
            data=data,
            files={"file": (request.filename, request.content, request.mime_type)},
        )

    def parse_payload(
        self,
        payload: object,
        request: TranscriptionRequest,
    ) -> TranscriptionResult:
        """Преобразовать ответ провайдера в DTO результата распознавания."""
        if not isinstance(payload, dict):
            raise TranscriptionError("provider_error", detail_code="malformed_response")

        text = payload.get("text")
        if not isinstance(text, str) or not text.strip():
            raise TranscriptionError("provider_error", detail_code="empty_transcript")

        language = payload.get("language")
        duration = payload.get("duration")
        duration_seconds = float(duration) if isinstance(duration, (int, float)) else None

        return TranscriptionResult(
            text=text.strip(),
            provider_kind=self.kind,
            model=self.model,
            language=str(language) if language is not None else request.language,
            # OpenAI-compatible responses carry no confidence score. Synthesizing
            # one from segment log-probabilities would be a fabricated signal,
            # which is worse than an honest None.
            confidence=None,
            duration_seconds=duration_seconds,
            truncation_check=self.evaluate_truncation(duration_seconds),
            segments=_parse_segments(payload.get("segments")),
        )


def _parse_segments(raw: object) -> tuple[TranscriptionSegment, ...]:
    """Разобрать `segments` из `verbose_json`, если они есть.

    Сегмент без таймкодов или без текста пропускается, а не заменяется нулями:
    реплика с началом в нуле встала бы в начало разговора и попала бы в цитату
    поиска не на своём месте. Отсутствие сегментов целиком — не ошибка: при
    `response_format=json` их и не должно быть.
    """
    if not isinstance(raw, list):
        return ()
    parsed: list[TranscriptionSegment] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        start, end = _as_float(item.get("start")), _as_float(item.get("end"))
        text = item.get("text")
        if start is None or end is None or not isinstance(text, str) or not text.strip():
            continue
        parsed.append(
            TranscriptionSegment(
                start_s=start,
                end_s=max(start, end),
                text=text.strip(),
                avg_logprob=_as_float(item.get("avg_logprob")),
                no_speech_prob=_as_float(item.get("no_speech_prob")),
                compression_ratio=_as_float(item.get("compression_ratio")),
            )
        )
    return tuple(parsed)


def _as_float(value: object) -> float | None:
    """Преобразовать значение провайдера в число; неподходящее вернуть пустым."""
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None
