"""Клиент языковой модели для оценки звонка по скрипту.

Запрос идёт прямо в OpenAI-совместимый `/chat/completions` — тот, что vLLM
отдаёт за `model-proxy`, — а не в воркфлоу Dify, как у анализа. Промпт и скрипт
живут в коде рядом с разбором ответа: скрипт меняется вместе с тарифами, и
правка, которую видно в диффе и которую проверяют тесты, надёжнее поля в
консоли Dify. И оценку можно проверить локально на любой совместимой модели.

`chat_template_kwargs` не отправляется. Быстрый путь выключает рассуждение
умолчанием сервера, а поле в запросе это умолчание перебивает (ADR-0011 §1):
включать рассуждение оценке незачем, а перебить умолчание случайно — легко.

Настройки свои (`SPEECH_QUALITY_LLM_*`), а не `LLM_*` основного бэкенда: иначе
нельзя было бы отключить оценку звонков, не отключив чат.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field

from src.application.errors.quality import (
    QualityModelError as QualityModelError,
    QualityModelNotConfigured as QualityModelNotConfigured,
)
from src.setting.config import Settings

#: Ответы, которые говорят о настройке, а не о звонке: неверный или пустой ключ,
#: нет доступа, неизвестный адрес или имя модели.


class QualityModelClient(BaseModel):
    """Один вызов: отдать сообщения, получить текст ответа модели."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    base_url: str
    model: str
    api_key: str = Field(default="", repr=False)
    timeout_seconds: float = 180.0
    max_tokens: int = 4096

    async def complete(self, messages: Sequence[Mapping[str, str]]) -> str:
        """Вызвать модель оценки с сообщениями и вернуть её текстовый ответ либо отказ."""
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [dict(message) for message in messages],
            # Ноль не делает ответ детерминированным — батчинг vLLM меняет числа,
            # — но убирает разброс, который зависит от нас.
            "temperature": 0,
            "max_tokens": self.max_tokens,
        }
        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(
                    f"{self.base_url}/chat/completions", headers=headers, json=payload
                )
        except httpx.TimeoutException as exc:
            raise QualityModelError("timeout", retryable=True) from exc
        except httpx.HTTPError as exc:
            raise QualityModelError("unreachable", retryable=True) from exc

        if response.status_code >= 500:
            raise QualityModelError(f"http_{response.status_code}", retryable=True)
        if response.status_code >= 400:
            # Неверный ключ, неизвестная модель или слишком длинный разговор для
            # контекста. Повтор даст то же самое; чинить надо настройку.
            raise QualityModelError(f"http_{response.status_code}", retryable=False)

        try:
            body = response.json()
        except ValueError as exc:
            raise QualityModelError("malformed_response", retryable=True) from exc
        return _content(body)


def _content(body: Any) -> str:
    """Текст ответа модели из тела `/chat/completions`."""
    choices = body.get("choices") if isinstance(body, dict) else None
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        raise QualityModelError("malformed_response", retryable=True)
    message = choices[0].get("message")
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, str) or not content.strip():
        # `length` — ответ упёрся в `max_tokens`: это не каприз модели, а потолок,
        # и в причине это должно быть видно, чтобы его поднимали, а не повторяли.
        finish = str(choices[0].get("finish_reason") or "")
        reason = "empty_answer_length" if finish == "length" else "empty_answer"
        raise QualityModelError(reason, retryable=finish != "length")
    return content


def build_quality_client(settings: Settings) -> QualityModelClient:
    """Собрать клиент по настройкам `SPEECH_QUALITY_LLM_*`.

    Отказ при пустом адресе, а не молчаливое отключение: команда, тихо решившая,
    что оценивать нечем, отчиталась бы о пустом периоде, и выглядело бы это как
    отсутствие звонков.
    """
    base_url = str(getattr(settings, "speech_quality_llm_base_url", "") or "").strip()
    if not base_url:
        raise QualityModelNotConfigured(
            "call quality evaluation needs SPEECH_QUALITY_LLM_BASE_URL (an OpenAI-compatible "
            "base address ending in /v1); without it no call can be scored"
        )
    return QualityModelClient(
        base_url=base_url.rstrip("/"),
        model=str(getattr(settings, "speech_quality_llm_model", "") or "qwen3.8-27b-fast"),
        api_key=str(getattr(settings, "speech_quality_llm_api_key", "") or ""),
        timeout_seconds=float(getattr(settings, "speech_quality_llm_timeout_seconds", 180.0)),
        max_tokens=int(getattr(settings, "speech_quality_llm_max_tokens", 4096)),
    )


__all__ = [
    "QualityModelClient",
    "QualityModelError",
    "QualityModelNotConfigured",
    "build_quality_client",
]
