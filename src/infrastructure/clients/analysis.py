"""Клиент воркфлоу Dify «Speech: анализ звонка».

Существующий `app/dify/client.py` сюда не годится, и это не вопрос вкуса: он
разговаривает с **чат-приложениями** потоком событий (`/chat-messages`,
server-sent events), а анализ звонка — это воркфлоу, у которого один
блокирующий вызов `/workflows/run` и структурный ответ. Общего между ними —
базовый адрес и заголовок авторизации.

Ключ приложения свой, а не из `DIFY_AGENT_KEYS`: там ключи агентов чата, у
каждого своё приложение. Смешав их, мы получили бы настройку, в которой нельзя
отключить анализ звонков, не отключив чат.
"""

from __future__ import annotations

import json
from typing import Any, Final

import httpx
from pydantic import BaseModel, ConfigDict, Field

from src.application.errors.external import (
    AnalysisError as AnalysisError,
    AnalysisNotConfigured as AnalysisNotConfigured,
)
from src.setting.config import Settings

#: Ответ воркфлоу приходит целиком, а не потоком: разбирать частичный JSON
#: незачем — до валидации он всё равно бесполезен.
RESPONSE_MODE: Final[str] = "blocking"

#: Кем воркфлоу видит вызывающего. Не пользователь: этап работает сам по себе,
#: и подставлять сюда чей-то идентификатор значило бы приписывать человеку
#: действия ночного конвейера.
WORKFLOW_USER: Final[str] = "speech-pipeline"


class SpeechAnalysisClient(BaseModel):
    """Один вызов: отдать разговор, получить тему, тональность и резюме."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    base_url: str
    app_key: str = Field(repr=False)
    timeout_seconds: float = 120.0

    async def run(self, inputs: dict[str, Any]) -> dict[str, Any]:
        """Выполнить воркфлоу и вернуть его `outputs`.

        Как значения приводятся к полям входа — см. `_as_input`.
        """
        payload = {
            "inputs": {key: _as_input(value) for key, value in inputs.items()},
            "response_mode": RESPONSE_MODE,
            "user": WORKFLOW_USER,
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(
                    f"{self.base_url}/workflows/run",
                    headers={"Authorization": f"Bearer {self.app_key}"},
                    json=payload,
                )
        except httpx.TimeoutException as exc:
            raise AnalysisError("timeout", retryable=True) from exc
        except httpx.HTTPError as exc:
            raise AnalysisError("unreachable", retryable=True) from exc

        if response.status_code >= 500:
            raise AnalysisError(f"http_{response.status_code}", retryable=True)
        if response.status_code >= 400:
            # 4xx — это неверный ключ или неверная форма входа. Повтор даст то
            # же самое; чинить надо настройку или сам воркфлоу.
            raise AnalysisError(f"http_{response.status_code}", retryable=False)

        try:
            body = response.json()
        except ValueError as exc:
            raise AnalysisError("malformed_response", retryable=True) from exc
        return _outputs(body)


def _as_input(value: Any) -> Any:
    """Значение в том виде, в каком его принимает поле входа воркфлоу.

    Три правила, и каждое куплено формой чужого API:

    * **флаг уходит числом.** Логического типа у полей входа Dify нет — есть
      строка, абзац, число, список, файл, — и флаг приходится объявлять числом.
      Питоновский `True` прошёл бы проверку числа случайно (`bool` там наследник
      `int`), но опираться на это нельзя: проверка написана про числа. Одна
      правка на их стороне — и воркфлоу отвечает `400` на каждый звонок, а
      выглядит это как «Dify сломался»;
    * **вложенное уходит строкой JSON.** Поля скалярные, и список реплик,
      переданный массивом, доехал бы туда неузнаваемым;
    * **скаляры и `None` — как есть.**
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (str, int, float)) or value is None:
        return value
    return json.dumps(value, ensure_ascii=False)


def _outputs(body: Any) -> dict[str, Any]:
    """Извлечь структурный outputs воркфлоу либо вернуть ошибку формата."""
    if not isinstance(body, dict):
        raise AnalysisError("malformed_response", retryable=True)
    data = body.get("data")
    if not isinstance(data, dict):
        raise AnalysisError("malformed_response", retryable=True)
    status = str(data.get("status") or "")
    if status and status != "succeeded":
        # Воркфлоу отработал и сам сообщил об отказе — это его внутренняя
        # ошибка, а не наша. Повторить стоит: узел мог не дождаться модели.
        raise AnalysisError(f"workflow_{status}", retryable=True)
    outputs = data.get("outputs")
    if not isinstance(outputs, dict):
        raise AnalysisError("no_outputs", retryable=True)
    return outputs


def build_analysis_client(settings: Settings) -> SpeechAnalysisClient:
    """Собрать клиент по настройкам `SPEECH_DIFY_*`.

    Отказ при пустом адресе или ключе, а не молчаливое отключение: воркер, тихо
    решивший, что анализировать нечем, оставил бы суточный пакет без тем и
    тональности, ничего об этом не сказав.
    """
    base_url = str(getattr(settings, "speech_dify_base_url", "") or "").strip().rstrip("/")
    app_key = str(getattr(settings, "speech_dify_app_key", "") or "").strip()
    if not base_url or not app_key:
        raise AnalysisNotConfigured(
            "call analysis needs SPEECH_DIFY_BASE_URL and SPEECH_DIFY_APP_KEY; without them "
            "the daily batch would be searchable but unlabelled, and nothing would say why"
        )
    return SpeechAnalysisClient(
        base_url=base_url,
        app_key=app_key,
        timeout_seconds=float(getattr(settings, "speech_dify_timeout_seconds", 120.0)),
    )
