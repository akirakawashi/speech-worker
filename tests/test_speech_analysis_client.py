"""Клиент воркфлоу Dify — как он приводит вход и разбирает ответ.

Проверяется форма запроса, а не то, что «код вызывается»: воркфлоу принимает
типизированные поля, и ошибка приведения выглядит снаружи как `400` на каждый
звонок — то есть как «Dify сломался», а не как наша опечатка.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from src.infrastructure.clients.analysis import (
    AnalysisError,
    AnalysisNotConfigured,
    SpeechAnalysisClient,
    build_analysis_client,
)


def _client(handler: Any) -> SpeechAnalysisClient:
    client = SpeechAnalysisClient(base_url="http://dify/v1", app_key="app-key")
    return client


class _Settings:
    def __init__(self, url: str = "", key: str = "") -> None:
        self.speech_dify_base_url = url
        self.speech_dify_app_key = key
        self.speech_dify_timeout_seconds = 30.0


async def _run(monkeypatch: pytest.MonkeyPatch, inputs: dict[str, Any]) -> dict[str, Any]:
    """Выполнить `run` на подставном транспорте и вернуть отправленное тело."""
    sent: dict[str, Any] = {}

    async def _post(self: Any, url: str, **kwargs: Any) -> httpx.Response:
        sent["url"] = url
        sent["headers"] = kwargs.get("headers")
        sent["json"] = kwargs.get("json")
        return httpx.Response(
            200,
            json={"data": {"status": "succeeded", "outputs": {"topic_code": "other"}}},
            request=httpx.Request("POST", url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "post", _post)
    await SpeechAnalysisClient(base_url="http://dify/v1", app_key="app-key").run(inputs)
    return sent


async def test_a_flag_goes_as_a_number_not_as_true(monkeypatch: pytest.MonkeyPatch) -> None:
    """Логического типа у полей входа Dify нет, и флаг объявлен числом.

    Питоновский `True` прошёл бы проверку числа случайно — `bool` там наследник
    `int`. Опираться на это нельзя: проверка живёт в чужом коде и написана про
    числа. Одна правка на их стороне — и воркфлоу отвечает `400` на каждый
    звонок, а выглядит это как «Dify сломался».
    """
    sent = await _run(monkeypatch, {"needs_roles": True, "needs_segment": False})

    values = sent["json"]["inputs"]
    assert values["needs_roles"] == 1
    assert values["needs_segment"] == 0
    # Именно `int`, а не `bool`: `True == 1` истинно и ничего бы не поймало.
    assert type(values["needs_roles"]) is int
    assert type(values["needs_segment"]) is int


async def test_nested_structures_go_as_json_strings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Поля входа скалярные: список реплик, переданный массивом, доехал бы неузнаваемым."""
    segments = [{"seq": 1, "speaker": "unknown", "text": "здравствуйте"}]
    sent = await _run(monkeypatch, {"segments": segments, "call_id": "c-1"})

    values = sent["json"]["inputs"]
    assert values["call_id"] == "c-1", "строка не должна оборачиваться"
    assert isinstance(values["segments"], str)
    # Кириллица остаётся кириллицей: `з...` в промпте модель прочтёт хуже,
    # а человек, открывший прогон в Dify, не прочтёт вовсе.
    assert "здравствуйте" in values["segments"]
    assert json.loads(values["segments"]) == segments


async def test_the_request_carries_the_key_and_the_blocking_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent = await _run(monkeypatch, {"call_id": "c-1"})

    assert sent["url"] == "http://dify/v1/workflows/run"
    assert sent["headers"]["Authorization"] == "Bearer app-key"
    # Потоковый режим здесь не нужен: до валидации частичный JSON бесполезен.
    assert sent["json"]["response_mode"] == "blocking"
    # Не человек: этап работает сам по себе, и приписывать его действия
    # чьей-то учётной записи нельзя.
    assert sent["json"]["user"] == "speech-pipeline"


@pytest.mark.parametrize(
    ("status", "retryable"),
    [(500, True), (503, True), (400, False), (401, False), (404, False)],
)
async def test_server_errors_are_retryable_and_client_errors_are_not(
    monkeypatch: pytest.MonkeyPatch, status: int, retryable: bool
) -> None:
    """`4xx` — это неверный ключ или неверная форма входа: повтор даст то же самое."""

    async def _post(self: Any, url: str, **kwargs: Any) -> httpx.Response:
        return httpx.Response(status, json={}, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx.AsyncClient, "post", _post)

    with pytest.raises(AnalysisError) as failure:
        await SpeechAnalysisClient(base_url="http://dify/v1", app_key="k").run({})

    assert failure.value.retryable is retryable
    assert str(status) in failure.value.reason


async def test_a_workflow_that_failed_on_its_own_is_worth_repeating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Узел мог не дождаться модели — это не наша ошибка формы."""

    async def _post(self: Any, url: str, **kwargs: Any) -> httpx.Response:
        return httpx.Response(
            200,
            json={"data": {"status": "failed", "outputs": None}},
            request=httpx.Request("POST", url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "post", _post)

    with pytest.raises(AnalysisError) as failure:
        await SpeechAnalysisClient(base_url="http://dify/v1", app_key="k").run({})

    assert failure.value.retryable is True
    assert failure.value.reason == "workflow_failed"


def test_an_unconfigured_workflow_refuses_loudly() -> None:
    """Молчаливое отключение оставило бы суточный пакет без тем, ничего не сказав."""
    with pytest.raises(AnalysisNotConfigured):
        build_analysis_client(_Settings(url="", key="k"))
    with pytest.raises(AnalysisNotConfigured):
        build_analysis_client(_Settings(url="http://dify/v1", key=""))

    client = build_analysis_client(_Settings(url="http://dify/v1/", key="k"))
    # Хвостовой слэш срезан: иначе адрес собирается с двойным.
    assert client.base_url == "http://dify/v1"
