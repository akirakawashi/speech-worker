"""Прямой вызов LLM: подготовленный запрос, ответ и решения о повторе."""

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from pydantic import JsonValue

from src.application.dto.model_execution import (
    ModelMessage,
    ModelPreparationRequest,
    PreparedModelRequest,
)
from src.application.errors.external import AnalysisError
from src.infrastructure.clients.analysis import SpeechAnalysisClient


class _Preparer:
    """Запомнить исходный DTO и вернуть готовый запрос без внешних сервисов."""

    def __init__(self) -> None:
        self.requests: list[ModelPreparationRequest] = []

    async def prepare(self, request: ModelPreparationRequest) -> PreparedModelRequest:
        self.requests.append(request)
        return PreparedModelRequest(
            contract_version="1.0",
            scenario=request.scenario,
            profile=request.profile,
            prompt_version="1.0",
            model="selected-by-backend",
            messages=(ModelMessage(role="user", content="Разговор: здравствуйте"),),
            temperature=0,
            max_tokens=8192,
        )


def _transport(monkeypatch: pytest.MonkeyPatch, handler: Callable[..., Any]) -> None:
    """Подставить HTTP-транспорт; сериализацию настоящего httpx сохранить."""
    factory = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: factory(transport=httpx.MockTransport(handler), **kwargs),
    )


async def test_sends_backend_request_unchanged_and_returns_model_content(monkeypatch) -> None:
    """Модель и сообщения выбирает preparer, ответ остаётся под ключом answer."""
    sent: list[httpx.Request] = []
    answer = '{"summary":"Клиент позвонил"}'

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(
            200, json={"choices": [{"finish_reason": "stop", "message": {"content": answer}}]}
        )

    _transport(monkeypatch, handler)
    preparer = _Preparer()
    inputs: dict[str, JsonValue] = {"segments": [{"text": "Здравствуйте"}], "needs_roles": True}
    client = SpeechAnalysisClient(
        base_url="http://model/v1",
        api_key="model-secret",
        preparer=preparer,
        scenario="reason",
        profile="candidate",
    )
    assert await client.run(inputs) == {"answer": answer}
    assert preparer.requests == [
        ModelPreparationRequest(scenario="reason", profile="candidate", inputs=inputs)
    ]
    assert str(sent[0].url) == "http://model/v1/chat/completions"
    assert sent[0].headers["authorization"] == "Bearer model-secret"
    assert json.loads(sent[0].content) == {
        "model": "selected-by-backend",
        "messages": [{"role": "user", "content": "Разговор: здравствуйте"}],
        "temperature": 0,
        "max_tokens": 8192,
    }
    assert "model-secret" not in repr(client)


async def test_local_model_may_have_no_api_key(monkeypatch) -> None:
    """Локальный сервер модели без авторизации не получает пустой Bearer."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert "authorization" not in request.headers
        return httpx.Response(200, json={"choices": [{"message": {"content": "{}"}}]})

    _transport(monkeypatch, handler)
    await SpeechAnalysisClient(base_url="http://model/v1", preparer=_Preparer()).run({})


@pytest.mark.parametrize(
    ("status", "retryable"),
    [(408, True), (429, True), (500, True), (503, True), (400, False), (401, False), (404, False)],
)
async def test_http_failure_has_retry_policy_without_response_body(monkeypatch, status, retryable):
    """Секреты из чужого тела ошибки не попадают в ошибку задания."""
    _transport(monkeypatch, lambda request: httpx.Response(status, text="sensitive-model-response"))
    with pytest.raises(AnalysisError) as error:
        await SpeechAnalysisClient(base_url="http://model/v1", preparer=_Preparer()).run({})
    assert error.value.reason == f"http_{status}"
    assert error.value.retryable is retryable
    assert "sensitive" not in str(error.value)


@pytest.mark.parametrize("failure", [httpx.ReadTimeout, httpx.ConnectError])
async def test_network_failure_can_be_retried(monkeypatch, failure):
    """Истечение таймаута и недоступный сервер допускают повтор задания."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise failure("temporary failure", request=request)

    _transport(monkeypatch, handler)
    with pytest.raises(AnalysisError) as error:
        await SpeechAnalysisClient(base_url="http://model/v1", preparer=_Preparer()).run({})
    assert error.value.retryable


@pytest.mark.parametrize(
    ("body", "reason", "retryable"),
    [
        ([], "malformed_response", True),
        ({}, "malformed_response", True),
        ({"choices": []}, "malformed_response", True),
        ({"choices": [None]}, "malformed_response", True),
        ({"choices": [{"message": None}]}, "malformed_response", True),
        ({"choices": [{"message": {"content": None}}]}, "empty_response", True),
        ({"choices": [{"message": {"content": " "}}]}, "empty_response", True),
        ({"choices": [{"finish_reason": "length"}]}, "output_truncated", False),
        ({"choices": [{"message": {"refusal": "refused"}}]}, "model_refused", False),
    ],
)
async def test_malformed_empty_truncated_and_refused_responses(
    monkeypatch, body, reason, retryable
):
    """Сбой протокола отличается от отказа модели и недостаточного лимита."""
    _transport(monkeypatch, lambda request: httpx.Response(200, json=body))
    with pytest.raises(AnalysisError) as error:
        await SpeechAnalysisClient(base_url="http://model/v1", preparer=_Preparer()).run({})
    assert (error.value.reason, error.value.retryable) == (reason, retryable)


async def test_non_json_response_is_retryable(monkeypatch):
    """Прокси может ответить HTML вместо JSON; тело не журналируем."""
    _transport(monkeypatch, lambda request: httpx.Response(200, text="<html>error</html>"))
    with pytest.raises(AnalysisError) as error:
        await SpeechAnalysisClient(base_url="http://model/v1", preparer=_Preparer()).run({})
    assert (error.value.reason, error.value.retryable) == ("malformed_response", True)


async def test_preparation_failure_does_not_call_model(monkeypatch):
    """При отказе подготовки запрос к модели не выполняется."""

    class FailingPreparer:
        async def prepare(self, request: ModelPreparationRequest) -> PreparedModelRequest:
            raise AnalysisError("invalid_model_request", retryable=False)

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("model called after preparation failure")

    _transport(monkeypatch, handler)
    with pytest.raises(AnalysisError, match="invalid_model_request"):
        await SpeechAnalysisClient(base_url="http://model/v1", preparer=FailingPreparer()).run({})
