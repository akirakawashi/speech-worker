"""Контракт подготовки запросов: бэкенд владеет промптом, воркер исполняет DTO."""

import json
from typing import Any

import httpx
import pytest

from src.application.dto.model_execution import (
    ModelMessage,
    ModelPreparationRequest,
    PreparedModelRequest,
)
from src.application.errors.external import AnalysisError, AnalysisNotConfigured
from src.di.analysis import build_analysis_client
from src.infrastructure.clients.model_requests import BackendModelRequestPreparer
from src.setting.config import Settings
from tests.test_speech_analysis_client import _transport


def _prepared() -> dict[str, Any]:
    """Вернуть синтетический ответ бэкенда без исходников соседнего проекта."""
    return PreparedModelRequest(
        contract_version="1.0",
        scenario="analysis",
        profile="production",
        prompt_version="1.0",
        model="backend-model",
        messages=(ModelMessage(role="user", content="Подготовленный разговор"),),
        temperature=0,
        max_tokens=8192,
    ).model_dump(mode="json")


async def test_worker_calls_backend_then_model_with_separate_credentials(monkeypatch) -> None:
    """В LLM уходит готовый промпт; служебный токен остаётся в запросе к бэкенду."""
    sent: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        if request.url.host == "backend":
            assert request.url.path == "/internal/v1/model-requests"
            assert request.headers["authorization"] == "Bearer worker-secret"
            assert json.loads(request.content) == {
                "contract_version": "1.0",
                "scenario": "analysis",
                "profile": "production",
                "inputs": {"segments": [{"text": "исходный разговор"}]},
            }
            return httpx.Response(200, json=_prepared())
        assert request.url.host == "model"
        assert request.headers["authorization"] == "Bearer model-secret"
        assert json.loads(request.content)["messages"] == [
            {"role": "user", "content": "Подготовленный разговор"}
        ]
        return httpx.Response(200, json={"choices": [{"message": {"content": "{}"}}]})

    _transport(monkeypatch, handler)
    settings = Settings(
        _env_file=None,
        speech_backend_base_url="http://backend/",
        speech_worker_api_token="worker-secret",
        speech_analysis_llm_base_url="http://model/v1/",
        speech_analysis_llm_api_key="model-secret",
    )
    client = build_analysis_client(settings)
    assert await client.run({"segments": [{"text": "исходный разговор"}]}) == {"answer": "{}"}
    assert len(sent) == 2
    assert client.base_url == "http://model/v1"
    assert "worker-secret" not in repr(client.preparer)


@pytest.mark.parametrize(
    "change",
    [
        {"contract_version": "2.0"},
        {"scenario": "reason"},
        {"profile": "candidate"},
        {"unexpected": "raw-secret"},
        {"messages": []},
        {"model": ""},
    ],
)
async def test_invalid_contract_is_not_forwarded_to_model(monkeypatch, change) -> None:
    """Неизвестный DTO и подмена назначения требуют исправления, повтор не поможет."""
    _transport(monkeypatch, lambda request: httpx.Response(200, json={**_prepared(), **change}))
    preparer = BackendModelRequestPreparer(base_url="http://backend", api_token="worker-secret")
    with pytest.raises(AnalysisError) as error:
        await preparer.prepare(ModelPreparationRequest(scenario="analysis", inputs={}))
    assert not error.value.retryable
    assert "raw-secret" not in str(error.value)


async def test_response_requires_explicit_contract_version(monkeypatch) -> None:
    """Ответ без версии не считается совместимым только из-за значений по умолчанию."""
    body = _prepared()
    del body["contract_version"]
    _transport(monkeypatch, lambda request: httpx.Response(200, json=body))
    preparer = BackendModelRequestPreparer(base_url="http://backend", api_token="worker-secret")
    with pytest.raises(AnalysisError) as error:
        await preparer.prepare(ModelPreparationRequest(scenario="analysis", inputs={}))
    assert (error.value.reason, error.value.retryable) == ("invalid_model_request", False)


@pytest.mark.parametrize(
    ("status", "retryable"), [(401, False), (422, False), (408, True), (429, True), (503, True)]
)
async def test_backend_refusals_have_retry_policy(monkeypatch, status, retryable) -> None:
    """Текст чужой ошибки не попадает в сообщение о неудаче задания."""
    _transport(monkeypatch, lambda request: httpx.Response(status, text="private transcript"))
    preparer = BackendModelRequestPreparer(base_url="http://backend", api_token="worker-secret")
    with pytest.raises(AnalysisError) as error:
        await preparer.prepare(ModelPreparationRequest(scenario="analysis", inputs={}))
    assert (error.value.reason, error.value.retryable) == (f"backend_http_{status}", retryable)
    assert "private transcript" not in str(error.value)


@pytest.mark.parametrize(
    "missing",
    ["speech_backend_base_url", "speech_worker_api_token", "speech_analysis_llm_base_url"],
)
def test_analysis_requires_backend_access_and_model_address(missing) -> None:
    """Неполные настройки останавливают сборку клиента до обработки очереди."""
    settings = Settings(
        _env_file=None,
        speech_backend_base_url="http://backend",
        speech_worker_api_token="worker-secret",
        speech_analysis_llm_base_url="http://model/v1",
    ).model_copy(update={missing: ""})
    with pytest.raises(AnalysisNotConfigured):
        build_analysis_client(settings)
