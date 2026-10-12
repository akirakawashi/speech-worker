"""Воркер выполняет DTO бэкенда, нормализует сеть и отдаёт повторы очереди."""

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from src.application.dto.calls import JobErrorClass
from src.application.dto.maintenance import MaintenanceRequest
from src.application.dto.model_execution import ModelMessage
from src.application.dto.quality import Evaluation
from src.application.dto.quality_batch import LineSnapshot
from src.application.dto.quality_execution import (
    QualityModelRequest,
    QualityStep,
    QualityTotals,
    QualityWork,
)
from src.application.errors.quality import QualityModelError
from src.application.pipeline.stages import StageError
from src.application.quality.execution import QualityExecutionService
from src.infrastructure.clients.quality import PreparedQualityModelClient, QualityModelClient
from src.infrastructure.clients.worker_control import BackendWorkerControl


@pytest.fixture
def steps():
    """Подготовить исходный снимок и готовые сообщения с параметрами бэкенда."""
    snapshot = LineSnapshot(lines=(), read_at=datetime.now(UTC))
    request = QualityModelRequest(
        model="chosen-by-backend",
        max_tokens=2048,
        prompt_version="test-script",
        messages=(ModelMessage(role="user", content="provided prompt"),),
    )
    result = QualityWork(
        snapshot=snapshot,
        model_name=request.model,
        script_version=request.prompt_version,
        totals=QualityTotals(),
        evaluation=Evaluation(
            model_name=request.model, script_version=request.prompt_version, script_applies=False
        ),
    )
    return QualityStep(snapshot=snapshot, request=request), QualityStep(
        snapshot=snapshot, result=result
    )


async def test_http_flow_preserves_backend_parameters_and_returns_raw_response(
    monkeypatch, steps
) -> None:
    """Локальных промптов нет; API модели получает выданные сообщения и ограничения."""
    pending, completed = steps
    seen = []

    async def handler(request):
        payload = json.loads(request.content)
        seen.append(request.url.path)
        if request.url.path == "/internal/v1/quality-steps":
            assert request.headers["Authorization"] == "Bearer internal-token"
            assert payload["contract_version"] == "1.0"
            if payload["responses"]:
                assert payload["responses"] == ["raw provider response"]
                assert payload["snapshot"] == pending.snapshot.model_dump(mode="json")
                return httpx.Response(200, json=completed.model_dump(mode="json"))
            return httpx.Response(200, json=pending.model_dump(mode="json"))
        assert request.url.path == "/v1/chat/completions"
        assert payload == {
            "model": "chosen-by-backend",
            "max_tokens": 2048,
            "temperature": 0,
            "messages": [{"role": "user", "content": "provided prompt"}],
        }
        return httpx.Response(
            200, json={"choices": [{"message": {"content": "raw provider response"}}]}
        )

    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs),
    )
    execution = QualityExecutionService(
        BackendWorkerControl(base_url="http://backend", api_token="internal-token"),
        PreparedQualityModelClient(
            QualityModelClient(
                base_url="http://model/v1", model="ignored-local-model", max_tokens=999
            )
        ),
    )
    assert await execution.evaluate(uuid4(), attempts=1, max_attempts=3) == completed.result
    assert seen == [
        "/internal/v1/quality-steps",
        "/v1/chat/completions",
        "/internal/v1/quality-steps",
    ]


async def test_retryable_model_failure_returns_to_queue_before_final_attempt(steps) -> None:
    """Первый таймаут не превращается в окончательно неудавшуюся оценку."""
    pending, _ = steps
    backend = AsyncMock()
    backend.prepare.return_value = pending
    model = AsyncMock()
    model.complete.side_effect = QualityModelError("timeout", retryable=True)
    with pytest.raises(StageError) as failure:
        await QualityExecutionService(backend, model).evaluate(uuid4(), attempts=1, max_attempts=3)
    assert failure.value.error_class is JobErrorClass.TIMEOUT
    assert backend.prepare.await_count == 1


async def test_last_network_failure_is_returned_to_backend_for_normalized_result(steps) -> None:
    """Последний отказ получает DTO для атомарного сохранения в транзакции задания."""
    pending, completed = steps
    result = completed.result.model_copy(
        update={"evaluation": None, "totals": None, "failure_reason": "model_error"}
    )
    backend = AsyncMock()
    backend.prepare.side_effect = [pending, QualityStep(snapshot=pending.snapshot, result=result)]
    model = AsyncMock()
    model.complete.side_effect = QualityModelError("unreachable", retryable=True)
    actual = await QualityExecutionService(backend, model).evaluate(
        uuid4(), attempts=3, max_attempts=3
    )
    assert actual == result
    assert backend.prepare.await_args.args[0].model_error is True
    assert backend.prepare.await_args.args[0].responses == ()


@pytest.mark.parametrize(
    "status,error",
    [
        (404, JobErrorClass.NOT_FOUND),
        (409, JobErrorClass.PROVIDER_ERROR),
        (503, JobErrorClass.PROVIDER_ERROR),
        (401, JobErrorClass.INVALID_OUTPUT),
    ],
)
async def test_backend_refusals_keep_only_error_class(monkeypatch, status, error) -> None:
    """В ошибку очереди не попадает тело ответа с токеном или расшифровкой."""
    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: real_client(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(status, text="private transcript and token")
            ),
            **kwargs,
        ),
    )
    control = BackendWorkerControl(base_url="http://backend", api_token="secret-token")
    with pytest.raises(StageError) as failure:
        await control.plan(MaintenanceRequest(action="ncc"))
    assert failure.value.error_class is error
    assert "private" not in str(failure.value)


async def test_unknown_contract_version_is_never_executed(monkeypatch, steps) -> None:
    """Неизвестный протокол отклоняется до вызова модели."""
    body = steps[0].model_dump(mode="json")
    body["contract_version"] = "2.0"
    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: real_client(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json=body)), **kwargs
        ),
    )
    model = AsyncMock()
    with pytest.raises(StageError) as failure:
        await QualityExecutionService(
            BackendWorkerControl(base_url="http://backend", api_token="token"), model
        ).evaluate(uuid4(), attempts=1, max_attempts=3)
    assert failure.value.error_class is JobErrorClass.INVALID_OUTPUT
    model.complete.assert_not_awaited()
