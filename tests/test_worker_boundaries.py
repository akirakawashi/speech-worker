"""Воркер ограничен приёмом, ASR и доставкой даже при обходе CLI."""

import json
from datetime import UTC, datetime
from unittest.mock import Mock

import httpx
import pytest
from pydantic import ValidationError

from src.application.dto.calls import JobKind, JobLane
from src.application.dto.maintenance import MaintenanceRequest
from src.application.pipeline.stages import StageError, StageRegistry
from src.application.pipeline.transcribe import TranscribeHandler
from src.infrastructure.clients.worker_control import BackendWorkerControl
from src.infrastructure.queue.worker import Worker
from src.setting.config import Settings


@pytest.mark.parametrize("kind", [JobKind.ANALYZE, JobKind.EVALUATE])
def test_runtime_rejects_foreign_handlers_before_database_access(kind):
    """Подстановка LLM-обработчика в реестр не разрешает запускать его на воркере."""
    registry = StageRegistry()
    registry.register(Mock(kind=kind))
    with pytest.raises(ValueError, match="only ingest and transcribe"):
        Worker(Mock(), registry, Mock(), lanes=[JobLane.DAILY])


async def test_raw_recognition_cannot_store_transcription_locally():
    """Старый путь store не записывает текст и не завершает задание независимо от API."""
    persistence = Mock()
    with pytest.raises(RuntimeError, match="belongs to the backend"):
        await TranscribeHandler().store(persistence, Mock(), Mock())
    assert persistence.mock_calls == []


def test_existing_env_can_contain_retired_llm_keys_without_exposing_them(monkeypatch):
    """Рабочий env сохраняется; удалённые модельные секреты игнорируются Settings."""
    monkeypatch.setenv("SPEECH_ANALYSIS_LLM_API_KEY", "retired-private-key")
    monkeypatch.setenv("SPEECH_QUALITY_LLM_API_KEY", "retired-quality-key")
    settings = Settings(_env_file=None)
    assert not any(
        name.startswith(("speech_analysis_llm_", "speech_quality_llm_"))
        for name in Settings.model_fields
    )
    assert "retired-private-key" not in settings.model_dump_json()
    assert "retired-quality-key" not in repr(settings)


@pytest.mark.parametrize("source", ["ncc", "telphin"])
async def test_worker_accepts_backend_intake_contract_with_null_evaluation(monkeypatch, source):
    """Совместимый ответ 1.0 передаёт только приём и не создаёт LLM-клиентов."""
    original = httpx.AsyncClient

    def respond(request):
        assert request.url == "https://backend.invalid/internal/v1/worker-plan"
        assert request.headers["Authorization"] == "Bearer worker-secret"
        assert json.loads(request.content)["action"] == source
        return httpx.Response(
            200,
            json={
                "contract_version": "1.0",
                "action": source,
                "run_at": datetime.now(UTC).isoformat(),
                "wait_seconds": 0,
                "ingest": {"source": source, "timezone": "Europe/Moscow"},
                "evaluation": None,
            },
        )

    transport = httpx.MockTransport(respond)
    monkeypatch.setattr(
        httpx, "AsyncClient", lambda *args, **kwargs: original(*args, transport=transport, **kwargs)
    )
    plan = await BackendWorkerControl(
        base_url="https://backend.invalid", api_token="worker-secret"
    ).plan(MaintenanceRequest(action=source))
    assert plan.ingest.source == source and plan.evaluation is None


async def test_worker_rejects_a_model_action_returned_by_backend(monkeypatch):
    """Некорректный ответ управления не возвращает воркеру задачу оценки или её текст."""
    original = httpx.AsyncClient
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(
            200,
            json={
                "contract_version": "1.0",
                "action": "quality",
                "run_at": datetime.now(UTC).isoformat(),
                "evaluation": {"private": "PRIVATE_TRANSCRIPT"},
            },
        )
    )
    monkeypatch.setattr(
        httpx, "AsyncClient", lambda *args, **kwargs: original(*args, transport=transport, **kwargs)
    )
    with pytest.raises(StageError) as error:
        await BackendWorkerControl(
            base_url="https://backend.invalid", api_token="worker-secret"
        ).plan(MaintenanceRequest(action="ncc"))
    assert "PRIVATE_TRANSCRIPT" not in str(error.value)


def test_worker_cannot_request_quality_schedule():
    """Постановка оценки принадлежит CPU-процессу бэкенда, а не приёму записей."""
    with pytest.raises(ValidationError):
        MaintenanceRequest(action="quality")
