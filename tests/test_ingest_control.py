"""HTTP-команды получения: контракт, связь с nonce и безопасные отказы транспорта."""

from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from src.application.dto.calls import JobErrorClass, ProcessingStatus
from src.application.dto.ingest_control import IngestClaim
from src.application.dto.pipeline_settings import PipelineSettings
from src.application.dto.worker import WorkerOptions
from src.application.pipeline.stages import StageDeferred, StageError
from src.infrastructure.clients.ingest_control import BackendIngestControl
from src.setting.config import Settings


@pytest.fixture
def claim():
    """Подготовить самостоятельный nonce для проверки служебного ответа."""
    return IngestClaim(
        call_id=uuid4(), job_id=uuid4(), claim_token=uuid4(), worker_id="worker", attempt=1
    )


def transport(monkeypatch, handler):
    """Подставить транспорт HTTP, оставив сериализацию, заголовки и валидацию настоящими."""
    original = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda *a, **kw: original(*a, transport=httpx.MockTransport(handler), **kw),
    )


async def test_worker_accepts_only_correlated_command_without_admission_settings(
    monkeypatch, claim
):
    """В запросе нет порога, доли и приоритета; команду определяет ответ бэкенда."""

    def reply(request):
        assert str(request.url) == "http://backend/internal/v1/ingest/prepare"
        assert request.headers["Authorization"] == "Bearer private-key"
        assert request.content == claim.model_dump_json().encode()
        return httpx.Response(
            200,
            json={
                "claim": claim.model_dump(mode="json"),
                "completion": {
                    "work": {"outcome": "skipped_short"},
                    "stage": {"processing_status": "skipped_short"},
                },
            },
        )

    transport(monkeypatch, reply)
    result = await BackendIngestControl(base_url="http://backend", api_token="private-key").prepare(
        claim
    )
    assert result.completion is not None
    assert result.completion.stage.processing_status == ProcessingStatus.SKIPPED_SHORT


@pytest.mark.parametrize("status", [408, 409, 429, 500, 503])
async def test_backend_outage_defers_without_exposing_response(monkeypatch, claim, status):
    """Отказ управления не расходует попытку скачивания и не раскрывает его ответ."""
    transport(monkeypatch, lambda req: httpx.Response(status, text="PRIVATE_RECORDING_URL"))
    with pytest.raises(StageDeferred) as error:
        await BackendIngestControl(base_url="http://backend", api_token="secret").prepare(claim)
    assert "PRIVATE_RECORDING_URL" not in str(error.value) and error.value.__cause__ is None


@pytest.mark.parametrize(
    "status,expected",
    [
        (401, JobErrorClass.INVALID_OUTPUT),
        (404, JobErrorClass.NOT_FOUND),
        (422, JobErrorClass.INVALID_OUTPUT),
    ],
)
async def test_permanent_control_error_has_normalized_class(monkeypatch, claim, status, expected):
    """Неверные права/ввод не маскируются под отсутствие записи у источника."""
    transport(monkeypatch, lambda req: httpx.Response(status, text="PRIVATE_RESPONSE"))
    with pytest.raises(StageError) as error:
        await BackendIngestControl(base_url="http://backend", api_token="secret").prepare(claim)
    assert not isinstance(error.value, StageDeferred)
    assert error.value.error_class == expected and "PRIVATE_RESPONSE" not in str(error.value)


@pytest.mark.parametrize("change", ["nonce", "llm", "schema"])
async def test_bad_or_foreign_backend_command_is_not_executed(monkeypatch, claim, change):
    """Другой nonce, LLM-переход и неподдерживаемая версия отклоняются без вывода входа."""
    body = {
        "claim": claim.model_dump(mode="json"),
        "completion": {
            "work": {"outcome": "stored"},
            "stage": {"processing_status": "transcribing", "next_kind": "transcribe"},
        },
    }
    if change == "nonce":
        body["claim"]["claim_token"] = str(uuid4())
    elif change == "llm":
        body["completion"]["stage"]["next_kind"] = "analyze"
    else:
        body["contract_version"] = "2.0"
    body["claim"]["worker_id"] += "PRIVATE_INPUT" if change == "schema" else ""
    transport(monkeypatch, lambda req: httpx.Response(200, json=body))
    with pytest.raises(StageError) as error:
        await BackendIngestControl(base_url="http://backend", api_token="secret").prepare(claim)
    assert error.value.error_class == JobErrorClass.INVALID_OUTPUT
    assert "PRIVATE_INPUT" not in str(error.value)


def test_old_intake_environment_is_ignored_and_worker_has_no_schedule_options(monkeypatch):
    """Рабочий env сохраняется, но его старые секреты и политики больше не используются."""
    for key in (
        "SPEECH_TELPHIN_APP_SECRET",
        "SPEECH_NCC_REPORTING_DB_PASSWORD",
        "SPEECH_PHONE_HASH_PEPPER",
    ):
        monkeypatch.setenv(key, "RETIRED_PRIVATE_SECRET")
    settings = Settings(_env_file=None)
    assert "RETIRED_PRIVATE_SECRET" not in settings.model_dump_json()
    assert "short_call_threshold_s" not in PipelineSettings.model_fields
    with pytest.raises(ValidationError):
        WorkerOptions.model_validate({"sources": "ncc"})
