"""Проверки входного DTO, выбора ресурсов этапа и закрытия пула PostgreSQL."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from pydantic import ValidationError

from src.application.dto.worker import WorkerOptions
from src.di import worker as module
from src.domain.calls.contracts import JobKind, JobLane
from src.presentation.cli.worker import main
from src.setting.config import Settings


@pytest.mark.parametrize(
    "parameters",
    [
        {"lanes": ""},
        {"lanes": "unknown"},
        {"stages": "unknown"},
        {"stages": []},
        {"stages": "analyze"},
        {"stages": "evaluate"},
        {"stages": "transcribe,analyze"},
    ],
)
def test_worker_options_reject_invalid_selection(parameters: dict[str, object]) -> None:
    """Неверные полосы и этапы отклоняются до получения задания."""
    with pytest.raises(ValidationError):
        WorkerOptions.model_validate(parameters)


def test_worker_options_preserve_pipeline_order_and_unique_lanes() -> None:
    """Повтор параметров не создаёт повторных обработчиков или полос."""
    options = WorkerOptions.model_validate(
        {"lanes": "backfill,daily,backfill", "stages": "transcribe,ingest,transcribe", "once": True}
    )
    assert options.lanes == (JobLane.BACKFILL, JobLane.DAILY)
    assert options.stages == (JobKind.INGEST, JobKind.TRANSCRIBE)
    assert options.once is True


@pytest.mark.parametrize(
    "arguments",
    [["--lanes", ""], ["--stages", "missing"], ["--stages", "analyze"], ["--stages", "evaluate"]],
)
def test_invalid_cli_does_not_construct_external_resources(
    monkeypatch: pytest.MonkeyPatch, arguments: list[str]
) -> None:
    """Ошибка аргументов завершается кодом CLI 2 без чтения настроек DI."""
    runner = Mock(side_effect=AssertionError("invalid CLI reached DI"))
    monkeypatch.setattr(module, "run_worker", runner)
    with pytest.raises(SystemExit) as result:
        main(arguments)
    assert result.value.code == 2
    runner.assert_not_called()


@pytest.fixture
def runtime(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> SimpleNamespace:
    """Подставить только БД и технический цикл; сборка зависимостей остаётся настоящей."""
    engine = SimpleNamespace(dispose=AsyncMock())
    session = MagicMock()
    session.__aenter__.return_value = session
    sessions = Mock(return_value=session)
    worker = SimpleNamespace(
        worker_id="synthetic-worker",
        run_once=AsyncMock(),
        run_forever=AsyncMock(),
    )
    worker_factory = Mock(return_value=worker)
    monkeypatch.setattr(module, "get_speech_ms_engine", Mock(return_value=engine))
    monkeypatch.setattr(module, "get_speech_ms_sessionmaker", Mock(return_value=sessions))
    monkeypatch.setattr(module, "speech_schema_is_ready", AsyncMock(return_value=True))
    monkeypatch.setattr(module, "PostgresJobQueue", Mock())
    monkeypatch.setattr(module, "Worker", worker_factory)
    monkeypatch.setattr(module, "_install_signal_handlers", Mock())
    settings = Settings(
        _env_file=None,
        speech_worker_temp_dir=tmp_path / "scratch",
        speech_analysis_llm_base_url="http://analysis.invalid/v1",
        speech_backend_base_url="http://backend.invalid",
        speech_worker_api_token="synthetic-key",
        speech_whisper_base_url="http://whisper.invalid/v1",
        speech_storage_base_url="http://storage.invalid",
        speech_storage_access_key_id="test-access",
        speech_storage_secret_key="test-secret",
        speech_ncc_export_dir=str(tmp_path),
    )
    return SimpleNamespace(engine=engine, worker=worker, factory=worker_factory, settings=settings)


async def test_transcribe_builds_delivery_without_llm_or_recording_source(runtime) -> None:
    """ASR использует хранилище и Whisper, не создавая клиентов LLM; пул закрывается."""
    await module._run_worker(
        WorkerOptions(stages=(JobKind.TRANSCRIBE,), once=True), runtime.settings
    )
    resources = runtime.factory.call_args.args[2]
    assert not hasattr(resources, "analysis") and not hasattr(resources, "evaluation")
    assert resources.fetcher is None
    assert resources.storage is not None
    assert resources.transcription is not None
    registry = runtime.factory.call_args.args[1]
    assert registry.kinds == {JobKind.TRANSCRIBE}
    assert registry.get(JobKind.TRANSCRIBE).completes_externally is True
    runtime.worker.run_once.assert_awaited_once()
    runtime.worker.run_forever.assert_not_awaited()
    runtime.engine.dispose.assert_awaited_once()


async def test_startup_failure_disposes_pool_before_processing(runtime) -> None:
    """Ненастроенный выбранный этап останавливает запуск, сохраняя закрытие пула."""
    settings = runtime.settings.model_copy(update={"speech_worker_api_token": ""})
    with pytest.raises(RuntimeError):
        await module._run_worker(WorkerOptions(stages=(JobKind.TRANSCRIBE,)), settings)
    runtime.factory.assert_not_called()
    runtime.engine.dispose.assert_awaited_once()


async def test_incompatible_schema_stops_before_claiming_a_job(runtime, monkeypatch) -> None:
    """Воркер не обрабатывает общую очередь на неверной ревизии схемы."""
    monkeypatch.setattr(module, "speech_schema_is_ready", AsyncMock(return_value=False))
    with pytest.raises(RuntimeError, match="speech-backend migrations"):
        await module._run_worker(WorkerOptions(stages=(JobKind.TRANSCRIBE,)), runtime.settings)
    runtime.factory.assert_not_called()
    runtime.engine.dispose.assert_awaited_once()


async def test_long_running_worker_disposes_pool_when_loop_finishes(runtime) -> None:
    """Штатная остановка рабочего цикла проходит через освобождение ресурсов DI."""
    await module._run_worker(WorkerOptions(stages=(JobKind.TRANSCRIBE,)), runtime.settings)
    runtime.worker.run_forever.assert_awaited_once()
    runtime.worker.run_once.assert_not_awaited()
    runtime.engine.dispose.assert_awaited_once()


async def test_ingest_once_needs_no_models_or_scheduled_intake(runtime, monkeypatch) -> None:
    """Одно задание приёма не запускает расписание и не создаёт клиентов моделей."""
    maintenance = Mock(side_effect=AssertionError("once must not start collectors"))
    monkeypatch.setattr(module, "build_maintenance", maintenance)
    await module._run_worker(WorkerOptions(stages=(JobKind.INGEST,), once=True), runtime.settings)
    resources = runtime.factory.call_args.args[2]
    assert not hasattr(resources, "analysis") and not hasattr(resources, "evaluation")
    assert resources.transcription is None
    assert resources.fetcher is not None and resources.storage is not None
    maintenance.assert_not_called()
    runtime.worker.run_once.assert_awaited_once()
    runtime.engine.dispose.assert_awaited_once()
