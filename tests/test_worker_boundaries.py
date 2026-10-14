"""Воркер ограничен приёмом, ASR и доставкой даже при обходе CLI."""

from unittest.mock import Mock

import pytest

from src.application.dto.calls import JobKind, JobLane
from src.application.pipeline.stages import StageRegistry
from src.application.pipeline.transcribe import TranscribeHandler
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
