"""Сборка прямого вызова LLM и подготовки запроса без глобальных клиентов."""

from src.application.dto.model_execution import ModelProfile, ModelScenario
from src.application.errors.external import AnalysisNotConfigured
from src.infrastructure.clients.analysis import SpeechAnalysisClient
from src.infrastructure.clients.model_requests import BackendModelRequestPreparer
from src.setting.config import Settings


def build_analysis_client(
    settings: Settings,
    *,
    scenario: ModelScenario = "analysis",
    profile: ModelProfile = "production",
) -> SpeechAnalysisClient:
    """Проверить настройки и собрать исполнителя указанного модельного сценария."""
    base_url = settings.speech_analysis_llm_base_url.strip().rstrip("/")
    if not base_url:
        raise AnalysisNotConfigured("SPEECH_ANALYSIS_LLM_BASE_URL is required")
    backend = settings.speech_backend_base_url.strip().rstrip("/")
    if not backend or not settings.speech_worker_api_token:
        raise AnalysisNotConfigured(
            "SPEECH_BACKEND_BASE_URL and SPEECH_WORKER_API_TOKEN are required"
        )
    preparer = BackendModelRequestPreparer(
        base_url=backend,
        api_token=settings.speech_worker_api_token,
        timeout_seconds=settings.speech_backend_timeout_seconds,
    )
    return SpeechAnalysisClient(
        base_url=base_url,
        api_key=settings.speech_analysis_llm_api_key,
        timeout_seconds=settings.speech_analysis_llm_timeout_seconds,
        preparer=preparer,
        scenario=scenario,
        profile=profile,
    )
