"""Настройки самостоятельного воркера и его фоновых команд.

Имена SPEECH_* совместимы с текущим бэкендом. Настройки HTTP API, входа
пользователей и cookie в этот проект не входят. Чтение .env происходит
при создании Settings в DI; импорт модуля не открывает внешние ресурсы.
"""

import json
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any

from pydantic import AnyHttpUrl, Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    """Загрузить настройки очереди, записей и существующих HTTP-сервисов."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Собственная база речи; общая с API, миграциями владеет speech-backend.
    speech_ms_database_url: str = ""
    db_pool_size: int = Field(default=5, ge=1)
    db_max_overflow: int = Field(default=3, ge=0)
    db_pool_timeout_seconds: float = Field(default=10.0, gt=0)
    db_pool_recycle_seconds: int = Field(default=1800, ge=1)
    db_write_trace_enabled: bool = False
    speech_display_timezone: str = "Europe/Moscow"
    speech_worker_temp_dir: Path = Path("/tmp/speech-worker")

    # NCC: способ перечисления звонков и доступ к записям.
    speech_ncc_source_kind: str = "local_export"
    speech_ncc_export_dir: str = ""
    speech_ncc_base_url: AnyHttpUrl | str = ""
    speech_ncc_username: str = ""
    speech_ncc_api_token: str = ""
    speech_ncc_timeout_seconds: float = Field(default=60.0, ge=5.0, le=600.0)
    speech_ncc_min_talk_seconds: int = Field(default=10, ge=0, le=3600)
    speech_ncc_projects: str = ""
    speech_ncc_session_anchor: str = ""
    speech_ncc_session_domains: str = "0,1,2,3"
    speech_ncc_session_nauss: str = "0"
    speech_ncc_reporting_database_url: str = ""
    speech_ncc_reporting_db_password: str = ""
    speech_ncc_reporting_views: str = ""
    speech_ncc_reporting_timezone: str = "Europe/Moscow"

    # Телфин: отдельный источник продаж и порционный приём истории.
    speech_telphin_base_url: str = "https://apiproxy.telphin.ru"
    speech_telphin_app_id: str = ""
    speech_telphin_app_secret: str = ""
    speech_telphin_timeout_seconds: float = Field(default=60.0, ge=5.0, le=600.0)
    speech_telphin_min_talk_seconds: int = Field(default=10, ge=0, le=3600)
    speech_telphin_excluded_extensions: str = "13240*000,13240*007,13240*444"
    speech_telphin_excluded_groups: str = "22001"
    speech_telphin_history_from: str = ""
    speech_telphin_history_days_per_run: int = Field(default=7, ge=1, le=60)
    speech_phone_hash_pepper: str = ""

    # Постоянные оригиналы находятся в MinIO на отдельном сервере.
    speech_storage_base_url: str = ""
    speech_storage_bucket: str = "speech-recordings"
    speech_storage_region: str = "us-east-1"
    speech_storage_access_key_id: str = ""
    speech_storage_secret_key: str = ""
    speech_storage_timeout_seconds: float = Field(default=60.0, ge=5.0, le=600.0)

    # Единственный движок распознавания конвейера сейчас — Whisper по HTTP.
    speech_whisper_base_url: str = ""
    speech_whisper_api_key: str = ""
    speech_whisper_model: str = "large-v3"
    speech_whisper_language: str = "ru"
    speech_whisper_timeout_seconds: float = Field(default=600.0, ge=30.0, le=1800.0)
    speech_whisper_vad_filter: bool = True

    # Воркер получает готовый запрос у бэкенда и вызывает модель напрямую.
    speech_backend_base_url: str = ""
    speech_worker_api_token: str = Field(default="", repr=False)
    speech_backend_timeout_seconds: float = Field(default=30.0, ge=1.0, le=120.0)
    speech_analysis_llm_base_url: str = ""
    speech_analysis_llm_api_key: str = Field(default="", repr=False)
    speech_analysis_llm_timeout_seconds: float = Field(default=120.0, ge=10.0, le=280.0)

    # Оценка продаж Телфина запускается отдельной командой.
    speech_quality_llm_base_url: str = ""
    speech_quality_llm_api_key: str = ""
    speech_quality_llm_model: str = "qwen3.8-27b-fast"
    speech_quality_llm_timeout_seconds: float = Field(default=180.0, ge=10.0, le=1800.0)
    speech_quality_llm_max_tokens: int = Field(default=4096, ge=512, le=32768)

    # Совместимость перенесённого общего HTTP-транспорта распознавания.
    # Конвейер использует SPEECH_WHISPER_*; эти поля нужны существующему
    # resolve_transcription_config и его проверкам совместимости.
    openrouter_enabled: bool = False
    openrouter_base_url: AnyHttpUrl | str = "https://openrouter.ai/api/v1"
    openrouter_api_key: str = ""
    openrouter_transcription_model: str = "openai/whisper-large-v3"
    openrouter_timeout_seconds: int = 60
    transcription_enabled: bool | None = None
    transcription_provider: str = ""
    transcription_base_url: str = ""
    transcription_api_key: str = ""
    transcription_model: str = ""
    transcription_timeout_seconds: float | None = None
    transcription_audio_window_seconds: float = 0.0
    transcription_response_format: str = "verbose_json"
    transcription_extra_options: Annotated[dict[str, str], NoDecode] = Field(default_factory=dict)

    @field_validator("transcription_enabled", "transcription_timeout_seconds", mode="before")
    @classmethod
    def empty_string_means_unset(cls, value: Any) -> Any:
        """Прочитать пустое значение необязательного поля как отсутствие настройки."""
        if isinstance(value, str) and value.strip() == "":
            return None
        return value

    @field_validator("transcription_extra_options", mode="before")
    @classmethod
    def parse_transcription_extra_options(cls, value: Any) -> dict[str, str]:
        """Прочитать словарь или JSON параметров транспорта; неверный JSON отклонить."""
        if value in (None, ""):
            return {}
        if isinstance(value, dict):
            return {str(key): str(item) for key, item in value.items()}
        try:
            parsed = json.loads(str(value))
        except json.JSONDecodeError as exc:
            raise ValueError("transcription_extra_options must be a JSON object") from exc
        if not isinstance(parsed, dict):
            raise ValueError("transcription_extra_options must be a JSON object")
        return {str(key): str(item) for key, item in parsed.items()}

    @field_validator("transcription_provider", mode="before")
    @classmethod
    def parse_transcription_provider(cls, value: Any) -> str:
        """Нормализовать имя провайдера транспорта и проверить поддерживаемые значения."""
        provider = str(value or "").strip().lower()
        if provider not in {"", "openrouter", "openai_compatible"}:
            raise ValueError("transcription_provider must be openrouter or openai_compatible")
        return provider


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Прочитать окружение и .env один раз при сборке команды, вернуть Settings."""
    return Settings()
