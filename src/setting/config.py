"""Настройки самостоятельного воркера из окружения и .env."""

from functools import lru_cache
from pathlib import Path

from pydantic import AnyHttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Загрузить настройки очереди, записей и существующих HTTP-сервисов."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Отдельная база речи.
    speech_ms_database_url: str = ""
    # Пул базы данных.
    db_pool_size: int = Field(default=5, ge=1)
    db_max_overflow: int = Field(default=3, ge=0)
    db_pool_timeout_seconds: float = Field(default=10.0, gt=0)
    db_pool_recycle_seconds: int = Field(default=1800, ge=1)
    db_write_trace_enabled: bool = False
    speech_display_timezone: str = "Europe/Moscow"
    speech_worker_temp_dir: Path = Path("/tmp/speech-worker")

    # Способ получения записей NCC.
    speech_ncc_source_kind: str = "local_export"
    speech_ncc_export_dir: str = ""
    # Источник записей NCC.
    speech_ncc_base_url: AnyHttpUrl | str = ""
    speech_ncc_username: str = ""
    speech_ncc_api_token: str = ""
    speech_ncc_timeout_seconds: float = Field(default=60.0, ge=5.0, le=600.0)
    speech_ncc_min_talk_seconds: int = Field(default=10, ge=0, le=3600)
    speech_ncc_session_anchor: str = ""
    speech_ncc_reporting_database_url: str = ""
    speech_ncc_reporting_db_password: str = ""
    speech_ncc_reporting_views: str = ""
    speech_ncc_reporting_timezone: str = "Europe/Moscow"

    # Источник продаж Телфина.
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

    # Оригиналы записей в MinIO.
    speech_storage_base_url: str = ""
    speech_storage_bucket: str = "speech-recordings"
    speech_storage_region: str = "us-east-1"
    speech_storage_access_key_id: str = ""
    speech_storage_secret_key: str = ""
    speech_storage_timeout_seconds: float = Field(default=60.0, ge=5.0, le=600.0)

    # Существующий Whisper по HTTP.
    speech_whisper_base_url: str = ""
    speech_whisper_api_key: str = ""
    speech_whisper_model: str = "large-v3"
    speech_whisper_language: str = "ru"
    speech_whisper_timeout_seconds: float = Field(default=600.0, ge=30.0, le=1800.0)
    speech_whisper_vad_filter: bool = True

    # Подготовка запросов бэкендом.
    speech_backend_base_url: str = ""
    speech_worker_api_token: str = Field(default="", repr=False)
    speech_backend_timeout_seconds: float = Field(default=30.0, ge=1.0, le=120.0)
    # Прямой вызов LLM.
    speech_analysis_llm_base_url: str = ""
    speech_analysis_llm_api_key: str = Field(default="", repr=False)
    speech_analysis_llm_timeout_seconds: float = Field(default=120.0, ge=10.0, le=280.0)

    # Оценка продаж Телфина.
    speech_quality_llm_base_url: str = ""
    speech_quality_llm_api_key: str = ""
    speech_quality_llm_timeout_seconds: float = Field(default=180.0, ge=10.0, le=1800.0)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Прочитать окружение и .env один раз при сборке команды, вернуть Settings."""
    return Settings()
