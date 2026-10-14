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
    speech_worker_temp_dir: Path = Path("/tmp/speech-worker")

    # Способ получения записей NCC.
    speech_ncc_source_kind: str = "local_export"
    speech_ncc_export_dir: str = ""
    # Источник записей NCC.
    speech_ncc_base_url: AnyHttpUrl | str = ""
    speech_ncc_username: str = ""
    speech_ncc_api_token: str = ""
    speech_ncc_timeout_seconds: float = Field(default=60.0, ge=5.0, le=600.0)

    # Скачивание записей Телфина по выданным бэкендом ссылкам.
    speech_telphin_timeout_seconds: float = Field(default=60.0, ge=5.0, le=600.0)

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

    # Управление получением записи и доставка ASR бэкенду.
    speech_backend_base_url: str = ""
    speech_worker_api_token: str = Field(default="", repr=False)
    speech_backend_timeout_seconds: float = Field(default=30.0, ge=1.0, le=120.0)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Прочитать окружение и .env один раз при сборке команды, вернуть Settings."""
    return Settings()
