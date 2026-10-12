from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.setting.config import Settings
from src.setting.transcription import resolve_transcription_config, transcription_available


def test_transcription_is_neutralized_in_tests() -> None:
    # conftest blanks the real .env values; without that this would resolve a
    # live provider and the offline paths below could never be exercised.
    assert resolve_transcription_config(Settings()) is None


def test_defaults_resolve_to_no_configuration() -> None:
    settings = Settings(_env_file=None)  # type: ignore[call-arg]

    assert resolve_transcription_config(settings) is None
    assert transcription_available(settings) is False


def test_legacy_openrouter_settings_still_resolve() -> None:
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        openrouter_enabled=True,
        openrouter_api_key="or-secret",
    )

    config = resolve_transcription_config(settings)

    assert config is not None
    assert config.kind == "openrouter"
    assert config.source == "legacy_openrouter"
    assert config.base_url_setting == "openrouter_base_url"
    assert config.model == "openai/whisper-large-v3"
    assert config.timeout_seconds == 60
    assert config.audio_window_seconds is None
    assert transcription_available(settings) is True


def test_self_hosted_provider_is_valid_without_api_key() -> None:
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        transcription_enabled=True,
        transcription_provider="openai_compatible",
        transcription_base_url="http://host.docker.internal:8011/v1",
        transcription_model="openai/whisper-large-v3",
        transcription_audio_window_seconds=30,
    )

    config = resolve_transcription_config(settings)

    assert config is not None
    assert config.kind == "openai_compatible"
    assert config.source == "transcription"
    assert config.api_key == ""
    assert config.base_url == "http://host.docker.internal:8011/v1"
    assert config.audio_window_seconds == 30
    assert config.response_format == "verbose_json"


def test_openrouter_provider_requires_an_api_key() -> None:
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        transcription_enabled=True,
        transcription_provider="openrouter",
        transcription_base_url="https://openrouter.ai/api/v1",
        transcription_model="openai/whisper-large-v3",
    )

    assert resolve_transcription_config(settings) is None


def test_enabled_falls_back_to_openrouter_flag_during_deprecation_window() -> None:
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        openrouter_enabled=True,
        transcription_provider="openai_compatible",
        transcription_base_url="http://vllm:8000/v1",
        transcription_model="openai/whisper-large-v3",
    )

    config = resolve_transcription_config(settings)

    assert config is not None
    assert config.kind == "openai_compatible"


def test_explicit_disable_wins_over_legacy_flag() -> None:
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        openrouter_enabled=True,
        openrouter_api_key="or-secret",
        transcription_enabled=False,
    )

    assert resolve_transcription_config(settings) is None


def test_explicit_provider_never_mixes_in_legacy_fields() -> None:
    # A half-filled TRANSCRIPTION_* block must fail closed rather than silently
    # borrowing the model name from the deprecated OPENROUTER_* block.
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        transcription_enabled=True,
        transcription_provider="openai_compatible",
        transcription_base_url="http://vllm:8000/v1",
        openrouter_transcription_model="openai/whisper-large-v3",
    )

    assert resolve_transcription_config(settings) is None


def test_timeout_falls_back_to_legacy_timeout() -> None:
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        transcription_enabled=True,
        transcription_provider="openai_compatible",
        transcription_base_url="http://vllm:8000/v1",
        transcription_model="whisper",
        openrouter_timeout_seconds=42,
    )

    config = resolve_transcription_config(settings)

    assert config is not None
    assert config.timeout_seconds == 42


def test_explicit_timeout_wins() -> None:
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        transcription_enabled=True,
        transcription_provider="openai_compatible",
        transcription_base_url="http://vllm:8000/v1",
        transcription_model="whisper",
        transcription_timeout_seconds=120,
        openrouter_timeout_seconds=42,
    )

    config = resolve_transcription_config(settings)

    assert config is not None
    assert config.timeout_seconds == 120


def test_unknown_provider_is_rejected_at_settings_level() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, transcription_provider="bogus")  # type: ignore[call-arg]


def test_base_url_trailing_slash_is_normalized() -> None:
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        transcription_enabled=True,
        transcription_provider="openai_compatible",
        transcription_base_url="http://vllm:8000/v1/",
        transcription_model="whisper",
    )

    config = resolve_transcription_config(settings)

    assert config is not None
    assert config.base_url == "http://vllm:8000/v1"


def test_config_repr_hides_the_api_key() -> None:
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        transcription_enabled=True,
        transcription_provider="openrouter",
        transcription_base_url="https://openrouter.ai/api/v1",
        transcription_model="openai/whisper-large-v3",
        transcription_api_key="or-secret",
    )

    config = resolve_transcription_config(settings)

    assert config is not None
    assert "or-secret" not in repr(config)


def test_transcription_available_tracks_config_resolution() -> None:
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        transcription_enabled=True,
        transcription_provider="openai_compatible",
        transcription_base_url="http://vllm:8000/v1",
        transcription_model="whisper",
    )

    assert transcription_available(settings) is (resolve_transcription_config(settings) is not None)


def test_transcription_enabled_is_independent_of_provider_configuration() -> None:
    from src.setting.transcription import transcription_enabled

    enabled_but_unconfigured = Settings(
        _env_file=None,  # type: ignore[call-arg]
        transcription_enabled=True,
        transcription_provider="openai_compatible",
        # base_url/model deliberately left blank
    )
    assert transcription_enabled(enabled_but_unconfigured) is True
    assert resolve_transcription_config(enabled_but_unconfigured) is None

    disabled = Settings(_env_file=None, transcription_enabled=False)  # type: ignore[call-arg]
    assert transcription_enabled(disabled) is False
