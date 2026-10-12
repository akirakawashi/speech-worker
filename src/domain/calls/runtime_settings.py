"""Правила администраторских настроек и безопасные значения по умолчанию."""

import logging
import re
from copy import deepcopy
from typing import Any

from src.domain.calls.contracts import SPEECH_DEFAULT_SETTINGS

logger = logging.getLogger(__name__)


_INTEGER_RANGES = {
    "retention_days": (1, 36500),
    "short_call_threshold_s": (0, 3600),
    "stereo_operator_channel": (0, 1),
    "transcribe_concurrency": (1, 32),
    "backfill_window_days": (1, 36500),
}


class UnknownSetting(KeyError):
    """Ключ, не объявленный в `SPEECH_DEFAULT_SETTINGS`.

    Опечатка в имени иначе вернула бы «умолчание» несуществующей настройки,
    то есть тихо изменила бы поведение на то, которого никто не выбирал.
    """


def validated_setting(key: str, value: Any) -> Any:
    """Проверить значение настройки по ключу; вернуть копию либо ошибку без самого значения."""
    if key not in SPEECH_DEFAULT_SETTINGS:
        raise UnknownSetting(key)
    # `Any`, а не выведенный тип: `SPEECH_DEFAULT_SETTINGS` объявлен как
    # `dict[str, object]`, и сравнение `type(value) is not type(default)` сузило бы
    # `value` до `object` — после чего ни сравнить с границей, ни пройти по ключам
    # его уже нельзя.
    default: Any = SPEECH_DEFAULT_SETTINGS[key]
    if type(value) is not type(default):
        raise ValueError(f"wrong_type: {key} expects {type(default).__name__}")
    if key in _INTEGER_RANGES:
        lower, upper = _INTEGER_RANGES[key]
        if not lower <= value <= upper:
            raise ValueError(f"out_of_range: {key} must be between {lower} and {upper}")
    elif key == "max_attempts":
        assert isinstance(default, dict)
        if set(value) != set(default) or any(
            type(v) is not int or not 1 <= v <= 100 for v in value.values()
        ):
            raise ValueError("invalid max_attempts: supply all stages with integers 1..100")
    elif key == "stereo_role_method" and value != "stereo_energy":
        raise ValueError("unsupported stereo_role_method: only stereo_energy is implemented")
    elif key == "daily_ingest_at" and not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", value):
        raise ValueError("invalid daily_ingest_at: expected HH:MM")
    return deepcopy(value)


def effective_setting(key: str, stored: Any) -> Any:
    """Вернуть проверенное значение или безопасное умолчание без записи содержимого в журнал."""
    if key not in SPEECH_DEFAULT_SETTINGS:
        raise UnknownSetting(key)
    if stored is not None:
        try:
            return validated_setting(key, stored)
        except ValueError:
            logger.warning("speech_settings[%s] is invalid; using the default", key)
    return deepcopy(SPEECH_DEFAULT_SETTINGS[key])
