"""Настройки, которые правит администратор, а не выкатка.

Живут в `speech_settings`, умолчания объявлены в
[`contracts.py`](contracts.py). Читаются здесь, а не каждым модулем по-своему,
по одной причине: **отсутствие строки и заданное значение должны различаться
одинаково везде**. Модуль, подставляющий своё умолчание при пустом ответе,
рано или поздно подставит его и там, где администратор осознанно выставил
другое, — и понять это по поведению будет нельзя.

Значение проверяется по типу умолчания. Настройка, испорченная руками в базе
(строка вместо числа, отрицательный срок), — это не повод молча взять
умолчание: срок хранения, прочитанный «как получилось», удаляет чужие данные не
тогда, когда должен.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.dto.calls import SPEECH_DEFAULT_SETTINGS
from src.application.runtime_settings import (
    UnknownSetting as UnknownSetting,
    effective_setting,
    validated_setting as validated_setting,
)

logger = logging.getLogger(__name__)

_READ_SQL = text("SELECT value FROM speech_settings WHERE key = :key")
_READ_ALL_SQL = text("SELECT key, value FROM speech_settings")

# Limits are per setting: channel zero and a disabled short-call threshold
# are valid, while zero retention or concurrency is not.


async def read_runtime_settings(session: AsyncSession) -> dict[str, Any]:
    """One bounded snapshot, refreshed before each job and daily batch."""
    stored = {row["key"]: row["value"] for row in (await session.execute(_READ_ALL_SQL)).mappings()}
    return {key: effective_setting(key, stored.get(key)) for key in SPEECH_DEFAULT_SETTINGS}


async def read_setting(session: AsyncSession, key: str) -> Any:
    """Значение настройки из базы либо объявленное умолчание."""
    if key not in SPEECH_DEFAULT_SETTINGS:
        raise UnknownSetting(f"{key!r} is not declared in SPEECH_DEFAULT_SETTINGS")
    stored = (await session.execute(_READ_SQL, {"key": key})).scalar_one_or_none()
    return effective_setting(key, stored)


async def read_positive_int(session: AsyncSession, key: str) -> int:
    """Настройка-число, про которую известно, что ноль и минус бессмысленны.

    Срок хранения `0` — это «удалить всё сегодня», и принять такое значение из
    базы молча значило бы дать опечатке стереть архив.
    """
    value = await read_setting(session, key)
    default = SPEECH_DEFAULT_SETTINGS[key]
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        logger.warning(
            "speech_settings[%s] is not a positive number; using the default",
            key,
        )
        assert isinstance(default, int)
        return default
    return value
