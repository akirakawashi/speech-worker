"""Ограничения технических ролей, допустимых в результате распознавания."""

from collections.abc import Sequence

from src.domain.calls.contracts import RoleMethod, Speaker


def validate_asr_attribution(role_method: RoleMethod, speakers: Sequence[Speaker]) -> None:
    """Проверить метод и роли ASR; вернуть None либо ValueError без текста разговора.

    Текстовую атрибуцию выполняет бэкенд. Без измерения канала участник
    остаётся unknown; известные роли допустимы только для технического метода.
    """
    if role_method is RoleMethod.LLM_ATTRIBUTION:
        raise ValueError("LLM attribution is not a transcription result")
    if role_method is RoleMethod.NONE and any(
        speaker is not Speaker.UNKNOWN for speaker in speakers
    ):
        raise ValueError("unmeasured roles must be unknown")
