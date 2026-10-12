"""Разбор объекта JSON из ответа языковой модели."""

from __future__ import annotations

import json
import re
from typing import Any, Final

_THINK_BLOCK: Final[re.Pattern[str]] = re.compile(
    r"<think\b[^>]*>.*?</think\s*>", re.DOTALL | re.IGNORECASE
)


def _json_object(raw: str) -> dict[str, Any] | None:
    """Достать объект JSON из того, что модель написала вокруг него.

    Три вещи, и каждая встречена на настоящем прогоне:

    * **рассуждение** `<think>…</think>` перед ответом. Внутри него модель
      набрасывает черновой JSON. Ища первую `{`, разбор взял бы скобку оттуда,
      а последнюю `}` — из настоящего ответа, и между ними осталась бы проза;
    * **обёртка** ```` ```json ````, хотя модель просили не оборачивать;
    * **приписка** после объекта — «если нужно уточнить, скажите».

    Снимать это здесь дешевле, чем тратить на одно и то же обе попытки этапа.
    """
    text = _THINK_BLOCK.sub("", raw).strip()
    # Незакрытый `<think>` — обрыв ответа: считать содержимое ответом нельзя,
    # там черновик.
    if "<think" in text.lower():
        return None
    if text.startswith("```"):
        # Содержимое между первой и второй тройкой обратных кавычек.
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else text[3:]
        if text.lstrip().lower().startswith("json"):
            text = text.lstrip()[4:]
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end == -1 or end < start:
        return None
    try:
        parsed = json.loads(text[start : end + 1])
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) else None
