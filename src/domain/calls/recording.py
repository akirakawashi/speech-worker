"""Правило ключа записи по идентификатору звонка и его дате."""

from __future__ import annotations

from datetime import datetime
from typing import Final

_UNSAFE_SEGMENTS: Final[frozenset[str]] = frozenset({"", ".", ".."})


class InvalidRecordingKey(ValueError):
    """Идентификатор записи содержит недопустимый сегмент пути."""

    def __init__(self, message: str, *, reason: str = "bad_object_key") -> None:
        """Принять описание нарушения и код ошибки формата ключа."""
        self.reason = reason
        super().__init__(message)


def _safe_part(value: str, *, what: str) -> str:
    """Кусок ключа, в котором не спрятан переход по каталогам.

    Ключ собирается из данных **источника** — идентификатора сессии и даты, —
    а подписывается и отправляется дословно: `canonical_uri` оставляет `/` и
    точки как есть, потому что в ключе они законны. Значит `..` доедет до
    хранилища `..`, и объект ляжет не туда, куда мы считаем. Обратная косая
    здесь тоже запрещена: в ключе S3 это обычный символ, а в имени, которое
    кто-то потом развернёт на диск, — разделитель.
    """
    if "\\" in value:
        raise InvalidRecordingKey(f"{what} содержит обратную косую черту", reason="bad_object_key")
    for segment in value.split("/"):
        if segment in _UNSAFE_SEGMENTS:
            raise InvalidRecordingKey(
                f"{what} содержит сегмент {segment!r} — ключ вышел бы за свой префикс",
                reason="bad_object_key",
            )
    return value


def object_key(session_id: str, day: datetime, *, date_path: str | None = None) -> str:
    """Ключ объекта для записи звонка.

    `date_path` передаётся, когда дата известна отдельно от времени — а это
    обычный случай: источник может знать только дату звонка.
    """
    if date_path is None:
        moment = day
        date_path = moment.strftime("%Y/%m/%d")
    # Идентификатор сессии — целиком один сегмент: разделителя в нём быть не
    # должно вовсе, а не «не должно быть перехода вверх».
    if "/" in session_id:
        raise InvalidRecordingKey("идентификатор сессии содержит `/`", reason="bad_object_key")
    return (
        f"calls/{_safe_part(date_path, what='путь даты')}/"
        f"{_safe_part(session_id, what='идентификатор сессии')}.mp3"
    )
