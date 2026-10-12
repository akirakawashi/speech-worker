"""Правила допустимого содержимого журнала действий."""

from typing import Any, Final

from pydantic import BaseModel, ConfigDict

MAX_PAYLOAD_KEYS: Final[int] = 20

MAX_VALUE_LENGTH: Final[int] = 200


class UnknownAuditEvent(ValueError):
    """Тип события вне закрытого набора.

    Отказ, а не запись «как есть»: событие с выдуманным типом не найдётся ни
    одним фильтром, то есть его как будто и нет.
    """


class AuditActor(BaseModel):
    """Кто совершил действие — человек или задача.

    Задача записывается как задача: `auth_mode='system'` и пустые роли. Выдав
    плановому удалению роль администратора, мы получили бы журнал, в котором
    ночная задача неотличима от человека, зашедшего ночью.
    """

    model_config = ConfigDict(frozen=True)

    sub: str
    name: str | None = None
    roles: tuple[str, ...] = ()


def normalize_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    """Обрезать payload до того, что журналу позволено хранить.

    Списки и словари сворачиваются в их размер, а не сохраняются: в них
    оказывается ровно то, ради чего журнал и ведётся — реплики, тексты,
    найденные сущности.
    """
    if not payload:
        return {}
    trimmed: dict[str, Any] = {}
    for key, value in list(payload.items())[:MAX_PAYLOAD_KEYS]:
        name = str(key)[:64]
        if isinstance(value, bool) or isinstance(value, int) or isinstance(value, float):
            trimmed[name] = value
        elif isinstance(value, (list, tuple, set)):
            trimmed[name] = {"count": len(value)}
        elif isinstance(value, dict):
            trimmed[name] = {"keys": len(value)}
        elif value is None:
            trimmed[name] = None
        else:
            trimmed[name] = str(value)[:MAX_VALUE_LENGTH]
    return trimmed
