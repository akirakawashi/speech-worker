"""Контекст корреляции текущего сценария, независимый от транспорта."""

from __future__ import annotations

from contextvars import ContextVar

_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)


def current_request_id() -> str | None:
    """Вернуть идентификатор текущего запроса либо None вне HTTP-контекста."""
    return _request_id.get()
