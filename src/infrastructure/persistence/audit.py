"""Журнал действий с персональными данными.

Фиксируется **факт** доступа, а не то, что было получено. Раскрытый текст,
содержимое разговора и сами персональные данные в `payload` не попадают:
журнал, хранящий то, ради ограничения доступа к чему он ведётся, — вторая копия
той же утечки.

Тип события берётся из закрытого набора (AUT-03). Не строка по вкусу
вызывающего: тип, который каждый пишет как хочет, невозможно ни отфильтровать,
ни посчитать, а именно по нему администратор ищет, кто и что смотрел.

Запись идёт **в транзакции вызывающего**. Отдельная означала бы, что действие и
запись о нём могут разойтись: доступ состоялся, журнал молчит — или наоборот.

Модуль живёт в `app/speech`, а не рядом с HTTP-сервисом, потому что в журнал
пишет не только он: удаление записей по сроку хранения — тоже действие с ПДн, и
самое необратимое из всех. Второй реализации правил обрезки payload быть не
должно — иначе одна из них однажды начнёт хранить то, что вторая прячет.
"""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.audit import normalize_payload as normalize_payload
from src.application.context import current_request_id
from src.application.dto.audit import (
    MAX_PAYLOAD_KEYS as MAX_PAYLOAD_KEYS,
    MAX_VALUE_LENGTH as MAX_VALUE_LENGTH,
    AuditActor as AuditActor,
    UnknownAuditEvent as UnknownAuditEvent,
)
from src.application.dto.calls import SPEECH_AUDIT_EVENT_TYPES

#: Сколько ключей payload переживёт запись. Ограничение не от жадности:
#: `payload` — единственное место, куда вызывающий кладёт что хочет, и без
#: потолка туда однажды попадёт весь ответ вместе с разговором.

#: Длина значения. По той же причине.

_INSERT_SQL = text(
    """
    INSERT INTO speech_audit_events (
        id, occurred_at, event_type, actor_sub, actor_name, actor_roles,
        call_id, request_id, payload
    )
    VALUES (
        gen_random_uuid(), now(), :event_type, :actor_sub, :actor_name,
        CAST(:actor_roles AS varchar(64)[]), :call_id, :request_id,
        CAST(:payload AS jsonb)
    )
    """
)


async def record_action(
    session: AsyncSession,
    actor: AuditActor,
    event_type: str,
    *,
    call_id: Any = None,
    payload: dict[str, Any] | None = None,
) -> None:
    """Записать событие в транзакции вызывающего."""
    if event_type not in SPEECH_AUDIT_EVENT_TYPES:
        raise UnknownAuditEvent(
            f"{event_type!r} is not one of the audit event types declared in contracts.py; "
            "an event nobody can filter for is an event that is not there"
        )
    await session.execute(
        _INSERT_SQL,
        {
            "event_type": event_type,
            "actor_sub": actor.sub,
            "actor_name": actor.name,
            "actor_roles": list(actor.roles),
            "call_id": call_id,
            "request_id": current_request_id(),
            "payload": json.dumps(normalize_payload(payload), ensure_ascii=False),
        },
    )
