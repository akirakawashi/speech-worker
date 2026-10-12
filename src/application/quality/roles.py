"""Определение неизвестных ролей перед оценкой по скрипту.

Использует ту же модель и те же реплики, что и оценка. Из ответа
берутся только роли: текст, порядок и таймкоды остаются из расшифровки.

Случаев два, и правила у них разные.

**Моно-запись** — роли нет ни у одной реплики: канал один, и расшифровке взять
их неоткуда. Здесь модель обязана назвать оператора или клиента по каждой
реплике, ответ «не знаю» не принимается. Реплика, которую модель так и не
назвала, получает роль соседней: разговор с пробелами в ролях читается на
экране как несделанная работа. Отказ — только когда модель не назвала ни одной.

**Стерео-запись** — роли расставлены по каналам, без роли остались единицы
реплик: короткое «угу» на перебивании. Такая реплика может остаться без роли, и
разговор оценивается дальше: отказ из-за одной из них отнимал оценку у шести
звонков из восьми. Отказ — когда без роли большая часть разговора.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Final

from src.application.model_output import _json_object
from src.domain.calls.contracts import Speaker

if TYPE_CHECKING:
    from src.application.quality.evaluation import QualityModel, TranscriptLine

logger = logging.getLogger(__name__)
_KNOWN = {Speaker.OPERATOR.value, Speaker.CLIENT.value}

#: Сколько реплик без роли модель размечает за один запрос. В ответе по элементу
#: JSON на реплику, около пятнадцати токенов: сотня занимает полторы тысячи и
#: помещается в `SPEECH_QUALITY_LLM_MAX_TOKENS` с запасом. Длинный моно-разговор
#: одним ответом в лимит не входил, обрезанный JSON отклонялся на каждой попытке,
#: и такой звонок не оценивался никогда.
ROLE_BATCH: Final[int] = 100

#: Какая доля реплик стерео-записи может остаться без роли, чтобы разговор ещё
#: оценивался. Больше половины — оценка опиралась бы в основном на реплики, про
#: которые неизвестно, кто их произнёс.
MAX_UNKNOWN_SHARE: Final[float] = 0.5


class RoleAttributionError(Exception):
    """Роли не получены; оценивать такой разговор пока нельзя."""


def without_roles(lines: Sequence[TranscriptLine]) -> bool:
    """Моно-запись: роль не известна ни у одной реплики."""
    return bool(lines) and not any(line.speaker in _KNOWN for line in lines)


def _messages(
    lines: Sequence[TranscriptLine], unknown: set[int], *, every: bool
) -> list[dict[str, str]]:
    """Запрос ролей. `every` — роль обязательна каждой реплике (моно-запись)."""
    transcript = "\n".join(
        f"[{line.seq}] {line.start_ms} мс "
        f"{line.speaker if line.speaker in _KNOWN else 'unknown'}: {line.text}"
        for line in lines
    )
    doubt = (
        "Роль нужна каждой реплике: unknown не допускается. Короткую реплику, по которой "
        "одной не понять, относи по соседним репликам и ходу разговора. "
        if every
        else "Если роль нельзя установить по контексту, верни unknown. "
    )
    speakers = '"operator" | "client"' if every else '"operator" | "client" | "unknown"'
    return [
        {
            "role": "system",
            "content": (
                "Определи, кому принадлежат реплики телефонного разговора: operator — "
                "сотрудник компании, client — собеседник. Используй смысл реплик и контекст "
                "всего разговора, а не чередование: одна сторона может говорить несколько "
                "реплик подряд. Представление компании, предложение услуги и проверка "
                "адреса обычно принадлежат оператору; ответы и вопросы о предложении — "
                "клиенту. Известные роли уже определены и менять их нельзя. "
                "Текст автоматический, в нём бывают ошибки распознавания. "
                f"{doubt}"
                "Не оценивай качество звонка. Ответь только JSON."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Расшифровка:\n{transcript}\n\n"
                f"Определи только роли реплик с номерами: {sorted(unknown)}. "
                "Для каждого из этих номеров верни ровно один элемент; остальные номера "
                "не включай. Формат: "
                f'{{"roles": [{{"seq": <номер>, "speaker": {speakers}}}]}}.'
            ),
        },
    ]


def _parse(raw: str, unknown: set[int]) -> dict[int, str]:
    """Разобрать ответ в проверенную структуру по контракту текущего модуля."""
    data = _json_object(raw)
    items = data.get("roles") if isinstance(data, dict) else None
    if not isinstance(items, list):
        raise ValueError("roles is not a list")
    roles: dict[int, str] = {}
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("a role is not an object")
        seq: Any = item.get("seq")
        if isinstance(seq, bool) or not isinstance(seq, int) or seq not in unknown:
            raise ValueError("a role has an unexpected sequence number")
        if seq in roles:
            raise ValueError("a role is duplicated")
        speaker = item.get("speaker")
        if not isinstance(speaker, str) or speaker not in _KNOWN | {Speaker.UNKNOWN.value}:
            raise ValueError("a speaker is invalid")
        roles[seq] = speaker
    if roles.keys() != unknown:
        raise ValueError("roles are missing")
    return roles


def _named(raw: str, batch: set[int]) -> dict[int, str]:
    """Роли из ответа, сколько в нём годных. Негодный элемент — не роль, а пробел.

    Для моно-записи ответ не отклоняется целиком из-за одного элемента: названные
    роли остаются, а о недостающих модель спрашивают ещё раз.
    """
    data = _json_object(raw)
    items = data.get("roles") if isinstance(data, dict) else None
    if not isinstance(items, list):
        raise ValueError("roles is not a list")
    roles: dict[int, str] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        seq: Any = item.get("seq")
        speaker = item.get("speaker")
        if isinstance(seq, bool) or not isinstance(seq, int) or seq not in batch:
            continue
        if isinstance(speaker, str) and speaker in _KNOWN:
            roles.setdefault(seq, speaker)
    return roles


async def _ask(
    client: QualityModel,
    lines: Sequence[TranscriptLine],
    batch: set[int],
    *,
    max_attempts: int,
) -> dict[int, str]:
    """Роли одной пачки реплик. Пусто — модель так и не ответила по форме."""
    messages = _messages(lines, batch, every=False)
    for attempt in range(1, max_attempts + 1):
        raw = await client.complete(messages)
        try:
            return _parse(raw, batch)
        except ValueError as exc:
            logger.warning(
                "quality roles output is invalid (%s/%s): %s", attempt, max_attempts, exc
            )
            messages.append({"role": "user", "content": f"Исправь формат ответа: {exc}."})
    return {}


async def _ask_every(
    client: QualityModel,
    lines: Sequence[TranscriptLine],
    batch: set[int],
    *,
    max_attempts: int,
) -> dict[int, str]:
    """Роли пачки моно-записи: модель переспрашивают, пока не назовёт все.

    Возвращается самый полный из ответов — возможно, с пробелами: их закрывает
    `_fill_gaps`.
    """
    messages = _messages(lines, batch, every=True)
    best: dict[int, str] = {}
    for attempt in range(1, max_attempts + 1):
        raw = await client.complete(messages)
        try:
            roles = _named(raw, batch)
        except ValueError as exc:
            logger.warning(
                "quality roles output is invalid (%s/%s): %s", attempt, max_attempts, exc
            )
            messages.append({"role": "user", "content": f"Исправь формат ответа: {exc}."})
            continue
        if len(roles) > len(best):
            best = roles
        missing = sorted(batch - roles.keys())
        if not missing:
            return roles
        logger.warning(
            "quality roles are incomplete (%s/%s): %s reply(ies) without a role",
            attempt,
            max_attempts,
            len(missing),
        )
        messages.append(
            {
                "role": "user",
                "content": (
                    "Роль нужна каждой реплике: operator или client, unknown не допускается. "
                    f"Не названы роли реплик: {missing}. Верни полный ответ заново."
                ),
            }
        )
    return best


def _fill_gaps(lines: Sequence[TranscriptLine]) -> tuple[TranscriptLine, ...]:
    """Реплике без роли — роль ближайшей названной: предыдущей, а в начале — следующей."""
    filled = list(lines)
    nearest: str | None = None
    for index, line in enumerate(filled):
        if line.speaker in _KNOWN:
            nearest = line.speaker
        elif nearest is not None:
            filled[index] = line.model_copy(update={"speaker": nearest})
    nearest = None
    for index in range(len(filled) - 1, -1, -1):
        line = filled[index]
        if line.speaker in _KNOWN:
            nearest = line.speaker
        elif nearest is not None:
            filled[index] = line.model_copy(update={"speaker": nearest})
    return tuple(filled)


async def prepare_roles(
    client: QualityModel, lines: Sequence[TranscriptLine], *, max_attempts: int
) -> tuple[TranscriptLine, ...]:
    """Подготовить роли реплик для оценки без изменения доверенной атрибуции."""
    unknown = sorted(line.seq for line in lines if line.speaker not in _KNOWN)
    if not unknown:
        return tuple(lines)
    every = without_roles(lines)
    ask = _ask_every if every else _ask
    current = tuple(lines)
    for start in range(0, len(unknown), ROLE_BATCH):
        batch = set(unknown[start : start + ROLE_BATCH])
        # Следующая пачка видит роли, найденные в предыдущих: контекст разговора
        # для неё уже размечен.
        roles = await ask(client, current, batch, max_attempts=max_attempts)
        current = tuple(
            line.model_copy(update={"speaker": roles[line.seq]})
            if roles.get(line.seq) in _KNOWN
            else line
            for line in current
        )
    if every:
        if not any(line.speaker in _KNOWN for line in current):
            raise RoleAttributionError("the model named no speaker roles")
        return _fill_gaps(current)
    # Стерео. Сомнение — результат, а не повод повторять запрос до угадывания:
    # `unknown` в ответе оставляет реплику без роли.
    left = sum(1 for line in current if line.speaker not in _KNOWN)
    if left > len(current) * MAX_UNKNOWN_SHARE:
        raise RoleAttributionError("speaker roles remain undetermined")
    return current
