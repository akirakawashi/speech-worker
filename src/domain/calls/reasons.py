"""Причины обращения для контакт-центра (S-32): справочник и разбор ответа модели.

Модель по расшифровке отвечает, **зачем звонил клиент**: одной
фразой со стороны клиента и одним кодом справочника `K01…K99`. Это не то, что
оператор записал в NCC, и не тема разговора: оператор выбирает категорию своей
формы, тема — о чём шла речь, а причина — с чем клиент пришёл (фича 12).

Справочник живёт в таблице `speech_call_reasons` и загружается из файла
(`seed_call_reasons`). Модель видит только включённые коды; код, названный мимо
отправленного списка, не сохраняется — подставлять «похожий» нельзя, как и с
путём по дереву.

Пустой справочник — не ошибка: поля причины просто не заполняются. Так этап
разбора работает и до загрузки справочника, и со старой редакцией воркфлоу.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Mapping
from typing import Any, Final

from pydantic import BaseModel, ConfigDict

from src.domain.calls.classifier import clean_label

logger = logging.getLogger(__name__)

#: Фраза причины — строка отчёта и подпись в карточке. Модели сказано «до 200
#: символов»; столько же разрешает база (`ck_speech_calls_call_reason_length`).
REASON_LIMIT: Final[int] = 200

#: Вид кода: буква K и две цифры. Коды неизменны и не переиспользуются —
#: выключенный код остаётся в звонках, которые им размечены.
CODE_PATTERN: Final[re.Pattern[str]] = re.compile(r"K[0-9]{2}")

#: Код в начале ответа модели: «K05», «K05 · Не работает…», « k05 ».
_CODE_IN_ANSWER: Final[re.Pattern[str]] = re.compile(r"^\s*(K[0-9]{2})\b", re.IGNORECASE)

#: Кириллическая «К» вместо латинской: промпт, подписи и реплики — по-русски, и
#: модель может написать код русской буквой. `IGNORECASE` кириллицу в латиницу
#: не сворачивает, и верный K05 пропал бы молча.
_CODE_LOOKALIKES: Final[dict[int, str]] = str.maketrans({"К": "K", "к": "K"})

#: Сколько знаков справочника принимает вход `reasons` воркфлоу
#: (`max_length` в `dify/speech/speech-call-analysis.dify.yml`). Длиннее — Dify
#: отвечает `400` на весь разбор, а не только на причину; загрузка справочника
#: такой файл отвергает, а этап такой справочник не шлёт.
REASONS_PROMPT_LIMIT: Final[int] = 16_000

#: Разделитель кода и подписи — как в образце владельца: «K05 · Не работает…».
CODE_SEPARATOR: Final[str] = " · "


class CallReason(BaseModel):
    """Код справочника причин."""

    model_config = ConfigDict(frozen=True)

    code: str
    label: str
    #: Указание модели: границы кода и правила выбора. В отчёт не идёт.
    hint: str = ""

    @property
    def caption(self) -> str:
        """Как код показывается человеку: «K05 · Не работает домашний интернет»."""
        return f"{self.code}{CODE_SEPARATOR}{self.label}"


def prompt_reasons(reasons: Iterable[CallReason]) -> str:
    """Справочник для модели: строка на код — «K05 · подпись — подсказка»."""
    return "\n".join(
        f"{reason.caption} — {reason.hint}" if reason.hint else reason.caption for reason in reasons
    )


class ReasonAnswer(BaseModel):
    """Причина из ответа модели. Пусто — модель не ответила или ответила мимо."""

    model_config = ConfigDict(frozen=True)

    text: str | None = None
    code: str | None = None


def interpret_reason(outputs: Mapping[str, Any], codes: frozenset[str]) -> ReasonAnswer:
    """Причина и код из ответа модели. Ни одного умолчания.

    Поля необязательны, как `ai_path`: звонок без причины остаётся размеченным,
    и отказ этой части ответа не роняет разбор. Код берётся только из
    отправленного списка; «K07» при выключенном K07 — это ответ мимо, а не
    основание включить его обратно.
    """
    if not codes:
        return ReasonAnswer()
    raw_text = outputs.get("call_reason")
    text = clean_label(raw_text)[:REASON_LIMIT].strip() if isinstance(raw_text, str) else ""

    code: str | None = None
    raw_code = outputs.get("call_reason_code")
    if isinstance(raw_code, str) and raw_code.strip():
        found = _CODE_IN_ANSWER.match(raw_code.translate(_CODE_LOOKALIKES))
        candidate = found.group(1).upper() if found else None
        if candidate in codes:
            code = candidate
        else:
            logger.getChild("interpret").info(
                "the reason code %r is not in the reference sent to the model", raw_code[:40]
            )
    return ReasonAnswer(text=text or None, code=code)


__all__ = [
    "CODE_PATTERN",
    "CODE_SEPARATOR",
    "REASONS_PROMPT_LIMIT",
    "REASON_LIMIT",
    "CallReason",
    "ReasonAnswer",
    "interpret_reason",
    "prompt_reasons",
]
