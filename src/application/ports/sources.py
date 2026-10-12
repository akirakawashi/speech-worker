"""Контракт источника звонков и потока записи."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import date
from typing import Protocol, runtime_checkable

from src.application.dto.sources import CallRef


@runtime_checkable
class DailyBatchSource(Protocol):
    """Источник суточного пакета."""

    @property
    def name(self) -> str:
        """Короткое имя источника — попадает в журналы и в панель админа."""
        ...

    async def list_calls(self, day: date) -> Sequence[CallRef]:
        """Все звонки за указанную дату.

        Порядок не важен, дубли недопустимы. Пустой результат — валидный ответ
        (выходной день), он не должен отличаться от ошибки на уровне типа:
        ошибка обязана быть исключением.
        """
        ...

    async def open_audio(self, ref: CallRef) -> AsyncIterator[bytes]:
        """Байты аудио потоком.

        Потоком, а не целиком: запись на час — это десятки мегабайт, и читать
        её в память ради того, чтобы тут же выложить в хранилище, незачем.

        Отсутствие записи выражается `RecordingNotAvailable` и **только им**.
        Любое другое исключение означает, что источник сломан, а не что записи
        нет, — и обрабатывается по-другому.
        """
        ...
