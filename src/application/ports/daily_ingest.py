"""Порты атомарной регистрации дня и хранения прогресса истории."""

from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from datetime import date, datetime
from typing import Protocol
from uuid import UUID

from src.application.dto.backfill import BackfillState
from src.application.dto.daily_ingest import IngestSettings, ProjectForIngest, RegisteredCall
from src.application.dto.sources import CallRef
from src.application.ports.queue import NewJob


class IngestTransaction(Protocol):
    """Все операции одной регистрации дня в общей транзакции."""

    @property
    def classifier_created(self) -> int:
        """Вернуть число новых узлов, добавленных этим прогоном."""
        ...

    async def try_lock(self, call_source: str) -> bool:
        """Захватить блокировку этого источника до конца транзакции."""
        ...

    async def settings(self) -> IngestSettings:
        """Получить проверенные пределы попыток обработки."""
        ...

    async def project(
        self, key: str | None, title: str | None, direction: str | None = None
    ) -> ProjectForIngest | None:
        """Сопоставить проект источника и вернуть разрешение на его приём."""
        ...

    async def operator(self, login: str | None, name: str | None = None) -> UUID | None:
        """Сопоставить оператора источника, если он известен."""
        ...

    async def classification(self, levels: Sequence[str]) -> UUID | None:
        """Сопоставить путь классификации, добавляя отсутствующие узлы."""
        ...

    async def upsert(
        self,
        ref: CallRef,
        *,
        operator_id: UUID | None,
        project_id: UUID | None,
        operator_node_id: UUID | None,
    ) -> RegisteredCall:
        """Идемпотентно зарегистрировать метаданные звонка."""
        ...

    async def enqueue(self, job: NewJob) -> UUID | None:
        """Поставить первый этап в той же транзакции, не меняя известное задание."""
        ...

    async def touch_project(self, project_id: UUID, moment: datetime) -> None:
        """Обновить время последнего звонка проекта."""
        ...

    async def record_day(self, day: date, source: str) -> None:
        """Зафиксировать полностью обработанный день источника."""
        ...


class DailyIngestRepository(Protocol):
    """Открытие общей транзакции регистрации дня."""

    def transaction(self) -> AbstractAsyncContextManager[IngestTransaction]:
        """Открыть транзакцию; успех коммитит все операции, отказ откатывает их."""
        ...


class BackfillRepository(Protocol):
    """Короткие атомарные операции прогресса исторического приёма."""

    async def read_state(self, cursor: str) -> BackfillState:
        """Получить текущее окно и курсор либо ошибку отсутствующего окна."""
        ...

    async def open_window(self, cursor: str, first: datetime, until: datetime) -> BackfillState:
        """Сохранить уже проверенное окно и вернуть начальное состояние."""
        ...

    async def pause(self, paused: bool) -> BackfillState:
        """Изменить паузу перечисления и вернуть состояние."""
        ...

    async def advance(self, cursor: str, to: datetime, done: bool) -> BackfillState:
        """Сохранить новый курсор и признак окончания одной транзакцией."""
        ...

    async def default_window_days(self) -> int:
        """Прочитать проверенную глубину окна из настроек обработки."""
        ...
