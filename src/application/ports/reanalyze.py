"""Операции поиска и повторной постановки звонков."""

from typing import Protocol
from uuid import UUID

from src.application.dto.calls import JobLane
from src.application.dto.reanalyze import ReanalyzeSelection


class ReanalyzeRepository(Protocol):
    """Короткое чтение кандидатов и отдельная транзакция на каждый звонок."""

    async def candidates(self, selection: ReanalyzeSelection) -> tuple[UUID, ...]:
        """Вернуть звонки без выбранного результата и без активных заданий."""
        ...

    async def enqueue(self, call_id: UUID, lane: JobLane) -> None:
        """Сбросить старый анализ и поставить новый; занятость и удаление — ошибки порта."""
        ...
