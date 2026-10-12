"""Порт получения подготовленного бэкендом запроса без деталей HTTP."""

from typing import Protocol, runtime_checkable

from src.application.dto.model_execution import ModelPreparationRequest, PreparedModelRequest


@runtime_checkable
class ModelRequestPreparer(Protocol):
    """Получить сообщения и параметры по входному DTO задания."""

    async def prepare(self, request: ModelPreparationRequest) -> PreparedModelRequest:
        """Передать вход бэкенду и вернуть его готовый запрос к модели."""
        ...
