"""Порт решений бэкенда для технического получения аудио."""

from typing import Protocol

from src.application.dto.ingest_control import (
    IngestClaim,
    IngestPlan,
    IngestResolution,
    ResolveIngest,
)


class IngestControl(Protocol):
    """Получить команду и передать наблюдения; правила допуска/повторов здесь не задаются."""

    async def prepare(self, claim: IngestClaim) -> IngestPlan:
        """Вернуть скачивание либо готовый итог; недоступность бэкенда выражается StageDeferred."""
        ...

    async def resolve(self, command: ResolveIngest) -> IngestResolution:
        """Получить готовое решение бэкенда для наблюдений той же попытки."""
        ...
