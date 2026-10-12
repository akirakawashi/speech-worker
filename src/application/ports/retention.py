"""Операции удаления подтверждённых записей и старых технических строк."""

from collections.abc import Sequence
from typing import Protocol

from src.application.dto.retention import AudioRetentionStats, ExpiredRecording, RetentionPolicy


class RecordingRemover(Protocol):
    """Хранилище с подтверждённым удалением и явной ошибкой корзины."""

    async def delete(self, key: str) -> bool:
        """Удалить ключ; отсутствие корзины или отказ — StorageError."""
        ...


class RetentionPersistence(Protocol):
    """Атомарно пометить только подтверждённые удаления и записать аудит."""

    async def mark_deleted(self, rows: Sequence[ExpiredRecording]) -> None:
        """Сохранить статусы и аудит в одной транзакции после ответа хранилища."""
        ...


class RetentionRepository(Protocol):
    """Операции хранения с короткими транзакциями для сценария очистки."""

    async def retention_days(self) -> int:
        """Прочитать положительный срок хранения записей в днях."""
        ...

    async def audio_stats(self, *, days: int) -> AudioRetentionStats:
        """Вернуть сводку просроченных записей."""
        ...

    async def expired_audio(self, *, days: int, limit: int) -> tuple[ExpiredRecording, ...]:
        """Вернуть старейшие просроченные записи ограниченной порцией."""
        ...

    async def mark_deleted(self, rows: Sequence[ExpiredRecording]) -> None:
        """В отдельной транзакции пометить удаления и сохранить аудит."""
        ...

    async def count_expired(self, policy: RetentionPolicy) -> int:
        """Посчитать завершённые технические строки за пределом срока."""
        ...

    async def prune(self, policy: RetentionPolicy, *, batch_size: int) -> int:
        """Удалить просроченные строки порциями и вернуть фактическое число."""
        ...
