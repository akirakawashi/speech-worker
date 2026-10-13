"""Порты долговременного буфера и передачи результата распознавания."""

from typing import Final, Protocol

from src.application.dto.calls import JobErrorClass
from src.application.dto.transcription_delivery import (
    TranscriptionDeliveryRequest,
    TranscriptionReceipt,
)

TRANSCRIPTION_DELIVERY_KEY: Final[str] = "_transcription_delivery"


class TranscriptionDeliveryError(Exception):
    """Нормализованный отказ доставки без тела HTTP-ответа и исходного текста."""

    def __init__(self, error_class: JobErrorClass, reason: str, *, retryable: bool) -> None:
        """Сохранить класс и возможность повтора; reason задаёт адаптер, а не внешний ответ."""
        super().__init__(f"transcription_delivery_{reason}")
        self.error_class = error_class
        self.retryable = retryable


class TranscriptionDeliveryBuffer(Protocol):
    """Зафиксировать подготовленный DTO до сетевой передачи, не завершая задание."""

    async def save(self, command: TranscriptionDeliveryRequest) -> bool:
        """Сохранить DTO при актуальном аудио и живом захвате; иначе вернуть False."""
        ...


class TranscriptionDelivery(Protocol):
    """Передать один DTO и проверить подтверждение его конкретного захвата."""

    async def deliver(self, command: TranscriptionDeliveryRequest) -> TranscriptionReceipt:
        """Вернуть квитанцию после commit бэкенда либо TranscriptionDeliveryError."""
        ...
