"""Внешние операции с записями, необходимые этапам конвейера."""

from pathlib import Path
from typing import Protocol, runtime_checkable

from src.application.dto.sources import CallRef
from src.application.dto.transcription import TranscriptionResult


@runtime_checkable
class RecordingFetcher(Protocol):
    """Получить запись источника в локальный временный файл."""

    async def download(self, ref: CallRef, path: Path) -> int:
        """Скачать запись по DTO локатора и вернуть число записанных байтов."""
        ...


@runtime_checkable
class RecordingStorage(Protocol):
    """Хранилище записей с операциями на файлах вместо объектов HTTP."""

    async def fetch(self, key: str, destination: Path) -> None:
        """Сохранить объект по ключу в файл; отказ выражается StorageError."""
        ...

    async def put_file(self, key: str, source: Path) -> None:
        """Записать содержимое файла в объект хранилища."""
        ...

    async def exists(self, key: str) -> bool:
        """Проверить наличие объекта по ключу."""
        ...


@runtime_checkable
class RecordingTranscriber(Protocol):
    """Распознавание подготовленной записи с таймкодами."""

    async def transcribe(self, path: Path) -> TranscriptionResult:
        """Вернуть DTO текста и сегментов распознавания подготовленного файла."""
        ...
