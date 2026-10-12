"""Передача записи между временным файлом и хранилищем."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import Protocol

import httpx


class ObjectStorageTransport(Protocol):
    """Технический потоковый транспорт; HTTP-ответ остаётся в infrastructure."""

    def open(
        self, key: str, *, byte_range: str | None = None
    ) -> AbstractAsyncContextManager[httpx.Response]:
        """Открыть поток объекта и закрыть HTTP-ответ при выходе из контекста."""
        ...

    async def put(self, key: str, chunks: AsyncIterator[bytes]) -> int:
        """Записать поток байтов и вернуть подтверждённый размер объекта."""
        ...

    async def exists(self, key: str) -> bool:
        """Проверить наличие объекта; отказ доступа выражается исключением."""
        ...


async def file_chunks(path: Path, size: int = 1 << 20) -> AsyncIterator[bytes]:
    """Читать файл порциями вне event loop и выдавать байты потоком."""
    with path.open("rb") as handle:
        while chunk := await asyncio.to_thread(handle.read, size):
            yield chunk


class PipelineStorage:
    """Адаптировать потоковый транспорт хранилища к файловому порту этапов."""

    def __init__(self, storage: ObjectStorageTransport) -> None:
        """Принять транспорт записи, чтения и проверки объектов."""
        self._storage = storage

    async def fetch(self, key: str, destination: Path) -> None:
        """Скачать объект в файл; HTTP-ответ остаётся внутри адаптера."""
        async with self._storage.open(key) as response:
            with destination.open("wb") as handle:
                async for chunk in response.aiter_bytes():
                    await asyncio.to_thread(handle.write, chunk)

    async def put_file(self, key: str, source: Path) -> None:
        """Записать файл в объект, читая содержимое порциями."""
        await self._storage.put(key, file_chunks(source))

    async def exists(self, key: str) -> bool:
        """Вернуть результат проверки существования объекта транспортом."""
        return bool(await self._storage.exists(key))
