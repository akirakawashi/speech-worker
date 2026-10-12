"""Потоковое скачивание записи источника во временный файл."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Protocol

from src.application.dto.calls import JobErrorClass
from src.application.dto.sources import CallRef
from src.application.errors.external import RecordingNotAvailable
from src.application.pipeline.stages import StageError


class AudioStreamSource(Protocol):
    """Внутренний контракт источника, возвращающего поток записи."""

    async def open_audio(self, ref: CallRef) -> AsyncIterator[bytes]:
        """По ссылке источника вернуть поток байтов или ошибку доступности."""
        ...


class SourceRecordingFetcher:
    """Получить запись, различая отказ источника и отказ локального диска."""

    def __init__(self, source: AudioStreamSource) -> None:
        """Принять источник, который возвращает поток байтов записи."""
        self._source = source

    async def download(self, ref: CallRef, path: Path) -> int:
        """Скачать запись на диск.

        Чтение источника и запись на свой диск охвачены **раздельно**, и это не
        аккуратность ради аккуратности. Один блок на оба не работает: сетевые
        отказы — `ConnectionResetError`, `TimeoutError` — это подклассы
        `OSError`, то есть попадают в ветку «не смогли записать файл». Класс
        отказа решает, в какой системе ночью будут искать поломку, и назвать
        оборванную сеть источника проблемой нашего диска значит отправить
        человека не туда.
        """
        chunks = await self._open(self._source, ref)
        written = 0
        try:
            handle = path.open("wb")
        except OSError as exc:
            raise StageError(JobErrorClass.UNEXPECTED_ERROR, f"could not create {path}") from exc
        with handle:
            while True:
                chunk = await self._next_chunk(chunks, ref)
                if chunk is None:
                    break
                try:
                    await asyncio.to_thread(handle.write, chunk)
                except OSError as exc:
                    raise StageError(
                        JobErrorClass.UNEXPECTED_ERROR,
                        f"could not write the recording to {path}",
                    ) from exc
                written += len(chunk)
        if written == 0:
            # Пустой файл — не запись. Дальше по конвейеру он выглядел бы как
            # разговор, в котором никто не сказал ни слова.
            raise StageError(
                JobErrorClass.PROVIDER_ERROR,
                f"source returned an empty recording for call {ref.session_id}",
            )
        return written

    @staticmethod
    async def _open(source: AudioStreamSource, ref: CallRef) -> AsyncIterator[bytes]:
        """Открыть поток источника, переведя ожидаемые отказы в ошибки этапа."""
        try:
            stream = await source.open_audio(ref)
        except RecordingNotAvailable:
            raise
        except Exception as exc:
            raise StageError(
                JobErrorClass.PROVIDER_ERROR,
                f"source failed to open the recording of call {ref.session_id}",
            ) from exc
        return stream

    @staticmethod
    async def _next_chunk(chunks: AsyncIterator[bytes], ref: CallRef) -> bytes | None:
        """Следующий кусок потока. `None` — поток кончился.

        Отдельно, потому что `async for` не даёт охватить только чтение: тело
        цикла попадает в тот же блок, и запись на диск разделила бы класс
        отказа с сетью.
        """
        try:
            return await anext(chunks)
        except StopAsyncIteration:
            return None
        except RecordingNotAvailable:
            # Источник вправе обнаружить отсутствие записи и посреди потока:
            # REST отвечает заголовками раньше, чем телом. Наверх — как есть,
            # решение об окончательности принимает `work`.
            raise
        except Exception as exc:
            raise StageError(
                JobErrorClass.PROVIDER_ERROR,
                f"source failed while streaming the recording of call {ref.session_id}",
            ) from exc
