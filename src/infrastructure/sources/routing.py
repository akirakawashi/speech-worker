"""Выбор скачивателя по источнику звонка."""

from collections.abc import AsyncIterator, Mapping

from src.application.dto.sources import CallRef
from src.infrastructure.sources.fetcher import AudioStreamSource


class RoutingSource:
    """Записи звонков — у источника звонка.

    Звонок источника, которого воркер не знает, — поломка, а не «записи нет»:
    `RecordingNotAvailable` превратил бы его в звонок без записи навсегда.
    """

    name = "routing"

    def __init__(self, sources: Mapping[str, AudioStreamSource]) -> None:
        """Настроить RoutingSource из переданных параметров и зависимостей."""
        self._sources = dict(sources)

    async def open_audio(self, ref: CallRef) -> AsyncIterator[bytes]:
        """Открыть поток записи звонка; отсутствие записи выражается ошибкой порта."""
        source = self._sources.get(ref.source)
        if source is None:
            raise RuntimeError(f"the worker has no source for {ref.source!r} calls")
        return await source.open_audio(ref)
