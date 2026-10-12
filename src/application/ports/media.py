"""Подготовка записи и измерение её параметров."""

from pathlib import Path
from typing import Protocol, runtime_checkable

from src.application.dto.media import AudioProbe, ChannelEnvelope


@runtime_checkable
class MediaProcessor(Protocol):
    """Выполнить технические операции над записью и вернуть измерения."""

    async def downmix(self, source: Path, destination: Path) -> Path:
        """Подготовить моно-запись для распознавания, вернуть путь результата."""
        ...

    async def envelope(self, source: Path) -> ChannelEnvelope:
        """Измерить энергию каналов исходной записи и вернуть её DTO."""
        ...

    async def probe(self, source: Path) -> AudioProbe:
        """Вернуть измеренные параметры и длительность записи."""
        ...
