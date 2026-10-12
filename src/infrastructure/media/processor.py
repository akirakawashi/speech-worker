"""Реализация операций подготовки записи через ffmpeg и ffprobe."""

from pathlib import Path

from src.application.dto.media import AudioProbe, ChannelEnvelope
from src.infrastructure.media import audio


class FfmpegMediaProcessor:
    """Адаптер измерения и подготовки записи для конвейера."""

    async def downmix(self, source: Path, destination: Path) -> Path:
        """Создать WAV mono 16 кГц из исходника и вернуть его путь."""
        return await audio.downmix_to_mono_16k(source, destination)

    async def envelope(self, source: Path) -> ChannelEnvelope:
        """Измерить энергию каналов исходной записи одним проходом ffmpeg."""
        return await audio.channel_envelope(source)

    async def probe(self, source: Path) -> AudioProbe:
        """Прочитать метаданные записи через ffprobe и вернуть DTO измерений."""
        return await audio.probe_audio(source)
