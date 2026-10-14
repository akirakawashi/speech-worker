"""Скачиватели не перечисляют звонки, не требуют metadata-секретов и закрывают потоки."""

from datetime import date
from unittest.mock import Mock

import httpx
import pytest

from src.application.dto.sources import CallRef
from src.application.errors.external import RecordingNotAvailable
from src.di.sources import build_worker_source
from src.infrastructure.sources.ncc.source import LocalRecordingSource, NccRecordingSource
from src.infrastructure.sources.telphin.source import TelphinAudioSource, TelphinError
from src.setting.config import Settings


def ref(**changes):
    """Ссылка готовой технической команды; список звонков в воркере не запрашивается."""
    return CallRef(
        session_id="node_domain_1_nauss_0_1766092829_1138986",
        day=date(2026, 1, 14),
        audio_locator="recording",
        **changes,
    )


async def test_local_audio_reads_only_command_locator(tmp_path):
    """Локальная выгрузка читается по готовому пути без обхода каталога и классификации."""
    path = tmp_path / "recording.mp3"
    path.write_bytes(b"audio-content")
    source = LocalRecordingSource()
    recording = ref().model_copy(update={"audio_locator": str(path)})
    assert (
        b"".join([chunk async for chunk in await source.open_audio(recording)]) == b"audio-content"
    )
    path.unlink()
    with pytest.raises(RecordingNotAvailable):
        await source.open_audio(recording)
    assert not hasattr(source, "list_calls")


async def test_ncc_audio_uses_exact_session_without_listing_or_history():
    """REST скачивания не зависит от reporting-базы, порога и опорного номера истории."""

    async def chunks():
        yield b"audio"

    client = Mock(download=Mock(return_value=chunks()))
    source = NccRecordingSource(client)
    assert b"".join([chunk async for chunk in await source.open_audio(ref())]) == b"audio"
    client.download.assert_called_once_with(ref().session_id)
    assert not hasattr(source, "list_calls")


def test_worker_builds_reporting_audio_without_reporting_database_or_telphin_credentials():
    """На GPU-хосте нужны только ключи чтения NCC, а не секреты приёма метаданных."""
    settings = Settings(
        _env_file=None,
        speech_ncc_source_kind="reporting_view",
        speech_ncc_base_url="http://ncc.invalid",
        speech_ncc_username="audio-user",
        speech_ncc_api_token="audio-key",
    )
    source = build_worker_source(settings)
    assert set(source._sources) == {"ncc", "telphin"}
    assert not hasattr(source, "list_calls")
    assert isinstance(source._sources["ncc"], NccRecordingSource)


class Stream(httpx.AsyncByteStream):
    """Запомнить закрытие ответа при обычном и прерванном скачивании."""

    def __init__(self):
        self.closed = False

    async def __aiter__(self):
        yield b"a" * (1 << 17)

    async def aclose(self):
        self.closed = True


@pytest.mark.parametrize("status", [200, 404, 410, 401, 503])
async def test_telphin_closes_response_and_distinguishes_missing_from_failure(status):
    """Только 404/410 означают отсутствие; чужая ошибка не раскрывает URL записи."""
    stream = Stream()

    def response(request):
        assert request.method == "GET" and "authorization" not in request.headers
        return httpx.Response(status, stream=stream)

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        source = TelphinAudioSource(download=client)
        recording = ref(audio_url="http://recording.invalid/PRIVATE_AUDIO_TOKEN")
        if status == 200:
            assert (
                len(b"".join([chunk async for chunk in await source.open_audio(recording)]))
                == 1 << 17
            )
        else:
            expected = RecordingNotAvailable if status in (404, 410) else TelphinError
            with pytest.raises(expected) as failed:
                _ = [chunk async for chunk in await source.open_audio(recording)]
            assert "PRIVATE_AUDIO_TOKEN" not in str(failed.value)
        assert stream.closed
        assert not client.is_closed, "внешним клиентом владеет вызывающий"


async def test_partial_telphin_download_closes_response():
    """Отмена чтения после первой порции освобождает ответ и сетевое соединение."""
    stream = Stream()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: httpx.Response(200, stream=stream))
    ) as client:
        source = TelphinAudioSource(download=client)
        chunks = await source.open_audio(ref(audio_url="http://recording.invalid/PRIVATE_TOKEN"))
        assert await anext(chunks)
        await chunks.aclose()
    assert stream.closed
