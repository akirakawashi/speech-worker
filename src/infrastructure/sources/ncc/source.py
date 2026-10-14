"""Только чтение аудио Телесейлза; перечислением и отбором звонков владеет бэкенд."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from pathlib import Path
from typing import TYPE_CHECKING

from src.application.dto.sources import CallRef
from src.application.errors.external import RecordingNotAvailable as RecordingNotAvailable
from src.infrastructure.sources.fetcher import AudioStreamSource

if TYPE_CHECKING:
    from src.infrastructure.sources.ncc.rest import NccRestClient
    from src.setting.config import Settings

SESSION_ID_RE = re.compile(r"^node(_\d+)?_domain_\d+_nauss_\d+_\d+_\d+$")


class LocalRecordingSource:
    """Читать уже указанный бэкендом файл локальной выгрузки порциями."""

    async def open_audio(self, ref: CallRef) -> AsyncIterator[bytes]:
        """Вернуть поток файла по локатору; отсутствие выражается RecordingNotAvailable."""
        path = Path(ref.audio_locator)
        if not path.is_file():
            raise RecordingNotAvailable("local recording is missing")

        async def chunks() -> AsyncIterator[bytes]:
            """Закрыть дескриптор после чтения, ошибки или отмены скачивания."""
            with path.open("rb") as handle:
                while chunk := handle.read(1 << 20):
                    yield chunk

        return chunks()


class NccRecordingSource:
    """Получать запись по идентификатору сессии без reporting-базы и обхода истории."""

    def __init__(self, client: NccRestClient) -> None:
        """Принять технический REST-клиент источника."""
        self._client = client

    async def open_audio(self, ref: CallRef) -> AsyncIterator[bytes]:
        """По идентификатору бэкенда вернуть поток REST; ошибки доступности не переиначивать."""
        return self._client.download(ref.session_id)


def build_recording_source(settings: Settings) -> AudioStreamSource:
    """Собрать скачиватель; учётные данные reporting и параметры отбора ему не нужны."""
    kind = settings.speech_ncc_source_kind.strip().lower()
    if kind == "local_export":
        if not settings.speech_ncc_export_dir.strip():
            raise RuntimeError("local recording source requires SPEECH_NCC_EXPORT_DIR")
        return LocalRecordingSource()
    if kind in {"sequence", "reporting_view"}:
        from src.infrastructure.sources.ncc.rest import build_ncc_rest_client

        return NccRecordingSource(build_ncc_rest_client(settings))
    raise RuntimeError("recording source supports local_export, sequence, reporting_view")
