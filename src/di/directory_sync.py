"""Сборка справочников NCC и сессий на одном event loop."""

import asyncio
import logging

from src.application.catalogues.directories import DirectorySyncService
from src.application.dto.directories import SyncReport
from src.infrastructure.persistence.directories import PostgresDirectoryRepository
from src.infrastructure.persistence.session import get_speech_ms_sessionmaker
from src.infrastructure.sources.ncc.directories import NccDirectorySource
from src.infrastructure.sources.ncc.rest import build_ncc_rest_client
from src.setting.config import get_settings


def run_directory_sync_command(*, employees: bool, projects: bool) -> list[SyncReport]:
    """Собрать порты, выполнить выбранные сценарии и вернуть их сводки после commit."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    async def run() -> list[SyncReport]:
        """Создать ресурсы и выполнить все операции внутри одного цикла событий."""
        settings = get_settings()
        sessions = get_speech_ms_sessionmaker(settings)
        source = NccDirectorySource(build_ncc_rest_client(settings))
        both = not (employees or projects)
        reports: list[SyncReport] = []
        if both or employees:
            async with sessions() as session, session.begin():
                reports.append(
                    await DirectorySyncService(
                        source, PostgresDirectoryRepository(session)
                    ).employees()
                )
        if both or projects:
            async with sessions() as session, session.begin():
                reports.append(
                    await DirectorySyncService(
                        source, PostgresDirectoryRepository(session)
                    ).projects()
                )
        return reports

    return asyncio.run(run())
