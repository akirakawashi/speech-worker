"""Сборка команд загрузки справочников и владение транзакцией."""

from pathlib import Path

from src.application.catalogues.service import seed_call_reasons, seed_classifier
from src.application.dto.catalogues import ClassifierSeedReport, ReasonsSeedReport
from src.infrastructure.data.catalogues import load_classifier, load_reasons
from src.infrastructure.persistence.catalogues import (
    PostgresClassifierCatalogue,
    PostgresReasonsCatalogue,
)
from src.infrastructure.persistence.session import get_speech_ms_sessionmaker
from src.setting.config import get_settings


async def run_classifier(path: Path, *, apply: bool) -> ClassifierSeedReport:
    """Загрузить и проверить файл, собрать сценарий и атомарно применить дерево."""
    paths = load_classifier(path)
    sessions = get_speech_ms_sessionmaker(get_settings())
    async with sessions() as session, session.begin():
        return await seed_classifier(PostgresClassifierCatalogue(session), paths, apply=apply)


async def run_reasons(path: Path, *, apply: bool) -> ReasonsSeedReport:
    """Загрузить и проверить файл, собрать сценарий и атомарно применить коды."""
    reasons = load_reasons(path)
    sessions = get_speech_ms_sessionmaker(get_settings())
    async with sessions() as session, session.begin():
        return await seed_call_reasons(PostgresReasonsCatalogue(session), reasons, apply=apply)


def catalogue_path(name: str) -> Path:
    """Вернуть путь к JSON-файлу справочника в установленном пакете."""
    from src.infrastructure.data import catalogues

    return Path(catalogues.__file__).with_name(name)
