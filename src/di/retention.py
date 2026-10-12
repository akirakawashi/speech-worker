"""Сборка очистки и владение блокировкой полного прогона."""

from src.application.errors.external import StorageError
from src.application.errors.retention import PruneAborted
from src.application.retention.service import prune_audio, prune_rows
from src.infrastructure.persistence.retention import PostgresRetentionRepository
from src.infrastructure.persistence.session import get_speech_ms_engine, get_speech_ms_sessionmaker
from src.infrastructure.retention.lock import _singleton as singleton
from src.infrastructure.storage.recordings import build_storage
from src.setting.config import get_settings


async def main_async(*, apply: bool, batch_size: int) -> int:
    """Собрать очистку, удержать блокировку прогона и вывести итог."""
    settings = get_settings()
    engine = get_speech_ms_engine(settings)
    sessions = get_speech_ms_sessionmaker(settings)

    async with singleton(engine) as acquired:
        if not acquired:
            print("не начат: предыдущий прогон удаления ещё идёт")
            return 0

        try:
            removed, freed, inconsistent = await prune_audio(
                PostgresRetentionRepository(sessions),
                lambda: build_storage(settings),
                apply=apply,
                batch_size=batch_size,
            )
        except PruneAborted as exc:
            print(f"""
ОСТАНОВЛЕНО: {exc}""")
            return 2
        except StorageError as exc:
            print(f"""
ОСТАНОВЛЕНО на удалении объекта: {exc}""")
            print(
                "часть объектов уже удалена и помечена; повторный прогон продолжит с места отказа"
            )
            return 2

        rows = await prune_rows(
            PostgresRetentionRepository(sessions), apply=apply, batch_size=batch_size
        )

    if apply:
        print(f"""
удалено записей разговоров: {removed}, освобождено {freed / 1_048_576:.1f} МиБ""")
    else:
        print(f"""
строк за сроком хранения: {rows}""")
        print("режим проверки; ничего не удалено. Для удаления — --apply.")
    if inconsistent:
        print(
            f"\nне тронуто из-за расхождения в данных: {len(inconsistent)} "
            f"(состояние «хранится» при пустом ключе): {', '.join(inconsistent[:5])}"
        )
        return 1
    return 0
