"""Удаление записей по сроку с подтверждением хранилища до изменения базы."""

import logging
import uuid
from collections.abc import Callable, Sequence

from src.application.dto.retention import ExpiredRecording, RetentionPolicy
from src.application.errors.external import StorageError
from src.application.errors.retention import PruneAborted
from src.application.ports.retention import (
    RecordingRemover,
    RetentionPersistence,
    RetentionRepository,
)
from src.domain.retention.policy import POLICY_BY_TABLE, SPEECH_DELETE_ORDER

logger = logging.getLogger(__name__)
_BUCKET_PROBE_KEY = "__retention-probe__/does-not-exist"
_PREVIEW_ROWS = 20


async def verify_bucket(storage: RecordingRemover) -> None:
    """Убедиться, что корзина та, прежде чем что-либо удалять.

    Проба ничего не удаляет: ключа `_BUCKET_PROBE_KEY` в корзине нет и быть не
    может. Зато `404` в ответ означает, что корзины нет вовсе, — и тогда каждое
    последующее «удаление» было бы отчётом ни о чём.
    """
    try:
        await storage.delete(_BUCKET_PROBE_KEY)
    except StorageError as exc:
        raise PruneAborted(f"корзина недоступна, удаление не начато: {exc}") from exc


async def delete_audio_batch(
    repository: RetentionPersistence,
    storage: RecordingRemover,
    rows: Sequence[ExpiredRecording],
) -> tuple[list[uuid.UUID], int, list[str]]:
    """Удалить объекты и пометить строки удалёнными.

    Возвращает: помеченные звонки, освобождённые байты, непорядок в данных.
    Ошибка хранилища не глотается — она поднимается наружу и останавливает
    прогон: продолжив, мы получили бы отчёт, в котором часть строк помечена
    удалённой без подтверждения.
    """
    deleted: list[uuid.UUID] = []
    freed = 0
    inconsistent: list[str] = []

    for row in rows:
        key = row.audio_object_key
        if not key:
            # Строка говорит «запись хранится», а ключа нет. Пометить её
            # удалённой значило бы записать в журнал удаление, которого не было.
            inconsistent.append(str(row.session_id))
            continue
        await storage.delete(str(key))
        deleted.append(row.id)
        freed += int(row.audio_bytes or 0)

    if deleted:
        await repository.mark_deleted([row for row in rows if row.id in deleted])
    return deleted, freed, inconsistent


async def prune_audio(
    repository: RetentionRepository,
    storage_factory: Callable[[], RecordingRemover],
    *,
    apply: bool,
    batch_size: int,
) -> tuple[int, int, list[str]]:
    """Аудио: посчитать просроченное, при `--apply` — удалить."""
    days = await repository.retention_days()
    stats = await repository.audio_stats(days=days)
    preview = await repository.expired_audio(days=days, limit=_PREVIEW_ROWS)

    expected = int(stats.calls)
    print(
        f"\nаудио: срок {days} дней, просрочено звонков {expected}, "
        f"объём {int(stats.bytes) / 1_048_576:.1f} МиБ"
    )
    if stats.oldest:
        print(f"       самый старый: {stats.oldest:%Y-%m-%d}")
    for row in preview:
        print(f"       {row.started_at:%Y-%m-%d}  {row.session_id}  {row.audio_object_key}")
    if expected > len(preview):
        print(f"       … и ещё {expected - len(preview)} — показаны первые {_PREVIEW_ROWS}")

    if not apply or not expected:
        return 0, 0, []

    storage = storage_factory()
    await verify_bucket(storage)

    removed = 0
    freed = 0
    inconsistent: list[str] = []
    while True:
        rows = await repository.expired_audio(days=days, limit=batch_size)
        if not rows:
            break
        done, batch_freed, bad = await delete_audio_batch(repository, storage, rows)
        removed += len(done)
        freed += batch_freed
        inconsistent.extend(bad)
        if not done:
            # Ни один объект не удалён, а строки есть — значит все они с пустым
            # ключом. Без выхода это был бы вечный цикл по тем же строкам.
            break
    return removed, freed, inconsistent


def speech_policies() -> list[RetentionPolicy]:
    """Политики строк в порядке удаления: дети раньше родителей."""
    return [POLICY_BY_TABLE[table] for table in SPEECH_DELETE_ORDER]


async def prune_rows(repository: RetentionRepository, *, apply: bool, batch_size: int) -> int:
    """Строки: задания и журнал.

    Считалка и пакетное удаление берутся у `src.infrastructure.retention.cleanup`
    — те же самые, а не похожие. Отличается только база и список таблиц:
    у Speech Analytics своя база, и общий прогон до этих таблиц не дотягивается.
    """
    total = 0
    print(f"""
{"таблица":<26}{"основание":<13}{"срок":>7}{"просрочено":>12}""")
    print("-" * 58)
    for policy in speech_policies():
        expired = await repository.count_expired(policy)
        total += expired
        print(f"{policy.table:<26}{policy.basis:<13}{str(policy.days) + 'д':>7}{expired:>12}")
        if apply and expired:
            removed = await repository.prune(policy, batch_size=batch_size)
            logger.info("удалено %s строк из %s", removed, policy.table)
    return total
