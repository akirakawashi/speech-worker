"""Формирование ключа записи с переводом ошибок доменного формата."""

from datetime import UTC, datetime

from src.application.errors.external import StorageError
from src.domain.calls.recording import (
    InvalidRecordingKey,
    object_key as recording_key,
)


def object_key(
    session_id: str, day: datetime | None = None, *, date_path: str | None = None
) -> str:
    """Вернуть ключ записи по идентификатору и дате, сохранив контракт ошибок.

    Args:
        session_id: Идентификатор сессии звонка.
        day: Момент звонка; None означает текущий UTC-момент.
        date_path: Уже известная дата в формате ГГГГ/ММ/ДД.

    Raises:
        StorageError: Если сегмент ключа нарушает формат записи.
    """
    try:
        return recording_key(session_id, day or datetime.now(UTC), date_path=date_path)
    except InvalidRecordingKey as exc:
        raise StorageError(str(exc), reason=exc.reason) from exc
