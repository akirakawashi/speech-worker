"""Временное рабочее пространство для обработки записи."""

from contextlib import AbstractContextManager
from pathlib import Path
from typing import Protocol, runtime_checkable


@runtime_checkable
class RecordingWorkspace(Protocol):
    """Создать рабочий каталог и гарантировать его очистку после этапа."""

    def directory(self, *, prefix: str) -> AbstractContextManager[Path]:
        """Вернуть контекст каталога с указанным префиксом; выход удаляет его файлы."""
        ...
