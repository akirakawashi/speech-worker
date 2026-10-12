"""DTO источника и результатов синхронизации справочников."""

from pydantic import BaseModel, ConfigDict

from src.domain.calls.directories import (
    DirectoryEmployee as DirectoryEmployee,
    OperatorWrite as OperatorWrite,
)


class DirectoryProject(BaseModel):
    """Поля проекта, которыми владеет источник; ручная разметка в этот DTO не входит."""

    model_config = ConfigDict(frozen=True)
    uuid: str
    title: str
    data_channel: str | None = None
    direction: str | None = None


class SyncReport(BaseModel):
    """Что сделала синхронизация.

    `failed` отдельно от `unchanged`: справочник, у которого половина строк не
    обновилась из-за отказов, не то же самое, что справочник, в котором ничего
    не менялось, — а по одному числу «обработано» их не различить.
    """

    model_config = ConfigDict(frozen=True)

    scope: str
    seen: int = 0
    created: int = 0
    updated: int = 0
    unchanged: int = 0
    failed: int = 0
    note: str | None = None

    @property
    def changed(self) -> int:
        """Вернуть число созданных и обновлённых записей справочника."""
        return self.created + self.updated

    def describe(self) -> str:
        """Вернуть читаемую сводку результата для вывода оператору."""
        parts = [
            f"{self.scope}: получено {self.seen}",
            f"заведено {self.created}",
            f"обновлено {self.updated}",
            f"без изменений {self.unchanged}",
        ]
        if self.failed:
            parts.append(f"с отказом {self.failed}")
        line = ", ".join(parts)
        return f"{line}. {self.note}" if self.note else line
