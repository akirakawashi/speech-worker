"""DTO загрузки справочников и изменений их записей."""

from uuid import UUID

from pydantic import BaseModel, ConfigDict

from src.domain.calls.reasons import CallReason as CallReason


class ClassifierSeedReport(BaseModel):
    """Число путей и созданных, обновлённых или неизменных узлов."""

    model_config = ConfigDict(frozen=True)

    paths: int
    nodes: int
    created: int
    updated: int

    def describe(self) -> str:
        """Вернуть читаемую сводку результата для вывода оператору."""
        unchanged = self.nodes - self.created - self.updated
        return (
            f"путей в файле: {self.paths}; узлов: {self.nodes}; новых: {self.created}; "
            f"обновлено: {self.updated}; без изменений: {unchanged}"
        )


class ReasonsSeedReport(BaseModel):
    """Итог загрузки причин, включая выключенные коды."""

    model_config = ConfigDict(frozen=True)

    codes: int
    created: int
    updated: int
    disabled: tuple[str, ...]

    def describe(self) -> str:
        """Вернуть читаемую сводку результата для вывода оператору."""
        unchanged = self.codes - self.created - self.updated
        line = (
            f"кодов в файле: {self.codes}; новых: {self.created}; обновлено: {self.updated}; "
            f"без изменений: {unchanged}; выключено: {len(self.disabled)}"
        )
        return line + (f" ({', '.join(self.disabled)})" if self.disabled else "")


class ClassifierNode(BaseModel):
    """Снимок узла для сравнения с файлом владельца."""

    model_config = ConfigDict(frozen=True)
    id: UUID
    parent_id: UUID | None
    label: str
    key: str
    source: str
    sort_order: int
    is_active: bool


class ClassifierNodeWrite(BaseModel):
    """Данные создания или обновления одного узла."""

    model_config = ConfigDict(frozen=True)
    parent_id: UUID | None
    level: int
    label: str
    key: str
    source: str
    sort_order: int


class ReasonRecord(BaseModel):
    """Снимок записи справочника причин перед загрузкой."""

    model_config = ConfigDict(frozen=True)
    code: str
    label: str
    hint: str
    sort_order: int
    is_active: bool


class ReasonWrite(BaseModel):
    """Изменение подписи, подсказки и порядка кода причины."""

    model_config = ConfigDict(frozen=True)
    code: str
    label: str
    hint: str
    sort_order: int
