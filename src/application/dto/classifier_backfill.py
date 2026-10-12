"""Сводка пополнения классификатора из операторских путей."""

from pydantic import BaseModel, ConfigDict


class ClassifierBackfillReport(BaseModel):
    """Итог пополнения дерева и связывания ранее принятых звонков."""

    model_config = ConfigDict(frozen=True)

    days: int
    calls: int
    paths: int
    created: int
    applied: bool
    #: Принятых звонков, связанных с узлом оператора.
    linked: int = 0

    def describe(self) -> str:
        """Вернуть читаемую сводку результата для вывода оператору."""
        created = "добавлено" if self.applied else "добавилось бы"
        linked = "связано" if self.applied else "связалось бы"
        return (
            f"дней: {self.days}; звонков с классификацией: {self.calls}; "
            f"различных путей: {self.paths}; узлов {created}: {self.created}; "
            f"принятых звонков {linked} с узлом оператора: {self.linked}"
        )
