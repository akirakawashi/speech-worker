"""Правила сохранения справочников без потери истории звонков."""

from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict


class DirectoryEmployee(BaseModel):
    """Сотрудник источника; логин связывает его с ранее принятыми звонками."""

    model_config = ConfigDict(frozen=True)
    login: str
    uuid: str | None = None
    display_name: str | None = None
    department: str | None = None
    removed: bool = False


class OperatorWrite(BaseModel):
    """Согласованные значения сотрудника для записи в локальный справочник."""

    model_config = ConfigDict(frozen=True)
    login: str
    ncc_uuid: str | None
    full_name: str
    department: str | None
    removed: bool


def merged_employees(
    active: Sequence[DirectoryEmployee], dismissed: Sequence[DirectoryEmployee]
) -> tuple[OperatorWrite, ...]:
    """Согласовать списки по логину: увольнение сильнее активности, пустое имя заменить логином."""
    by_login = {employee.login: employee for employee in active}
    by_login.update({employee.login: employee for employee in dismissed})
    dismissed_logins = {employee.login for employee in dismissed}
    return tuple(
        OperatorWrite(
            login=employee.login,
            ncc_uuid=employee.uuid,
            full_name=employee.display_name or employee.login,
            department=employee.department,
            removed=employee.removed or employee.login in dismissed_logins,
        )
        for employee in by_login.values()
    )
