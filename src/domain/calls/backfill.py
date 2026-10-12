"""Состояние окна истории и правило перехода к предыдущим суткам."""

from datetime import date, datetime, timedelta

from pydantic import BaseModel, ConfigDict


class BackfillNotStarted(RuntimeError):
    """Бэкфилл не запускали.

    Отдельный тип, а не пустое состояние: «нечего делать, потому что всё
    сделано» и «нечего делать, потому что не начинали» — разные ответы, и
    администратору важно, какой из них он видит.
    """


class BackfillState(BaseModel):
    """Где сейчас находится разбор истории."""

    model_config = ConfigDict(frozen=True)
    window_from: datetime
    window_until: datetime
    cursor: datetime
    paused: bool
    done: bool

    @property
    def next_day(self) -> date | None:
        """Дата, которую обработает следующая порция, или `None`.

        Курсор указывает на границу уже пройденного: обрабатывается **сутки
        перед ним**. Так первая порция берёт вчерашний день, а не сегодняшний,
        который ещё идёт и в котором звонки продолжают появляться.
        """
        if self.done or self.paused:
            return None
        candidate = self.cursor - timedelta(days=1)
        if candidate < self.window_from:
            return None
        return candidate.date()

    @property
    def days_left(self) -> int:
        """Вернуть число календарных дней, оставшихся в окне истории."""
        if self.done:
            return 0
        return max(0, (self.cursor - self.window_from).days)

    def describe(self) -> str:
        """Вернуть читаемую сводку результата для вывода оператору."""
        if self.done:
            return (
                "разбор истории завершён: окно "
                f"{self.window_from.date()}"
                "…"
                f"{self.window_until.date()}"
                " пройдено целиком"
            )
        state = "на паузе" if self.paused else "идёт"
        return (
            "разбор истории "
            f"{state}"
            ": курсор "
            f"{self.cursor.date()}"
            ", окно "
            f"{self.window_from.date()}"
            "…"
            f"{self.window_until.date()}"
            ", осталось суток "
            f"{self.days_left}"
        )
