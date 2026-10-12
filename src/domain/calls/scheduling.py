"""Правила дневной доли обработки и начала ночного окна."""

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict

DEFERRED_PRIORITY_STEP = 10
MAX_RUN_PRIORITY = 80


class DayShare(BaseModel):
    """Дневная доля приёма: какая часть новых звонков идёт в обработку сразу.

    Ночь задана часами в поясе колл-центра, от `night_from` включительно до
    `night_until`; через полночь — обычный случай (19 → 7).
    """

    model_config = ConfigDict(frozen=True)
    share: float
    night_from: int
    night_until: int
    timezone: str = "Europe/Moscow"

    def night_start(self, now: datetime) -> datetime | None:
        """Начало ближайшей ночи. `None` — ночь уже идёт: откладывать некуда."""
        local = now.astimezone(ZoneInfo(self.timezone))
        if self.night_from > self.night_until:
            at_night = local.hour >= self.night_from or local.hour < self.night_until
            day = local.date()
        else:
            at_night = self.night_from <= local.hour < self.night_until
            day = local.date() + timedelta(days=local.hour >= self.night_until)
        if at_night:
            return None
        return datetime.combine(day, time(self.night_from), tzinfo=local.tzinfo)
