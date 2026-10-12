"""Календарные дни колл-центра — полуоткрытым интервалом.

Отдельно от `report`: отчёт тянет openpyxl, а границы суток нужны и тем, у кого
его нет, — `reanalyze` запускается в образе `speech-worker`, где openpyxl не
ставится, и импорт через `report` ронял команду на первой строке (SP-326).
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


def period_bounds(day_from: date, day_to: date, timezone: str) -> tuple[datetime, datetime]:
    """Календарные дни колл-центра, оба включительно, — полуоткрытый интервал.

    Сутки считаются в поясе колл-центра, а не в UTC: «звонки за 14-е» для
    человека — его 14-е, и в UTC три вечерних часа уехали бы в соседний день.
    """
    zone = ZoneInfo(timezone)
    start = datetime.combine(day_from, time.min, tzinfo=zone)
    end = datetime.combine(day_to + timedelta(days=1), time.min, tzinfo=zone)
    return start, end


__all__ = ["period_bounds"]
