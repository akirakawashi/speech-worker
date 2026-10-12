"""Правила определения говорящего по измеренной энергии каналов."""

from __future__ import annotations

import math
from typing import Final

from pydantic import BaseModel, ConfigDict

DOMINANCE_MARGIN_DB: Final[float] = 3.0


class ChannelEnvelope(BaseModel):
    """Уровень каждого канала по окнам постоянной длины.

    Уровни — в децибелах относительно полной шкалы (dBFS), как их даёт
    `astats`. Тишина приходит как `-inf`, и это правильное значение, а не
    отсутствующее: в окне действительно нет сигнала.
    """

    model_config = ConfigDict(frozen=True)

    window_s: float
    #: По списку уровней на канал. Внешний индекс — номер канала с нуля.
    levels: tuple[tuple[float, ...], ...]

    @property
    def channels(self) -> int:
        """Вернуть число каналов измеренной огибающей записи."""
        return len(self.levels)

    @property
    def duration_s(self) -> float:
        """Вернуть измеренную длительность огибающей записи в секундах."""
        return max((len(row) for row in self.levels), default=0) * self.window_s

    def window_range(self, start_s: float, end_s: float) -> tuple[int, int]:
        """Полуинтервал окон, покрывающих отрезок времени.

        Конец округляется вверх, а начало вниз: реплика, начавшаяся посреди
        окна, всё равно в нём звучала.
        """
        first = max(0, int(start_s // self.window_s))
        last = max(first + 1, math.ceil(end_s / self.window_s))
        return first, last

    def mean_power(self, channel: int, start_s: float, end_s: float) -> float:
        """Средняя мощность канала на отрезке, в линейной шкале.

        Усредняется мощность, а не децибелы. Децибелы логарифмичны, и их
        среднее — среднее геометрическое: одно тихое окно посреди речи утянуло
        бы результат сильнее, чем оно того стоит, а окно тишины (`-inf`)
        обратило бы всё среднее в минус бесконечность.
        """
        if not 0 <= channel < self.channels:
            raise IndexError(f"channel {channel} is out of range for {self.channels}")
        first, last = self.window_range(start_s, end_s)
        row = self.levels[channel][first:last]
        if not row:
            return 0.0
        return sum(10 ** (level / 10) for level in row) / len(row)

    def dominant_channel(
        self, start_s: float, end_s: float, *, margin_db: float = DOMINANCE_MARGIN_DB
    ) -> int | None:
        """Какой канал звучит на отрезке. `None` — уверенно сказать нельзя.

        `None` возвращается, когда разница меньше порога: говорят одновременно,
        оба молчат, или запись оказалась не стерео. Это честный ответ, и
        вызывающий обязан перейти к запасному методу, а не подставить нулевой
        канал по умолчанию — иначе весь разговор достанется оператору.
        """
        if self.channels < 2:
            return None
        powers = [self.mean_power(ch, start_s, end_s) for ch in range(self.channels)]
        best = max(range(len(powers)), key=lambda ch: powers[ch])
        rest = max(power for ch, power in enumerate(powers) if ch != best)
        if powers[best] <= 0.0:
            return None
        if rest <= 0.0:
            return best
        if 10 * math.log10(powers[best] / rest) < margin_db:
            return None
        return best
