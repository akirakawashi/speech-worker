"""Реплики, вердикты и правила вычисления оценки звонка."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class TranscriptLine(BaseModel):
    """Реплика с номером, ролью, временем начала и текстом."""

    model_config = ConfigDict(frozen=True)

    seq: int
    speaker: str
    start_ms: int
    #: Текст реплики.
    text: str


class StageVerdict(BaseModel):
    """Применимость и выполнение этапа скрипта с основаниями вывода."""

    model_config = ConfigDict(frozen=True)

    key: str
    applicable: bool
    #: Неприменимый этап засчитан: `passed` у него всегда `True`.
    passed: bool
    evidence: tuple[int, ...] = ()
    comment: str = ""

    def as_json(self) -> dict[str, Any]:
        """Вернуть вердикт как JSON-совместимые значения для сохранения."""
        return {
            "key": self.key,
            "applicable": self.applicable,
            "passed": self.passed,
            "evidence": list(self.evidence),
            "comment": self.comment,
        }


class ObjectionVerdict(BaseModel):
    """Результат обработки возражения и реплики, подтверждающие вывод."""

    model_config = ConfigDict(frozen=True)

    type: str
    handled: bool
    #: Реплика клиента с возражением; `None`, если модель её не назвала.
    seq: int | None = None
    evidence: tuple[int, ...] = ()
    comment: str = ""

    def as_json(self) -> dict[str, Any]:
        """Вернуть вердикт как JSON-совместимые значения для сохранения."""
        return {
            "type": self.type,
            "seq": self.seq,
            "handled": self.handled,
            "evidence": list(self.evidence),
            "comment": self.comment,
        }


class Evaluation(BaseModel):
    """Вердикты по этапам и правила вычисления итоговой оценки звонка."""

    model_config = ConfigDict(frozen=True)

    script_version: str
    model_name: str
    #: Оценивается ли звонок по этому скрипту вообще. `False` — входящее
    #: обращение, звонок другой кампании: баллов у такого звонка нет.
    script_applies: bool
    stages: tuple[StageVerdict, ...] = ()
    objections: tuple[ObjectionVerdict, ...] = ()
    outcome: str | None = None
    summary: str | None = None
    #: Только роли, определённые подготовкой оценки. Сохраняются с оценкой.
    speaker_roles: tuple[tuple[int, str], ...] = ()

    @property
    def stage_count(self) -> int | None:
        """Вернуть число этапов оценки; неприменимому скрипту соответствует None."""
        return len(self.stages) if self.script_applies else None

    @property
    def passed_count(self) -> int | None:
        """Вернуть число засчитанных этапов; неприменимому скрипту соответствует None."""
        if not self.script_applies:
            return None
        return sum(1 for stage in self.stages if stage.passed)

    @property
    def score(self) -> int | None:
        """Доля засчитанных этапов в процентах, целым числом."""
        if not self.script_applies or not self.stages:
            return None
        return round(100 * sum(1 for stage in self.stages if stage.passed) / len(self.stages))

    @property
    def all_passed(self) -> bool | None:
        """«Полностью корректный звонок» из макета: засчитаны все этапы."""
        if not self.script_applies:
            return None
        return all(stage.passed for stage in self.stages)
