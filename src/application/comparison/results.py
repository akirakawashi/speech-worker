"""Подсчёт совпадений, отказов и времени сравниваемых моделей."""

import statistics
from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict, Field

from src.application.dto.comparison import Outcome, ReasonOutcome, ReasonRow, Row, Sampled
from src.domain.calls.classifier import CLASSIFIER_DEPTH, NodePaths, level_match, present_levels


def _percent(part: int, whole: int) -> str:
    """Форматировать долю в процентах; пустой знаменатель обозначить прочерком."""
    return f"{100 * part / whole:.1f} %" if whole else "—"


def _timing(seconds: Sequence[float]) -> str:
    """Вернуть медиану, p95 и максимум длительности прогонов в секундах."""
    if not seconds:
        return "—"
    ordered = sorted(seconds)
    p95 = ordered[min(len(ordered) - 1, round(0.95 * (len(ordered) - 1)))]
    return (
        f"медиана {statistics.median(ordered):.1f} с, p95 {p95:.1f} с, максимум {ordered[-1]:.1f} с"
    )


def reason_group(code: str) -> str:
    """Группа справочника по первой цифре: «K0» — связь, «K1» — деньги … «K9» — служебные."""
    return code[:2]


class Tally(BaseModel):
    """Итог одного варианта по выборке."""

    model_config = ConfigDict(frozen=False, arbitrary_types_allowed=True)

    name: str
    runs: int = 0
    errors: int = 0
    #: Путь ИИ ушёл мимо дерева уже на уровне 1.
    off_tree: int = 0
    compared: int = 0
    matched_levels: int = 0
    full_matches: int = 0
    level_hits: list[int] = Field(default_factory=lambda: [0] * CLASSIFIER_DEPTH)
    level_present: list[int] = Field(default_factory=lambda: [0] * CLASSIFIER_DEPTH)
    same_node: int = 0
    same_sentiment: int = 0
    same_reason: int = 0
    reason_compared: int = 0
    #: Ответ без кода причины у звонка, у которого код записан. Отдельно от
    #: «другой код»: боевое приложение разбора причину не спрашивает (S-34 —
    #: её пишет `fill_reasons`), и без этого счётчика сверка показала бы «та же
    #: причина 0 %», как будто модель ошиблась на каждом звонке (SP-337, 29.09).
    reason_missing: int = 0
    seconds: list[float] = Field(default_factory=list)

    def add_match(self, operator: Sequence[str], ai: Sequence[str]) -> None:
        # Путь мимо дерева — ноль совпавших уровней, а не «сравнивать не с чем»:
        # иначе отказы модели поднимали бы процент совпадения.
        """Учесть совпадения уровней двух путей классификации."""
        marks = level_match(operator, ai) or (False,) * CLASSIFIER_DEPTH
        present = present_levels(operator)
        self.compared += 1
        self.matched_levels += sum(marks)
        self.full_matches += int(all(marks))
        for index in range(CLASSIFIER_DEPTH):
            self.level_hits[index] += int(marks[index])
            self.level_present[index] += int(present[index])

    def add_fresh(self, outcome: Outcome, call: Sampled, operator: Sequence[str]) -> None:
        """Учесть свежий ответ модели относительно оператора и сохранённого анализа."""
        self.runs += 1
        self.seconds.append(outcome.seconds)
        work = outcome.work
        if work is None:
            self.errors += 1
            return
        if not work.path:
            self.off_tree += 1
        self.add_match(operator, work.path)
        self.same_node += int(work.node_id == call.ai_node_id)
        sentiment = work.sentiment.value if work.sentiment else None
        self.same_sentiment += int(sentiment == call.sentiment)
        if call.reason_code is not None:
            if work.call_reason_code is None:
                self.reason_missing += 1
            else:
                self.reason_compared += 1
                self.same_reason += int(work.call_reason_code == call.reason_code)

    def describe(self) -> list[str]:
        """Вернуть читаемую сводку результата для вывода оператору."""
        lines = [f"{self.name}:"]
        if self.runs:
            lines.append(
                f"  прогонов {self.runs}, отказов {self.errors}, мимо дерева {self.off_tree}"
            )
        lines.append(
            "  совпадение с оператором "
            f"{_percent(self.matched_levels, self.compared * CLASSIFIER_DEPTH)}"
            f" ({self.matched_levels} уровней из {self.compared * CLASSIFIER_DEPTH}),"
            f" полное у {self.full_matches} из {self.compared}"
        )
        levels = ", ".join(
            f"{index + 1} — {self.level_hits[index]}/{self.level_present[index]}"
            for index in range(CLASSIFIER_DEPTH)
        )
        lines.append(f"  по уровням (совпал/есть у оператора): {levels}")
        if self.runs:
            done = self.runs - self.errors
            lines.append(
                f"  с сохранённым разбором: тот же узел {_percent(self.same_node, done)},"
                f" та же тональность {_percent(self.same_sentiment, done)},"
                f" та же причина {_percent(self.same_reason, self.reason_compared)}"
                f" (ответов без кода {self.reason_missing})"
            )
            lines.append(f"  время: {_timing(self.seconds)}")
        return lines


class ReasonTally(BaseModel):
    """Итог одного приложения причин по выборке."""

    model_config = ConfigDict(frozen=False, arbitrary_types_allowed=True)

    name: str
    runs: int = 0
    failed: int = 0
    no_code: int = 0
    same_code: int = 0
    same_group: int = 0
    seconds: list[float] = Field(default_factory=list)

    def add(self, outcome: ReasonOutcome, stored: str) -> None:
        """Добавить результат в накопленные счётчики группы."""
        self.runs += 1
        self.seconds.append(outcome.seconds)
        if outcome.failed:
            self.failed += 1
        elif outcome.code is None:
            self.no_code += 1
        else:
            self.same_code += int(outcome.code == stored)
            self.same_group += int(reason_group(outcome.code) == reason_group(stored))

    def describe(self) -> list[str]:
        """Вернуть читаемую сводку результата для вывода оператору."""
        answered = self.runs - self.failed - self.no_code
        return [
            f"{self.name}:",
            f"  прогонов {self.runs}, сбой LLM {self.failed}, без кода {self.no_code}",
            f"  с сохранённым кодом: тот же код {_percent(self.same_code, answered)}"
            f" ({self.same_code} из {answered}),"
            f" та же группа {_percent(self.same_group, answered)}",
            f"  время: {_timing(self.seconds)}",
        ]


def summarize(rows: Sequence[Row], paths: NodePaths) -> list[str]:
    """Вернуть сводку совпадений, отказов и времени моделей анализа."""
    stored = Tally(name="сохранённый разбор")
    candidate = Tally(name="кандидат")
    control = Tally(name="контроль — боевое приложение сейчас")
    both = agree = 0
    for row in rows:
        operator = paths.labels(row.call.operator_node_id)
        stored.add_match(operator, paths.labels(row.call.ai_node_id))
        if row.candidate is not None:
            candidate.add_fresh(row.candidate, row.call, operator)
        if row.control is not None:
            control.add_fresh(row.control, row.call, operator)
            fresh, again = row.candidate, row.control
            if fresh is not None and fresh.work is not None and again.work is not None:
                both += 1
                agree += int(fresh.work.node_id == again.work.node_id)
    lines = [*stored.describe(), *candidate.describe()]
    if control.runs:
        lines += control.describe()
        lines.append(
            f"кандидат и контроль: тот же узел {_percent(agree, both)} ({agree} из {both})"
        )
    return lines


def summarize_reasons(rows: Sequence[ReasonRow]) -> list[str]:
    """Вернуть сводку совпадений кодов и групп причин сравниваемых моделей."""
    candidate = ReasonTally(name="кандидат")
    control = ReasonTally(name="контроль — боевое приложение причин сейчас")
    both = agree = 0
    for row in rows:
        candidate.add(row.candidate, row.stored)
        if row.control is not None:
            control.add(row.control, row.stored)
            if row.candidate.code and row.control.code:
                both += 1
                agree += int(row.candidate.code == row.control.code)
    lines = candidate.describe()
    if control.runs:
        lines += control.describe()
        lines.append(f"кандидат и контроль: тот же код {_percent(agree, both)} ({agree} из {both})")
    return lines
