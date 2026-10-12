"""Оценка звонка по скрипту: 1 или 0 за каждый этап, как у контролёра.

Модель получает скрипт и расшифровку с ролями и отвечает объектом
JSON: применим ли скрипт к звонку, вердикт по каждому этапу с номерами реплик,
на которых он основан, возражения клиента, итог звонка. Балл считается здесь,
а не моделью: арифметику ей доверять незачем.

Неизвестные роли определяются до оценки; готовые роли не переписываются.
Затем отдельный запрос сверяет черновик оценки с разговором. Невалидный
результат этой проверки оставляет звонок без оценки, а не с нулевым баллом.

В модель уходит текст расшифровки как есть — как и у анализа. Откуда берутся
реплики — см. `src/presentation/cli/evaluate.py`.

**Ссылка на реплику проверяется.** Номер, которого нет в расшифровке, из
ответа выбрасывается: цитата, ведущая в пустоту, хуже отсутствия цитаты —
контролёр откроет звонок и не найдёт, на чём основан вывод.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from typing import Any, Final, Protocol

# Разбор ответа тот же, что у анализа: рассуждение `<think>`, обёртка ```json и
# приписка после объекта встречены там на настоящих прогонах той же модели.
from src.application.model_output import _json_object
from src.application.quality.roles import RoleAttributionError, prepare_roles
from src.domain.calls.contracts import Speaker
from src.domain.quality.evaluation import (
    Evaluation as Evaluation,
    ObjectionVerdict as ObjectionVerdict,
    StageVerdict as StageVerdict,
    TranscriptLine as TranscriptLine,
)
from src.domain.quality.script import (
    CURRENT_SCRIPT,
    OTHER_OBJECTION,
    UNKNOWN_OUTCOME,
    CallScript,
)

logger = logging.getLogger(__name__)

#: Сколько раз модель получает шанс ответить по схеме. Три — по измерению
#: анализа на корпусе из шестидесяти одного звонка: «дважды подряд не вышло»
#: у этой модели — ещё не «не выйдет никогда».
MAX_OUTPUT_ATTEMPTS: Final[int] = 3

#: Пояснение к этапу — подсказка контролёру, а не абзац.
COMMENT_LIMIT: Final[int] = 300
SUMMARY_LIMIT: Final[int] = 600

#: Сколько реплик хранится доказательством одного вердикта. Больше — это уже
#: не ссылка на место в разговоре, а пересказ разговора.
EVIDENCE_LIMIT: Final[int] = 10

#: Возражений в одном звонке больше этого не бывает; длинный список — признак
#: того, что модель перечисляет каждую реплику клиента.
OBJECTIONS_LIMIT: Final[int] = 20

_SPEAKER_LABELS: Final[dict[str, str]] = {
    Speaker.OPERATOR.value: "Оператор",
    Speaker.CLIENT.value: "Клиент",
    Speaker.UNKNOWN.value: "Не определён",
}


class QualityModel(Protocol):
    """Что оценке нужно от клиента модели. Протокол — ради подмены в тестах."""

    @property
    def model(self) -> str:
        """Вернуть имя модели, которое сохраняется вместе с оценкой звонка."""
        ...

    async def complete(self, messages: Sequence[Mapping[str, str]]) -> str:
        """По сообщениям промпта получить текст ответа; при отказе поднять QualityModelError."""
        ...


class QualityError(Exception):
    """Звонок не оценён. Текст ответа модели в причину не попадает (BR-13)."""

    def __init__(self, reason: str, *, code: str = "invalid_evaluation") -> None:
        """Настроить QualityError из переданных параметров и зависимостей."""
        super().__init__(reason)
        self.reason = reason
        self.code = code


def _clock(start_ms: int) -> str:
    """Преобразовать время реплики в читаемую подпись для разговора."""
    seconds = max(0, start_ms) // 1000
    return f"{seconds // 60:02d}:{seconds % 60:02d}"


def build_messages(script: CallScript, lines: Sequence[TranscriptLine]) -> list[dict[str, str]]:
    """Системная инструкция и сообщение со скриптом и расшифровкой."""
    system = (
        "Ты — контролёр качества колл-центра. Ты слушаешь звонок оператора и ставишь по "
        "каждому этапу скрипта 1 или 0, как это делает контролёр. Правила:\n"
        "1. Сначала реши, оценивается ли звонок по этому скрипту. Если нет — "
        f'"script_applies": false, списки пустые, "outcome": "{UNKNOWN_OUTCOME}".\n'
        "2. Засчитывай смысл, а не дословность: читать скрипт слово в слово оператор не "
        "обязан.\n"
        '3. Этап, условие которого в звонке не наступило, — "applicable": false.\n'
        '4. "passed": true — только если оператор выполнил требование этапа. Сомневаешься — '
        'false, и объясни почему в "comment".\n'
        '5. "evidence" — номера реплик в квадратных скобках, на которых основан вывод. '
        "Номеров, которых нет в расшифровке, не пиши.\n"
        "6. Расшифровка автоматическая: в ней бывают ошибки распознавания, а реплика с ролью "
        "«Не определён» может принадлежать любой стороне. За очевидную ошибку распознавания "
        "оценку не снижай.\n"
        "7. Проверяй все требования этапа по всему разговору. Если этап выполнен частично, "
        "в комментарии назови только недостающие требования. Не отрицай действия, которые "
        "есть в репликах: представление компании, описание тарифа, проверку адреса и другие.\n"
        "8. Для каждого выполненного применимого этапа укажи в evidence реплики оператора. "
        "Для невыполненного этапа укажи связанные реплики, если они есть; отсутствие "
        "действия объясни в comment. Сверь каждый комментарий с текстом перед ответом.\n"
        "9. Ответь только объектом JSON, без пояснений до и после."
    )

    stages = "\n".join(
        f"- {stage.key} — {stage.label}. Требование: {stage.requirement} "
        f"Применим: {stage.applies_when or 'в любом звонке по скрипту.'}"
        for stage in script.stages
    )
    objections = "\n".join(
        f"- {item.key} — {item.label}. Ответ по скрипту: {item.expected_answer}"
        for item in script.objections
    )
    outcomes = "; ".join(f"{key} — {label}" for key, label in script.outcomes)
    transcript = "\n".join(
        f"[{line.seq}] {_clock(line.start_ms)} "
        f"{_SPEAKER_LABELS.get(line.speaker, _SPEAKER_LABELS[Speaker.UNKNOWN.value])}: "
        f"{line.text}"
        for line in lines
    )
    stage_keys = ", ".join(stage.key for stage in script.stages)
    user = (
        f"# Скрипт: {script.title} (редакция {script.version})\n\n"
        f"## Какие звонки оцениваются\n{script.applies_to}\n\n"
        f"## Этапы\n{stages}\n\n"
        f"## Возражения и вопросы клиента\n{objections}\n\n"
        f"## Итоги звонка\n{outcomes}\n\n"
        f"## Тарифы\n{script.tariffs}\n\n"
        f"# Расшифровка\n{transcript}\n\n"
        "# Формат ответа\n"
        "{\n"
        '  "script_applies": true,\n'
        '  "stages": [{"key": "<ключ этапа>", "applicable": true, "passed": true, '
        '"evidence": [<номер реплики>], "comment": "<одно предложение>"}],\n'
        '  "objections": [{"type": "<ключ возражения>", "seq": <номер реплики клиента>, '
        '"handled": true, "evidence": [<номер реплики оператора>], "comment": "<одно '
        'предложение>"}],\n'
        '  "outcome": "<ключ итога>",\n'
        '  "summary": "<два-три предложения: что оператор сделал хорошо и что упустил>"\n'
        "}\n"
        f'В "stages" — ровно один элемент на каждый этап: {stage_keys}. '
        f'В "objections" — каждое возражение или вопрос клиента; чего нет в списке — '
        f'"type": "{OTHER_OBJECTION}". Возражений не было — пустой список.'
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def describe_raw_problem(raw: str) -> str:
    """Почему из ответа не достать объект. Только для журнала: текста ответа здесь нет."""
    lowered = raw.lower()
    if "<think" in lowered and "</think" not in lowered:
        # Оборванное рассуждение — потолок токенов, а не плохой промпт.
        return f"the model ran out of output tokens while reasoning ({len(raw)} characters)"
    if "{" not in raw:
        return "the model answered without a JSON object"
    return "the JSON object in the answer does not parse"


def _text(value: Any, limit: int) -> str:
    """Прочитать текстовое значение и нормализовать отсутствие."""
    return value.strip()[:limit] if isinstance(value, str) else ""


def _evidence(value: Any, seqs: frozenset[int]) -> tuple[int, ...]:
    """Прочитать номера реплик, подтверждающих вердикт модели."""
    if not isinstance(value, list):
        return ()
    found: list[int] = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, int) or item not in seqs:
            continue
        if item not in found:
            found.append(item)
    return tuple(sorted(found)[:EVIDENCE_LIMIT])


def _flag(item: Mapping[str, Any], name: str, where: str) -> bool:
    """Прочитать флаг ответа модели без принятия произвольной строки за True."""
    value = item.get(name)
    if not isinstance(value, bool):
        raise ValueError(f"{where}: {name} is not a boolean")
    return value


def _stages(value: Any, script: CallScript, seqs: frozenset[int]) -> tuple[StageVerdict, ...]:
    """Проверить ответы по этапам и привести их к порядку скрипта."""
    if not isinstance(value, list):
        raise ValueError("stages is not a list")
    by_key: dict[str, StageVerdict] = {}
    for item in value:
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        if not isinstance(key, str) or script.stage(key) is None or key in by_key:
            # Этап, которого нет в скрипте, — мнение модели сверх формы, а
            # повтор этапа — её же черновик. Ни то ни другое балл не меняет.
            continue
        applicable = _flag(item, "applicable", f"stage {key}")
        passed = _flag(item, "passed", f"stage {key}") if applicable else True
        by_key[key] = StageVerdict(
            key=key,
            applicable=applicable,
            passed=passed,
            evidence=_evidence(item.get("evidence"), seqs),
            comment=_text(item.get("comment"), COMMENT_LIMIT),
        )
    missing = [stage.key for stage in script.stages if stage.key not in by_key]
    if missing:
        # Пропущенный этап не засчитывается и не проваливается: умолчание здесь
        # сдвинуло бы балл, и никто бы не узнал, что этапа модель не видела.
        raise ValueError(f"stages are missing: {', '.join(missing)}")
    return tuple(by_key[stage.key] for stage in script.stages)


def _objections(
    value: Any, script: CallScript, seqs: frozenset[int]
) -> tuple[ObjectionVerdict, ...]:
    """Проверить возражения модели относительно справочника скрипта."""
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError("objections is not a list")
    known = {item.key for item in script.objections}
    verdicts: list[ObjectionVerdict] = []
    for item in value[:OBJECTIONS_LIMIT]:
        if not isinstance(item, dict):
            continue
        kind = item.get("type")
        kind = kind if isinstance(kind, str) and kind in known else OTHER_OBJECTION
        seq = item.get("seq")
        verdicts.append(
            ObjectionVerdict(
                type=kind,
                handled=_flag(item, "handled", f"objection {kind}"),
                seq=seq
                if isinstance(seq, int) and not isinstance(seq, bool) and seq in seqs
                else None,
                evidence=_evidence(item.get("evidence"), seqs),
                comment=_text(item.get("comment"), COMMENT_LIMIT),
            )
        )
    return tuple(verdicts)


def parse_evaluation(
    raw: str, *, script: CallScript, seqs: frozenset[int], model_name: str
) -> Evaluation:
    """Ответ модели — в оценку. `ValueError`, если ответ не годится.

    Умолчаний нет намеренно, как у анализа: подставив «засчитано» вместо
    непонятного ответа, мы получили бы балл, в котором настоящие оценки
    смешаны с тем, что модель не смогла сказать.
    """
    data = _json_object(raw)
    if data is None:
        raise ValueError(describe_raw_problem(raw))
    applies = data.get("script_applies")
    if not isinstance(applies, bool):
        raise ValueError("script_applies is not a boolean")
    if not applies:
        return Evaluation(
            script_version=script.version,
            model_name=model_name,
            script_applies=False,
            summary=_text(data.get("summary"), SUMMARY_LIMIT) or None,
        )

    stages = _stages(data.get("stages"), script, seqs)
    objections = _objections(data.get("objections"), script, seqs)
    outcome = data.get("outcome")
    if not isinstance(outcome, str) or outcome not in dict(script.outcomes):
        raise ValueError("outcome is not one of the script's outcomes")
    return Evaluation(
        script_version=script.version,
        model_name=model_name,
        script_applies=True,
        stages=stages,
        objections=objections,
        outcome=outcome,
        summary=_text(data.get("summary"), SUMMARY_LIMIT) or None,
    )


def _validate_evidence(evaluation: Evaluation, lines: Sequence[TranscriptLine]) -> None:
    """Проверить, что основания вердикта ссылаются на существующие реплики."""
    roles = {line.seq: line.speaker for line in lines}
    # Реплика без роли может принадлежать любой стороне — так сказано и модели
    # (правило 6). Доказательством она годится; реплика другой стороны — нет.
    by_operator = {Speaker.OPERATOR.value, Speaker.UNKNOWN.value}
    by_client = {Speaker.CLIENT.value, Speaker.UNKNOWN.value}
    for stage in evaluation.stages:
        if stage.applicable and stage.passed:
            if not any(roles.get(seq) in by_operator for seq in stage.evidence):
                raise ValueError(f"passed stage {stage.key} has no operator evidence")
    for objection in evaluation.objections:
        if objection.seq is None or roles.get(objection.seq) not in by_client:
            raise ValueError("an objection has no client line")
        if objection.handled:
            if not any(roles.get(seq) in by_operator for seq in objection.evidence):
                raise ValueError("a handled objection has no operator evidence")


async def _review(
    client: QualityModel,
    evaluation: Evaluation,
    messages: Sequence[Mapping[str, str]],
    lines: Sequence[TranscriptLine],
    script: CallScript,
    seqs: frozenset[int],
) -> Evaluation:
    """Проверить полноту и согласованность вердикта модели со скриптом и репликами."""
    draft = {
        "script_applies": evaluation.script_applies,
        "stages": [stage.as_json() for stage in evaluation.stages],
        "objections": [item.as_json() for item in evaluation.objections],
        "outcome": evaluation.outcome or UNKNOWN_OUTCOME,
        "summary": evaluation.summary,
    }
    review_messages = [dict(message) for message in messages] + [
        {"role": "assistant", "content": json.dumps(draft, ensure_ascii=False)},
        {
            "role": "user",
            "content": (
                "Проверь готовую оценку по расшифровке выше, реплика за репликой. "
                "Это проверка фактов: исправь ошибки в применимости скрипта, этапах, "
                "комментариях и итогах. Особенно проверь отрицания действий оператора. "
                "Если оператор представился и назвал компанию, нельзя писать, что он "
                "этого не сделал. Если он сказал «подключение бесплатное» или сообщил о "
                "бесплатном роутере, нельзя включать эти действия в список упущений. "
                "У частично выполненного этапа перечисли только действительно "
                "недостающие требования. Смысл важнее дословности; за очевидные ошибки "
                "распознавания оценку не снижай. Проверь также условия применимости "
                "этапов и ссылки evidence на роли говорящих. Пересмотри passed, если "
                "первоначальный вывод неверен. Не добавляй действий, которых нет в "
                "разговоре. Верни полный объект JSON в том же формате, даже если "
                "исправлений нет."
            ),
        },
    ]
    last: ValueError | None = None
    for attempt in range(1, MAX_OUTPUT_ATTEMPTS + 1):
        raw = await client.complete(review_messages)
        try:
            reviewed = parse_evaluation(raw, script=script, seqs=seqs, model_name=client.model)
            _validate_evidence(reviewed, lines)
            return reviewed
        except ValueError as exc:
            last = exc
            logger.warning(
                "quality review is invalid (%s/%s): %s", attempt, MAX_OUTPUT_ATTEMPTS, exc
            )
            review_messages.append(
                {
                    "role": "user",
                    "content": f"Исправь проверенную оценку: {exc}. Верни полный JSON.",
                }
            )
    raise QualityError(f"the model returned an unusable review: {last}")


async def evaluate(
    client: QualityModel,
    lines: Sequence[TranscriptLine],
    *,
    script: CallScript = CURRENT_SCRIPT,
) -> Evaluation:
    """Оценить один звонок.

    Повтор невалидного ответа — здесь, а не уровнем выше: первый невалидный
    ответ у языковой модели — обычное дело, и звонок из-за него не должен
    остаться без оценки. Отказ самой модели (`QualityModelError`) не
    повторяется: это сеть или настройка, и решает вызывающий.
    """
    if not lines:
        raise QualityError("the call has no transcript lines")
    if len({line.seq for line in lines}) != len(lines):
        raise QualityError("the transcript has duplicate sequence numbers")
    try:
        prepared = await prepare_roles(client, lines, max_attempts=MAX_OUTPUT_ATTEMPTS)
    except RoleAttributionError as exc:
        raise QualityError(str(exc), code="roles_undetermined") from exc
    inferred_roles = tuple(
        (after.seq, after.speaker)
        for before, after in zip(lines, prepared, strict=True)
        if before.speaker != after.speaker
    )
    lines = prepared
    messages = build_messages(script, lines)
    seqs = frozenset(line.seq for line in lines)
    last: ValueError | None = None
    for attempt in range(1, MAX_OUTPUT_ATTEMPTS + 1):
        raw = await client.complete(messages)
        try:
            evaluation = parse_evaluation(raw, script=script, seqs=seqs, model_name=client.model)
            _validate_evidence(evaluation, lines)
            evaluation = await _review(client, evaluation, messages, lines, script, seqs)
            return evaluation.model_copy(update={"speaker_roles": inferred_roles})
        except ValueError as exc:
            last = exc
            logger.warning(
                "quality output does not match the schema (%s/%s): %s",
                attempt,
                MAX_OUTPUT_ATTEMPTS,
                exc,
            )
            messages.append(
                {
                    "role": "user",
                    "content": (
                        f"Исправь ответ: {exc}. Перепроверь applicable: если условие этапа "
                        "не наступило (например, клиент не сказал, что ему неудобно говорить, "
                        "или не было возражений), укажи applicable: false. Для выполненного "
                        "применимого этапа нужны реальные реплики оператора в evidence. "
                        "Если требование не подтверждено, укажи passed: false. Не подставляй "
                        "произвольные номера ради формата. Верни полный исправленный JSON."
                    ),
                }
            )
    raise QualityError(f"the model returned an unusable answer {MAX_OUTPUT_ATTEMPTS} times: {last}")


__all__ = [
    "MAX_OUTPUT_ATTEMPTS",
    "Evaluation",
    "ObjectionVerdict",
    "QualityError",
    "QualityModel",
    "StageVerdict",
    "TranscriptLine",
    "build_messages",
    "evaluate",
    "parse_evaluation",
]
