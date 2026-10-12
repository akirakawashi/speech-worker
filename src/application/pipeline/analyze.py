"""Этап `analyze`: классификация, тональность и резюме звонка.

Последний этап цепочки, и единственный, чей отказ **не убирает звонок из
поиска**. Текст к этому моменту расшифрован; отсутствует только разметка.
Требование PRC-05 прямо говорит: отказ разметки оставляет звонок найденным.

В языковую модель уходит текст расшифровки как есть (`text_raw`): обезличивания
в конвейере нет.

**Классификация — путь по пятиуровневому дереву оператора** (SP-310, решение
владельца S-22): тот же путь, что оператор ставит в NCC, — чтобы сравнивать их
уровень в уровень. Модель видит включённые ветки классификатора и отвечает
названиями уровней. Путь, названный мимо дерева, узлов не заводит: остаётся то,
что нашлось выше, — строить классификатор из догадок модели нельзя. Сегмент и
продукт звонка — уровни 1 и 2 этого пути (S-21).

**Тема по предложению ИИ помощника** (SP-309, путём с SP-310) — отдельно: там
модель говорит, как считает сама, и на любом уровне вправе дать свою
формулировку. Дерево от этого не меняется — предложение живёт только в звонке.

**Причина обращения** (S-32) — в том же ответе: зачем звонил клиент, фразой и
кодом справочника `speech_call_reasons`. Поля необязательные: пустой справочник
или ответ без причины разбор не роняют, а старая редакция воркфлоу вход
`reasons` просто не объявляет — LLM отбрасывает необъявленный вход.
"""

from __future__ import annotations

import logging
import re
import uuid
from typing import Any, Final

from src.application.dto.pipeline import (
    AnalyzeWork as AnalyzeWork,
)
from src.application.dto.pipeline_input import AnalysisSegment, CallForAnalysis
from src.application.errors.external import AnalysisError, AnalysisNotConfigured
from src.application.model_output import _json_object as _json_object
from src.application.pipeline.stages import StageDeps, StageError, StageHandler, StageResult
from src.application.ports.pipeline import StagePersistence
from src.application.ports.queue import ClaimedJob
from src.domain.calls.classifier import (
    EMPTY_LEVEL_LABEL,
    ClassifierIndex,
    classifier_levels,
    clean_label,
    dimensions,
)
from src.domain.calls.contracts import (
    JobErrorClass,
    JobKind,
    ProcessingStatus,
    RoleMethod,
    Sentiment,
    Speaker,
)
from src.domain.calls.reasons import (
    REASONS_PROMPT_LIMIT,
    CallReason,
    interpret_reason,
    prompt_reasons,
)

logger = logging.getLogger(__name__)

#: Резюме обрезается: оно живёт в карточке звонка рядом с темой, и абзац на
#: полстраницы там не помещается ни на одном экране.
SUMMARY_LIMIT = 300

#: Название уровня в предложении ИИ помощника — подпись в карточке и строка
#: отчёта, а не абзац. Модели сказано «до 80 символов»; предел с запасом на её «до».
SUGGESTION_LABEL_LIMIT = 120

#: Сколько раз воркфлоу получает шанс ответить по схеме.
#:
#: Было два, стало три — по измерению на корпусе из шестидесяти одного звонка.
#: Один из них дважды подряд получил в ответ прозу вместо JSON, задание умерло
#: терминальным `invalid_output`, а тот же звонок, запущенный ещё раз руками,
#: разобрался с первой попытки.
#:
#: `temperature = 0` не делает модель детерминированной: батчинг на стороне
#: vLLM меняет числа. «Дважды подряд не вышло» — это не «не выйдет никогда»,
#: и ровно от этого третья попытка и спасает.
#:
#: Больше трёх не нужно: цена измерена — один лишний вызов на шестьдесят один
#: звонок, — а систематически неразбираемый ответ тремя попытками не чинится.
MAX_OUTPUT_ATTEMPTS = 3


#: Классификатор целиком, с выключенными узлами. Какие ветки показывать модели,
#: решает `ClassifierIndex`: выключенный узел убирает и всю ветку под собой, а
#: этого одним `WHERE` не сказать.

#: Справочник причин — только включённые коды: выключенный модель не видит, а
#: код мимо отправленного списка разбор не сохранит.

#: Сегмент и продукт пишутся **только если их не знает проект**. `COALESCE`
#: здесь неуместен: он вернул бы старое значение и при этом переписал источник.
#: Условие в `CASE` смотрит на источник, потому что источник и есть ответ на
#: вопрос «знает ли проект».
#:
#: Предложение ИИ помощника — названия уровней (`jsonb`) и узел, если весь путь
#: предложения есть в дереве.


class AnalyzeHandler(StageHandler[AnalyzeWork]):
    """Классифицировать звонок, оценить тональность и написать резюме."""

    kind = JobKind.ANALYZE

    async def work(self, job: ClaimedJob, deps: StageDeps) -> AnalyzeWork:
        """Загрузить снимок и выполнить долгую фазу этапа через порты; вернуть DTO для записи."""
        call, segments, classifier, reasons = await self._load(job, deps)
        if not segments:
            raise StageError(
                JobErrorClass.NOT_FOUND, f"call {job.call_id} has no segments to analyse"
            )
        if deps.analysis is None:
            raise StageError(JobErrorClass.PROVIDER_ERROR, "the worker has no analysis client")
        if not len(classifier):
            # Без дерева модели не из чего выбирать, и каждый ответ ушёл бы мимо.
            # Это ошибка развёртывания: повтор задания дерево не загрузит.
            raise StageError(
                JobErrorClass.INVALID_OUTPUT,
                "the classifier is empty: load it with "
                "python -m src.presentation.cli.seed_classifier --apply",
            )

        needs_roles = call.role_method == RoleMethod.NONE.value
        reference = prompt_reasons(reasons)
        if len(reference) > REASONS_PROMPT_LIMIT:
            # Справочник правили в обход загрузки. Длиннее предела воркфлоу
            # отвечает `400` на весь разбор, а причина не стоит разбора звонка.
            logger.getChild("work").warning(
                "the reason reference is %d characters, the workflow accepts %d: sent without it",
                len(reference),
                REASONS_PROMPT_LIMIT,
            )
            reasons, reference = [], ""
        inputs = {
            "call_id": str(job.call_id),
            "needs_roles": needs_roles,
            "classifier": classifier.prompt_tree(),
            "reasons": reference,
            "segments": [
                {"seq": int(s.seq), "speaker": str(s.speaker), "text": s.text_raw} for s in segments
            ],
        }

        outputs = await self._run_until_valid(deps, inputs, job)
        return _interpret(
            outputs,
            classifier=classifier,
            needs_roles=needs_roles,
            reason_codes=frozenset(reason.code for reason in reasons),
        )

    async def _run_until_valid(
        self, deps: StageDeps, inputs: dict[str, Any], job: ClaimedJob
    ) -> dict[str, Any]:
        """Вызвать воркфлоу и убедиться, что ответ разбирается.

        Повтор здесь, внутри одной попытки задания, а не через очередь. Причина
        в том, что `invalid_output` — терминальный класс: отдав его очереди, мы
        похоронили бы задание после **первого** невалидного ответа модели, а
        первый невалидный ответ у языковой модели — обычное дело.
        """
        if deps.analysis is None:
            raise RuntimeError("analysis backend is not configured")
        last: Exception | None = None
        for attempt in range(1, MAX_OUTPUT_ATTEMPTS + 1):
            outputs: dict[str, Any]
            try:
                outputs = await deps.analysis.run(inputs)
            except AnalysisNotConfigured as exc:
                raise StageError(JobErrorClass.INVALID_OUTPUT, str(exc)) from exc
            except AnalysisError as exc:
                if not exc.retryable:
                    raise StageError(
                        JobErrorClass.PROVIDER_ERROR, f"analysis workflow refused: {exc.reason}"
                    ) from exc
                raise StageError(
                    JobErrorClass.TIMEOUT
                    if exc.reason == "timeout"
                    else JobErrorClass.PROVIDER_ERROR,
                    f"analysis workflow failed: {exc.reason}",
                ) from exc

            # Сырой ответ модели приводится к объекту здесь, а не в LLM:
            # разбор решает, годен ли ответ, и место ему там, где он покрыт
            # тестами, а не в поле GUI, исполнение которого зависит от службы
            # песочницы.
            problem = describe_raw_problem(outputs)
            outputs = normalise_outputs(outputs)
            try:
                if problem:
                    raise ValueError(problem)
                _require_shape(outputs)
            except ValueError as exc:
                last = exc
                logger.getChild("work").warning(
                    "call=%s: analysis output does not match the schema (%s/%s): %s",
                    job.call_id,
                    attempt,
                    MAX_OUTPUT_ATTEMPTS,
                    exc,
                )
                continue
            return outputs

        raise StageError(
            JobErrorClass.INVALID_OUTPUT,
            f"analysis workflow returned an unusable answer {MAX_OUTPUT_ATTEMPTS} times: {last}",
        )

    async def _load(
        self, job: ClaimedJob, deps: StageDeps
    ) -> tuple[CallForAnalysis, list[AnalysisSegment], ClassifierIndex, list[CallReason]]:
        """Получить DTO входа разбора через репозиторий и подготовить доменное дерево."""
        if deps.repository is None:
            raise RuntimeError("pipeline repository is not configured")
        data = await deps.repository.load_analysis(job.call_id)
        if data is None:
            raise StageError(JobErrorClass.NOT_FOUND, f"call {job.call_id} is gone")
        return (
            data.call,
            list(data.segments),
            ClassifierIndex([node.model_dump() for node in data.classifier]),
            list(data.reasons),
        )

    async def store(
        self, persistence: StagePersistence, job: ClaimedJob, work: AnalyzeWork
    ) -> StageResult:
        """Сохранить DTO разбора в транзакции и вернуть завершение цепочки."""
        await persistence.save_analysis(job, work)
        return StageResult(processing_status=ProcessingStatus.DONE, next_kind=None)


#: Поля, в которых воркфлоу может вернуть сырой ответ модели. `answer` — как
#: называет его LLM в приложениях-чатах, `text` — как называется выход узла
#: модели; воркфлоу отдаёт одно из двух в зависимости от того, как собран.
_RAW_ANSWER_KEYS: Final[tuple[str, ...]] = ("answer", "text", "output")


#: Рассуждение модели перед ответом. Рассуждающие модели пишут его в открытую, и
#: внутри него встречается черновой JSON — не тот, который они в итоге дают.
_THINK_BLOCK: Final[re.Pattern[str]] = re.compile(
    r"<think\b[^>]*>.*?</think\s*>", re.DOTALL | re.IGNORECASE
)


def describe_raw_problem(outputs: Any) -> str | None:
    """Что не так с сырым ответом, если с ним что-то не так.

    Отдельно от `_require_shape`, потому что говорит о другом: та проверяет
    разобранный объект, а эта — сам ответ модели, который до объекта не дошёл.

    Оборванное рассуждение — не поломка промпта и не каприз модели, а потолок
    токенов. Записав в лог «path is missing», мы отправили бы читателя искать
    ошибку в промпте.
    """
    if not isinstance(outputs, dict) or "path" in outputs:
        return None
    for key in _RAW_ANSWER_KEYS:
        raw = outputs.get(key)
        if not isinstance(raw, str) or not raw.strip():
            continue
        stripped = _THINK_BLOCK.sub("", raw)
        if "<think" in stripped.lower():
            return (
                f"the model ran out of output tokens while reasoning "
                f"({len(raw)} characters, no closing </think>): raise max_tokens"
            )
        if "{" not in stripped:
            return "the model answered without a JSON object"
        return None
    return "the model returned no answer at all"


def normalise_outputs(outputs: Any) -> dict[str, Any]:
    """Привести выход воркфлоу к объекту, который умеет читать этап.

    Две формы, и обе настоящие:

    * **разобранная** — воркфлоу сам вернул `path`, `sentiment` и прочее.
      Так было задумано изначально, и так будет, если узел кода в LLM
      заработает;
    * **сырая** — воркфлоу вернул текст ответа модели одним полем. Так он
      устроен сейчас: узел кода зависит от службы песочницы, а она на боевом
      контуре отвечает `403`.

    Возвращается объект в любом случае; годен он или нет — решает
    `_require_shape`, и решает одинаково для обеих форм.
    """
    if not isinstance(outputs, dict):
        return {}
    if "path" in outputs:
        return outputs
    for key in _RAW_ANSWER_KEYS:
        raw = outputs.get(key)
        if isinstance(raw, str) and raw.strip():
            return _json_object(raw) or {}
    return outputs


def _require_shape(outputs: Any) -> None:
    """Ответ разбирается или нет. Ни одного умолчания здесь нет намеренно.

    Подставив `neutral` вместо непонятной тональности, мы получили бы отчёт, в
    котором нейтральные звонки — это смесь настоящих нейтральных и тех, что
    модель не смогла разметить.

    Годен ли путь, здесь не решается: путь, ушедший мимо дерева, — ответ, и
    разбор сохранит найденное. `ai_path` не проверяется вовсе: это мнение
    модели сверх разметки, и звонок без него остаётся размеченным.
    """
    if not isinstance(outputs, dict):
        raise ValueError("outputs must be an object")
    path = outputs.get("path")
    if not isinstance(path, list) or not any(
        isinstance(level, str) and level.strip() for level in path
    ):
        raise ValueError("path is missing")
    sentiment = outputs.get("sentiment")
    if not isinstance(sentiment, str) or sentiment not in {s.value for s in Sentiment}:
        raise ValueError("sentiment is not one of the three known values")


def _interpret(
    outputs: dict[str, Any],
    *,
    classifier: ClassifierIndex,
    needs_roles: bool,
    reason_codes: frozenset[str] = frozenset(),
) -> AnalyzeWork:
    """Проверить ответ анализа и преобразовать его в результат этапа."""
    asked = _levels(outputs["path"])
    resolved = classifier.resolve(asked)
    if len(resolved.labels) < len(asked):
        logger.getChild("interpret").info(
            "the path leaves the classifier after level %d of %d", len(resolved.labels), len(asked)
        )

    summary = outputs.get("summary")
    trimmed = str(summary).strip()[:SUMMARY_LIMIT] if isinstance(summary, str) else None

    roles: list[tuple[int, Speaker]] = []
    if needs_roles:
        # Роли берутся только когда их просили. Иначе модель переписала бы
        # атрибуцию, полученную измерением энергии каналов, — а измерение
        # надёжнее догадки по тексту.
        for item in outputs.get("roles") or []:
            if not isinstance(item, dict):
                continue
            speaker = str(item.get("speaker") or "")
            if speaker not in {s.value for s in Speaker}:
                continue
            try:
                roles.append((int(item["seq"]), Speaker(speaker)))
            except (KeyError, TypeError, ValueError):
                continue

    segment, product = dimensions(resolved.labels)
    suggestion, suggestion_node_id = _suggestion(outputs.get("ai_path"), classifier)
    reason = interpret_reason(outputs, reason_codes)

    return AnalyzeWork(
        node_id=resolved.node_id,
        path=resolved.labels,
        sentiment=Sentiment(str(outputs["sentiment"])),
        summary=trimmed or None,
        segment=segment,
        product=product,
        roles=tuple(roles),
        suggestion=suggestion,
        suggestion_node_id=suggestion_node_id,
        call_reason=reason.text,
        call_reason_code=reason.code,
    )


def _levels(value: Any) -> tuple[str, ...]:
    """Уровни из ответа модели по местам: «(пусто)» и прочерк — пустой уровень."""
    if not isinstance(value, list):
        return ()
    return classifier_levels(
        "" if not isinstance(item, str) or clean_label(item) == EMPTY_LEVEL_LABEL else item
        for item in value
    )


def _suggestion(
    value: Any, classifier: ClassifierIndex
) -> tuple[tuple[str, ...], uuid.UUID | None]:
    """Тема по суждению модели: путь, на любом уровне которого может быть своя формулировка.

    Путь, целиком нашедшийся в дереве, — это узел дерева, и названия берутся из
    дерева: иначе одна и та же ветка считалась бы в отчёте дважды — узлом и
    словами. Путь с формулировкой, которой в дереве нет, хранится словами.
    """
    levels = tuple(level[:SUGGESTION_LABEL_LIMIT].strip() for level in _levels(value))
    if not any(levels):
        return (), None
    resolved = classifier.resolve(levels)
    if resolved.node_id is not None and len(resolved.labels) == len(levels):
        return resolved.labels, resolved.node_id
    return levels, None


__all__ = ["SUGGESTION_LABEL_LIMIT", "SUMMARY_LIMIT", "AnalyzeHandler", "AnalyzeWork"]
