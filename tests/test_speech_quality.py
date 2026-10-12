"""Оценка звонка по скрипту: промпт, разбор ответа модели, балл и клиент.

Без базы: здесь проверяется то, что решает, какой балл получит сотрудник, —
правило «неприменимый этап засчитан», отказ от умолчаний при неполном ответе и
то, что ссылка на несуществующую реплику не доходит до карточки.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

import httpx
import pytest

from src.application.quality.evaluation import (
    MAX_OUTPUT_ATTEMPTS,
    QualityError,
    TranscriptLine,
    build_messages,
    evaluate,
    parse_evaluation,
)
from src.application.quality.roles import ROLE_BATCH
from src.domain.quality.script import (
    CURRENT_SCRIPT,
    OTHER_OBJECTION,
    UNKNOWN_OUTCOME,
    VOLNA_OUTBOUND,
)
from src.infrastructure.clients.quality import (
    QualityModelClient,
    QualityModelError,
    QualityModelNotConfigured,
    build_quality_client,
)

SCRIPT = VOLNA_OUTBOUND

LINES = (
    TranscriptLine(
        seq=0,
        speaker="operator",
        start_ms=1_000,
        text="Добрый день, меня зовут [ИМЯ], компания Волна.",
    ),
    TranscriptLine(seq=1, speaker="client", start_ms=4_000, text="Да, слушаю."),
    TranscriptLine(seq=2, speaker="operator", start_ms=6_500, text="Вас всё устраивает по тарифу?"),
    TranscriptLine(seq=3, speaker="client", start_ms=9_000, text="Не интересно."),
    TranscriptLine(
        seq=4,
        speaker="operator",
        start_ms=11_000,
        text="Не займу много времени, всё одним тарифом.",
    ),
)
SEQS = frozenset(line.seq for line in LINES)


def _answer(**overrides: Any) -> dict[str, Any]:
    """Полный годный ответ: все этапы засчитаны, одно возражение отработано."""
    answer: dict[str, Any] = {
        "script_applies": True,
        "stages": [
            {
                "key": stage.key,
                "applicable": True,
                "passed": True,
                "evidence": [0],
                "comment": "выполнено",
            }
            for stage in SCRIPT.stages
        ],
        "objections": [
            {"type": "not_interested", "seq": 3, "handled": True, "evidence": [4], "comment": ""}
        ],
        "outcome": "request",
        "summary": "Звонок по скрипту.",
    }
    answer.update(overrides)
    return answer


def _stage(answer: dict[str, Any], key: str) -> dict[str, Any]:
    found: dict[str, Any] = next(item for item in answer["stages"] if item["key"] == key)
    return found


def _parse(answer: dict[str, Any] | str) -> Any:
    raw = answer if isinstance(answer, str) else json.dumps(answer, ensure_ascii=False)
    return parse_evaluation(raw, script=SCRIPT, seqs=SEQS, model_name="test-model")


# --------------------------------------------------------------------------- #
# Скрипт
# --------------------------------------------------------------------------- #


def test_the_script_has_the_ten_stages_of_the_mock_up_in_order() -> None:
    """Сводка обязана сходиться с макетом заказчика пункт в пункт."""
    assert [stage.label for stage in SCRIPT.stages] == [
        "Приветствие, установление контакта",
        "Выявление потребностей",
        "Отработка «неудобно говорить» (техника 2 минут)",
        "Презентация продукта",
        "Отработка возражений",
        "Проверка технической возможности",
        "Согласование даты/времени монтажа (если есть возможность)",
        "Фиксация адреса и следующих шагов (если нет возможности)",
        "Обязательные условия — паспорт держателя, 18+",
        "Завершение контакта",
    ]


def test_keys_of_the_script_are_unique() -> None:
    stage_keys = [stage.key for stage in SCRIPT.stages]
    objection_keys = [item.key for item in SCRIPT.objections]
    outcome_keys = [key for key, _ in SCRIPT.outcomes]
    assert len(stage_keys) == len(set(stage_keys))
    assert len(objection_keys) == len(set(objection_keys))
    assert len(outcome_keys) == len(set(outcome_keys))
    assert OTHER_OBJECTION in objection_keys
    assert UNKNOWN_OUTCOME in outcome_keys
    assert CURRENT_SCRIPT is SCRIPT


# --------------------------------------------------------------------------- #
# Промпт
# --------------------------------------------------------------------------- #


def test_the_prompt_numbers_every_line_with_its_role_and_time() -> None:
    messages = build_messages(SCRIPT, LINES)
    assert [message["role"] for message in messages] == ["system", "user"]
    user = messages[1]["content"]
    assert "[0] 00:01 Оператор: Добрый день, меня зовут [ИМЯ], компания Волна." in user
    assert "[3] 00:09 Клиент: Не интересно." in user
    for stage in SCRIPT.stages:
        assert stage.key in user
    assert SCRIPT.version in user


def test_an_unknown_speaker_is_named_as_such_in_the_prompt() -> None:
    user = build_messages(
        SCRIPT, [TranscriptLine(seq=7, speaker="mystery", start_ms=61_000, text="Алло")]
    )[1]["content"]
    assert "[7] 01:01 Не определён: Алло" in user


# --------------------------------------------------------------------------- #
# Разбор ответа
# --------------------------------------------------------------------------- #


def test_a_complete_answer_scores_every_passed_stage() -> None:
    evaluation = _parse(_answer())
    assert evaluation.script_applies is True
    assert evaluation.score == 100
    assert evaluation.passed_count == evaluation.stage_count == len(SCRIPT.stages)
    assert evaluation.all_passed is True
    assert evaluation.outcome == "request"
    assert evaluation.model_name == "test-model"
    assert evaluation.script_version == SCRIPT.version


def test_a_failed_stage_lowers_the_score_and_the_full_correctness() -> None:
    answer = _answer()
    _stage(answer, "passport")["passed"] = False
    evaluation = _parse(answer)
    assert evaluation.score == 90
    assert evaluation.passed_count == 9
    assert evaluation.all_passed is False


def test_a_stage_that_did_not_arise_counts_as_passed() -> None:
    """Правило заказчика: «если возражений не было — этап считается выполненным»."""
    answer = _answer()
    _stage(answer, "busy").update({"applicable": False, "passed": False})
    evaluation = _parse(answer)
    busy = next(stage for stage in evaluation.stages if stage.key == "busy")
    assert busy.applicable is False
    assert busy.passed is True
    assert evaluation.score == 100


def test_a_reference_to_a_line_that_does_not_exist_is_dropped() -> None:
    answer = _answer()
    _stage(answer, "greeting")["evidence"] = [99, 2, 0, 2, True]
    evaluation = _parse(answer)
    greeting = next(stage for stage in evaluation.stages if stage.key == "greeting")
    assert greeting.evidence == (0, 2)


def test_a_missing_stage_is_not_defaulted() -> None:
    answer = _answer()
    answer["stages"] = [item for item in answer["stages"] if item["key"] != "closing"]
    with pytest.raises(ValueError, match="closing"):
        _parse(answer)


def test_a_stage_outside_the_script_and_a_repeated_stage_change_nothing() -> None:
    answer = _answer()
    answer["stages"].append({"key": "made_up", "applicable": True, "passed": False})
    answer["stages"].append({"key": "greeting", "applicable": True, "passed": False})
    evaluation = _parse(answer)
    assert evaluation.score == 100
    assert [stage.key for stage in evaluation.stages] == [stage.key for stage in SCRIPT.stages]


def test_a_verdict_that_is_not_a_boolean_is_refused() -> None:
    answer = _answer()
    _stage(answer, "needs")["passed"] = "да"
    with pytest.raises(ValueError, match="needs"):
        _parse(answer)


def test_an_outcome_outside_the_script_is_refused() -> None:
    with pytest.raises(ValueError, match="outcome"):
        _parse(_answer(outcome="sold"))


def test_a_call_outside_the_script_has_no_scores() -> None:
    evaluation = _parse({"script_applies": False, "summary": "Входящее обращение."})
    assert evaluation.script_applies is False
    assert evaluation.stages == ()
    assert evaluation.score is None
    assert evaluation.all_passed is None
    assert evaluation.passed_count is None
    assert evaluation.summary == "Входящее обращение."


def test_an_objection_outside_the_list_becomes_other() -> None:
    answer = _answer(
        objections=[{"type": "aliens", "seq": 42, "handled": False, "evidence": [4, 50]}]
    )
    objection = _parse(answer).objections[0]
    assert objection.type == OTHER_OBJECTION
    assert objection.seq is None
    assert objection.evidence == (4,)
    assert objection.handled is False


def test_an_objection_without_a_verdict_is_refused() -> None:
    with pytest.raises(ValueError, match="handled"):
        _parse(_answer(objections=[{"type": "cheaper", "seq": 3}]))


def test_reasoning_and_a_fence_around_the_answer_are_removed() -> None:
    raw = (
        '<think>черновик {"script_applies": false}</think>\n```json\n'
        + json.dumps(_answer(), ensure_ascii=False)
        + "\n```\nЕсли нужно уточнить — скажите."
    )
    assert _parse(raw).score == 100


def test_reasoning_cut_off_by_the_token_limit_is_named() -> None:
    with pytest.raises(ValueError, match="output tokens"):
        _parse('<think>{"script_applies": true, "stages": [')


def test_an_answer_without_json_is_named() -> None:
    with pytest.raises(ValueError, match="without a JSON object"):
        _parse("Оператор молодец.")


# --------------------------------------------------------------------------- #
# Оценка с повтором
# --------------------------------------------------------------------------- #


class _FakeModel:
    model = "fake-model"

    def __init__(self, *answers: str) -> None:
        self._answers = list(answers)
        self.calls: list[Sequence[Mapping[str, str]]] = []

    async def complete(self, messages: Sequence[Mapping[str, str]]) -> str:
        self.calls.append(messages)
        return self._answers.pop(0) if len(self._answers) > 1 else self._answers[0]


async def test_an_unusable_answer_is_asked_again() -> None:
    model = _FakeModel("не JSON", json.dumps(_answer(), ensure_ascii=False))
    evaluation = await evaluate(model, LINES, script=SCRIPT)
    assert evaluation.score == 100
    assert evaluation.model_name == "fake-model"
    assert len(model.calls) == 3


async def test_three_unusable_answers_leave_the_call_unscored() -> None:
    model = _FakeModel(*["{}"] * MAX_OUTPUT_ATTEMPTS)
    with pytest.raises(QualityError, match="unusable"):
        await evaluate(model, LINES, script=SCRIPT)
    assert len(model.calls) == MAX_OUTPUT_ATTEMPTS


async def test_a_call_without_lines_is_not_sent_to_the_model() -> None:
    model = _FakeModel()
    with pytest.raises(QualityError):
        await evaluate(model, [], script=SCRIPT)
    assert model.calls == []


async def test_a_refusal_of_the_model_is_not_retried_here() -> None:
    class _Refusing(_FakeModel):
        async def complete(self, messages: Sequence[Mapping[str, str]]) -> str:
            self.calls.append(messages)
            raise QualityModelError("http_401", retryable=False)

    model = _Refusing()
    with pytest.raises(QualityModelError):
        await evaluate(model, LINES, script=SCRIPT)
    assert len(model.calls) == 1


def _roles(lines: Sequence[TranscriptLine] = LINES) -> str:
    return json.dumps({"roles": [{"seq": line.seq, "speaker": line.speaker} for line in lines]})


async def test_mono_roles_are_prepared_before_scoring_the_same_text() -> None:
    mono = tuple(line.model_copy(update={"speaker": "unknown"}) for line in LINES)
    model = _FakeModel(_roles(), json.dumps(_answer()))
    evaluation = await evaluate(model, mono)
    assert evaluation.score == 100
    assert evaluation.speaker_roles == tuple((line.seq, line.speaker) for line in LINES)
    assert len(model.calls) == 3
    assert model.calls[1] == build_messages(SCRIPT, LINES)
    assert all(line.speaker == "unknown" for line in mono)


async def test_known_stereo_roles_go_straight_to_scoring() -> None:
    model = _FakeModel(json.dumps(_answer()))
    evaluation = await evaluate(model, LINES)
    assert len(model.calls) == 2
    assert model.calls[0] == build_messages(SCRIPT, LINES)
    assert evaluation.speaker_roles == ()


async def test_only_unknown_roles_are_inferred_in_a_stereo_transcript() -> None:
    mixed = tuple(
        line.model_copy(update={"speaker": "unknown"}) if line.seq == 1 else line for line in LINES
    )
    model = _FakeModel(_roles([LINES[1]]), json.dumps(_answer()))
    evaluation = await evaluate(model, mixed)
    assert evaluation.speaker_roles == ((1, "client"),)
    assert model.calls[1] == build_messages(SCRIPT, LINES)


@pytest.mark.parametrize(
    "roles",
    [
        [],
        [{"seq": 1, "speaker": "client"}, {"seq": 1, "speaker": "operator"}],
        [{"seq": 99, "speaker": "client"}],
        [{"seq": True, "speaker": "client"}],
        [{"seq": 1, "speaker": "other"}],
        [{"seq": 0, "speaker": "client"}, {"seq": 1, "speaker": "client"}],
    ],
)
async def test_bad_or_overwritten_roles_are_not_applied(roles: list[dict[str, Any]]) -> None:
    """Негодный ответ о ролях ролей не даёт: реплика остаётся без роли, оценка идёт дальше."""
    mixed = tuple(
        line.model_copy(update={"speaker": "unknown"}) if line.seq == 1 else line for line in LINES
    )
    bad = [json.dumps({"roles": roles})] * MAX_OUTPUT_ATTEMPTS
    model = _FakeModel(*bad, json.dumps(_answer()))
    evaluation = await evaluate(model, mixed)
    assert evaluation.score == 100
    assert evaluation.speaker_roles == ()
    assert len(model.calls) == MAX_OUTPUT_ATTEMPTS + 2
    # В оценку ушла расшифровка как есть — с репликой без роли.
    assert model.calls[MAX_OUTPUT_ATTEMPTS] == build_messages(SCRIPT, mixed)


async def test_a_mono_call_the_model_cannot_label_stays_unscored() -> None:
    """Моно: «не знаю» — не ответ. Модель переспрашивают, а без единой роли оценки нет."""
    model = _FakeModel('{"roles": [{"seq": 0, "speaker": "unknown"}]}')
    with pytest.raises(QualityError) as caught:
        await evaluate(model, [TranscriptLine(seq=0, speaker="unknown", start_ms=0, text="Алло")])
    assert caught.value.code == "roles_undetermined"
    assert len(model.calls) == MAX_OUTPUT_ATTEMPTS
    assert "unknown не допускается" in model.calls[0][0]["content"]
    assert "Не названы роли реплик: [0]" in model.calls[-1][-1]["content"]


async def test_mono_roles_are_asked_again_until_every_reply_has_one() -> None:
    mono = tuple(line.model_copy(update={"speaker": "unknown"}) for line in LINES)
    partial = json.dumps(
        {
            "roles": [
                {"seq": 0, "speaker": "operator"},
                {"seq": 1, "speaker": "unknown"},
                {"seq": 2, "speaker": "operator"},
                {"seq": 4, "speaker": "operator"},
            ]
        }
    )
    model = _FakeModel(partial, _roles(), json.dumps(_answer()))
    evaluation = await evaluate(model, mono)
    assert evaluation.score == 100
    assert evaluation.speaker_roles == tuple((line.seq, line.speaker) for line in LINES)
    assert len(model.calls) == 4
    # «Не знаю» и пропуск — одно и то же: роли у реплики нет, о ней спрашивают снова.
    assert "Не названы роли реплик: [1, 3]" in model.calls[1][-1]["content"]


async def test_a_mono_reply_the_model_never_names_takes_the_role_of_its_neighbour() -> None:
    mono = tuple(line.model_copy(update={"speaker": "unknown"}) for line in LINES)
    partial = json.dumps(
        {
            "roles": [
                {"seq": 2, "speaker": "operator"},
                {"seq": 3, "speaker": "client"},
                {"seq": 4, "speaker": "operator"},
            ]
        }
    )
    model = _FakeModel(*[partial] * MAX_OUTPUT_ATTEMPTS, json.dumps(_answer()))
    evaluation = await evaluate(model, mono)
    assert evaluation.score == 100
    # Первые две реплики модель не назвала ни разу: им досталась роль ближайшей
    # названной. Без роли в моно-записи не остаётся ни одна.
    assert evaluation.speaker_roles == (
        (0, "operator"),
        (1, "operator"),
        (2, "operator"),
        (3, "client"),
        (4, "operator"),
    )
    assert len(model.calls) == MAX_OUTPUT_ATTEMPTS + 2
    assert model.calls[MAX_OUTPUT_ATTEMPTS] == build_messages(
        SCRIPT,
        tuple(
            line.model_copy(update={"speaker": "operator"}) if line.seq == 1 else line
            for line in LINES
        ),
    )


async def test_a_reply_without_a_role_does_not_block_the_score() -> None:
    """Одна неясная реплика стерео-записи — не причина оставить весь звонок без оценки."""
    mixed = tuple(
        line.model_copy(update={"speaker": "unknown"}) if line.seq == 1 else line for line in LINES
    )
    answer = _answer()
    # Возражение прозвучало как раз в реплике без роли: она могла быть клиентской.
    answer["objections"][0]["seq"] = 1
    model = _FakeModel('{"roles": [{"seq": 1, "speaker": "unknown"}]}', json.dumps(answer))
    evaluation = await evaluate(model, mixed)
    assert evaluation.score == 100
    assert evaluation.objections[0].seq == 1
    assert evaluation.speaker_roles == ()
    assert len(model.calls) == 3
    assert model.calls[1] == build_messages(SCRIPT, mixed)


async def test_most_replies_without_a_role_leave_the_call_unscored() -> None:
    mostly = tuple(
        line.model_copy(update={"speaker": "unknown"}) if line.seq < 3 else line for line in LINES
    )
    roles = json.dumps({"roles": [{"seq": seq, "speaker": "unknown"} for seq in (0, 1, 2)]})
    model = _FakeModel(roles)
    with pytest.raises(QualityError) as caught:
        await evaluate(model, mostly)
    assert caught.value.code == "roles_undetermined"
    assert len(model.calls) == 1


async def test_roles_of_a_long_call_are_asked_in_batches() -> None:
    """Длинный моно-разговор: роли целиком в один ответ не помещаются."""
    truth = tuple(
        TranscriptLine(
            seq=seq,
            speaker="operator" if seq % 2 == 0 else "client",
            start_ms=seq * 1000,
            text=f"Реплика {seq}",
        )
        for seq in range(2 * ROLE_BATCH + 50)
    )
    mono = tuple(line.model_copy(update={"speaker": "unknown"}) for line in truth)

    class _ByRequest(_FakeModel):
        """Отвечает ролями ровно на те номера, о которых спросили."""

        def __init__(self) -> None:
            super().__init__()
            self.asked: list[list[int]] = []

        async def complete(self, messages: Sequence[Mapping[str, str]]) -> str:
            self.calls.append(messages)
            request = re.search(r"с номерами: \[([\d, ]+)\]", messages[-1]["content"])
            if request is None:
                return json.dumps(_answer())
            seqs = [int(item) for item in request.group(1).split(",")]
            self.asked.append(seqs)
            return _roles([truth[seq] for seq in seqs])

    model = _ByRequest()
    evaluation = await evaluate(model, mono)
    assert evaluation.score == 100
    assert [len(seqs) for seqs in model.asked] == [ROLE_BATCH, ROLE_BATCH, 50]
    assert sorted(seq for seqs in model.asked for seq in seqs) == [line.seq for line in truth]
    assert len(evaluation.speaker_roles) == len(truth)
    # Вторая пачка видит роли, найденные первой.
    assert "[0] 0 мс operator: Реплика 0" in model.calls[1][-1]["content"]
    assert f"[{ROLE_BATCH}] {ROLE_BATCH * 1000} мс unknown:" in model.calls[1][-1]["content"]


async def test_a_bad_roles_response_is_retried_before_scoring() -> None:
    mono = tuple(line.model_copy(update={"speaker": "unknown"}) for line in LINES)
    model = _FakeModel("не JSON", _roles(), json.dumps(_answer()))
    evaluation = await evaluate(model, mono)
    assert evaluation.score == 100
    assert len(model.calls) == 4


async def test_a_passed_stage_needs_operator_evidence() -> None:
    answer = _answer()
    _stage(answer, "greeting")["evidence"] = [1]
    model = _FakeModel(json.dumps(answer), json.dumps(_answer()))
    evaluation = await evaluate(model, LINES)
    assert evaluation.score == 100
    assert len(model.calls) == 3
    assert "no operator evidence" in model.calls[1][-1]["content"]


async def test_an_operator_line_cannot_be_a_client_objection() -> None:
    answer = _answer()
    answer["objections"][0]["seq"] = 0
    model = _FakeModel(*[json.dumps(answer)] * MAX_OUTPUT_ATTEMPTS)
    with pytest.raises(QualityError, match="client line"):
        await evaluate(model, LINES)


async def test_review_can_correct_a_contradictory_comment_and_verdict() -> None:
    draft = _answer()
    _stage(draft, "greeting").update(passed=False, comment="Оператор не представился.")
    corrected = _answer()
    _stage(corrected, "greeting")["comment"] = "Представился и назвал компанию."
    model = _FakeModel(json.dumps(draft), json.dumps(corrected))
    evaluation = await evaluate(model, LINES)
    assert evaluation.score == 100
    assert evaluation.stages[0].comment == "Представился и назвал компанию."
    assert LINES[0].text in model.calls[1][1]["content"]
    assert "Оператор не представился." in model.calls[1][-2]["content"]


async def test_an_invalid_review_is_not_published_as_the_original_score() -> None:
    invalid = _answer()
    _stage(invalid, "greeting")["evidence"] = [1]
    model = _FakeModel(json.dumps(_answer()), json.dumps(invalid))
    with pytest.raises(QualityError, match="unusable review"):
        await evaluate(model, LINES)
    assert len(model.calls) == 1 + MAX_OUTPUT_ATTEMPTS


# --------------------------------------------------------------------------- #
# Клиент модели
# --------------------------------------------------------------------------- #


def _client() -> QualityModelClient:
    return QualityModelClient(base_url="http://llm/v1", model="m", api_key="secret")


def _respond(monkeypatch: pytest.MonkeyPatch, response: httpx.Response) -> list[dict[str, Any]]:
    sent: list[dict[str, Any]] = []

    async def _post(self: httpx.AsyncClient, url: str, **kwargs: Any) -> httpx.Response:
        sent.append({"url": url, **kwargs})
        response.request = httpx.Request("POST", url)
        return response

    monkeypatch.setattr(httpx.AsyncClient, "post", _post)
    return sent


def _completion(content: str | None, finish_reason: str = "stop") -> httpx.Response:
    return httpx.Response(
        200,
        json={"choices": [{"message": {"content": content}, "finish_reason": finish_reason}]},
    )


async def test_the_client_asks_chat_completions_without_thinking_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent = _respond(monkeypatch, _completion('{"ok": true}'))
    assert await _client().complete([{"role": "user", "content": "привет"}]) == '{"ok": true}'
    assert sent[0]["url"] == "http://llm/v1/chat/completions"
    assert sent[0]["headers"] == {"Authorization": "Bearer secret"}
    payload = sent[0]["json"]
    assert payload["model"] == "m"
    assert payload["temperature"] == 0
    # Поле перебило бы умолчание быстрого пути и включило рассуждение (ADR-0011).
    assert "chat_template_kwargs" not in payload


@pytest.mark.parametrize(
    ("status", "retryable"),
    [(500, True), (503, True), (401, False), (404, False)],
)
async def test_http_errors_say_whether_a_retry_helps(
    monkeypatch: pytest.MonkeyPatch, status: int, retryable: bool
) -> None:
    _respond(monkeypatch, httpx.Response(status, text="разговор целиком"))
    with pytest.raises(QualityModelError) as caught:
        await _client().complete([{"role": "user", "content": "x"}])
    assert caught.value.reason == f"http_{status}"
    assert caught.value.retryable is retryable
    assert "разговор" not in str(caught.value)


def test_only_key_and_address_refusals_stop_the_whole_batch() -> None:
    """Слишком длинный разговор — отказ одного звонка, а не причина бросить остальные."""
    assert QualityModelError("http_401", retryable=False).misconfigured
    assert QualityModelError("http_404", retryable=False).misconfigured
    assert not QualityModelError("http_400", retryable=False).misconfigured
    assert not QualityModelError("empty_answer_length", retryable=False).misconfigured
    assert not QualityModelError("timeout", retryable=True).misconfigured


async def test_an_answer_cut_by_the_token_limit_is_not_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _respond(monkeypatch, _completion(None, finish_reason="length"))
    with pytest.raises(QualityModelError) as caught:
        await _client().complete([{"role": "user", "content": "x"}])
    assert caught.value.reason == "empty_answer_length"
    assert caught.value.retryable is False


async def test_a_body_without_choices_is_malformed(monkeypatch: pytest.MonkeyPatch) -> None:
    _respond(monkeypatch, httpx.Response(200, json={"error": "nope"}))
    with pytest.raises(QualityModelError, match="malformed_response"):
        await _client().complete([{"role": "user", "content": "x"}])


async def test_a_timeout_is_retryable(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _post(self: httpx.AsyncClient, url: str, **kwargs: Any) -> httpx.Response:
        raise httpx.ReadTimeout("slow")

    monkeypatch.setattr(httpx.AsyncClient, "post", _post)
    with pytest.raises(QualityModelError) as caught:
        await _client().complete([{"role": "user", "content": "x"}])
    assert caught.value.reason == "timeout"
    assert caught.value.retryable is True


class _Settings:
    speech_quality_llm_base_url = "http://10.0.0.1:8000/v1/"
    speech_quality_llm_api_key = "super-secret-value"
    speech_quality_llm_model = ""
    speech_quality_llm_timeout_seconds = 30.0
    speech_quality_llm_max_tokens = 2048


def test_the_client_is_built_from_settings() -> None:
    client = build_quality_client(_Settings())
    assert client.base_url == "http://10.0.0.1:8000/v1"
    assert client.model == "qwen3.8-27b-fast"
    assert client.timeout_seconds == 30.0
    assert client.max_tokens == 2048
    assert "super-secret-value" not in repr(client)


def test_no_address_is_a_deployment_error_not_a_silent_skip() -> None:
    class _Empty(_Settings):
        speech_quality_llm_base_url = "  "

    with pytest.raises(QualityModelNotConfigured, match="SPEECH_QUALITY_LLM_BASE_URL"):
        build_quality_client(_Empty())


@pytest.mark.parametrize(
    ("argv", "reason"),
    [
        # Без лимита первый же запуск ушёл бы в модель со всем архивом.
        (["--all"], "--all needs --limit"),
        (["--all", "--limit", "100", "--from", "2026-10-01", "--to", "2026-10-02"], "--all takes"),
        (["--all", "--limit", "100", "--call", "telphin:1"], "--all takes"),
        (["--from", "2026-10-01"], "give --from and --to"),
        (["--all", "--limit", "100", "--skip-failed", "--force"], "--skip-failed does not apply"),
        (["--call", "telphin:1", "--skip-failed"], "--skip-failed does not apply"),
        # Сверка смотрит все оценки: отбор звонков к ней не относится.
        (["--drop-stale", "--all", "--limit", "100"], "--drop-stale checks every"),
        (["--drop-stale", "--call", "telphin:1"], "--drop-stale checks every"),
        (["--drop-stale", "--force"], "--drop-stale checks every"),
    ],
)
def test_the_command_refuses_arguments_that_contradict_each_other(
    argv: list[str], reason: str
) -> None:
    """Отказ — до настроек и базы: опечатка в юните не должна стоить запроса к модели."""
    from src.presentation.cli.evaluate import main

    with pytest.raises(SystemExit) as refused:
        main(argv)
    assert reason in str(refused.value)
