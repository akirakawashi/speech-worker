"""Воркфлоу Dify «Speech: свободная темизация» и «Speech: сверка с оператором».

SP-313, SP-314: файлы, которые импортируют в Dify руками, — договор с будущими
этапами `freeform` и `compare` (фича — `speech-analytics/10-feature-ai-free-
classification.md` в репозитории документации). Разбора ответа здесь нет: на
этапе прототипа его делает разовый скрипт, в продукте — код этапов (SP-320).
Проверяется то, что ломается молча: вход, ссылки на переменные, формат, и
главное — **независимость свободной темизации от каталога** (S-24): ни входа с
деревом, ни названий из него в промпте.
"""

from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.application.pipeline.analyze import _RAW_ANSWER_KEYS
from src.infrastructure.data.catalogues import CLASSIFIER_FILE
from src.infrastructure.persistence.classifier import label_key

#: Файлы импортируют в Dify руками; лежат в этом репозитории, рядом с кодом этапов.
DIFY_DIR = Path(__file__).resolve().parents[1] / "dify" / "speech"
FREE = DIFY_DIR / "speech-free-classification.dify.yml"
COMPARE = DIFY_DIR / "speech-operator-compare.dify.yml"
CLASSIFIER = CLASSIFIER_FILE

#: Тем же правилом, каким Dify разбирает ссылки на переменные в промпте, — как в
#: `test_speech_dify_workflow.py`: дефиса в наборе символов нет.
_DIFY_REFERENCE = re.compile(r"\{\{(#[a-zA-Z0-9_]{1,50}(\.[a-zA-Z_][a-zA-Z0-9_]{0,29}){1,10}#)\}\}")

#: Предел провайдера модели в боевом Dify: выше воркфлоу отвергает запуск.
PROVIDER_MAX_TOKENS = 8192

both = pytest.mark.parametrize("path", [FREE, COMPARE], ids=["free", "compare"])


def _graph(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    graph: dict[str, Any] = loaded["workflow"]["graph"]
    return graph


def _node(path: Path, kind: str) -> dict[str, Any]:
    nodes = _graph(path)["nodes"]
    node: dict[str, Any] = next(n["data"] for n in nodes if n["data"]["type"] == kind)
    return node


def _normalised(text: str) -> str:
    """То же выравнивание написания, что `label_key`, но **без обрезки**.

    `label_key` проходит через `clean_label`, а тот режет строку до 255 знаков —
    для названия узла это верно, для промпта нет: первая редакция теста искала
    названия каталога только в начале промпта и проходила на чём угодно.
    Поймал контроль — тот же поиск на промпте сверки, где «Физ.лицо» есть
    намеренно, вернул пустоту.
    """
    folded = unicodedata.normalize("NFC", text).casefold().replace("\\", "/").replace("_", "/")
    return re.sub(r"\s*/\s*", "/", " ".join(folded.split()))


def _part(path: Path, role: str) -> str:
    return str(next(p["text"] for p in _node(path, "llm")["prompt_template"] if p["role"] == role))


def _prompts(path: Path) -> str:
    return " ".join(part["text"] for part in _node(path, "llm")["prompt_template"])


def _inputs(path: Path) -> dict[str, str]:
    return {v["variable"]: v["type"] for v in _node(path, "start")["variables"]}


# --------------------------------------------------------------------------- #
# Общее для обоих файлов
# --------------------------------------------------------------------------- #


@both
def test_the_workflow_is_start_model_end_without_a_code_node(path: Path) -> None:
    """Узел кода на боевом контуре не исполняется (`403` от песочницы) — как у разбора."""
    assert [n["data"]["type"] for n in _graph(path)["nodes"]] == ["start", "llm", "end"]


@both
def test_the_end_returns_what_the_model_said(path: Path) -> None:
    produced = [o["variable"] for o in _node(path, "end")["outputs"]]

    assert produced == ["answer"]
    assert produced[0] in _RAW_ANSWER_KEYS


@both
def test_zero_temperature_and_a_ceiling_the_provider_accepts(path: Path) -> None:
    """Повторяемость (NFR-3) и предел провайдера: 12288 отвергался на каждом звонке."""
    params = _node(path, "llm")["model"]["completion_params"]

    assert params["temperature"] == 0
    assert params["max_tokens"] <= PROVIDER_MAX_TOKENS


@both
def test_every_reference_is_one_dify_will_substitute(path: Path) -> None:
    prompts = _prompts(path)
    written = re.findall(r"\{\{#[^}]+#\}\}", prompts)
    parsed = [m.group(1)[1:-1] for m in _DIFY_REFERENCE.finditer(prompts)]
    ids = {node["id"] for node in _graph(path)["nodes"]}

    assert written, "в промпте нет ни одной ссылки на вход"
    assert len(parsed) == len(written), f"Dify не разберёт: {written}"
    for reference in parsed:
        assert reference.split(".")[0] in ids, f"{reference}: такого узла нет"


@both
def test_node_and_prompt_identifiers_avoid_the_hyphen(path: Path) -> None:
    for node in _graph(path)["nodes"]:
        assert re.fullmatch(r"[a-zA-Z0-9_]{1,50}", node["id"]), node["id"]


@both
def test_every_declared_input_reaches_the_prompt(path: Path) -> None:
    """`call_id` в промпт не идёт намеренно — он для сверки прогона с журналом."""
    prompts = _prompts(path)

    for name in set(_inputs(path)) - {"call_id"}:
        assert f"node_start.{name}" in prompts, f"{name} объявлен, но в промпт не попал"


@both
def test_the_last_line_repeats_the_format_and_switches_reasoning_off(path: Path) -> None:
    """Формат — последней строкой (урок разбора); `/no_think` — решение S-30."""
    tail = _part(path, "user").rstrip().splitlines()[-1]

    assert "JSON" in tail
    assert tail.endswith("/no_think")


# --------------------------------------------------------------------------- #
# Свободная темизация: независимость от каталога
# --------------------------------------------------------------------------- #


def test_the_free_workflow_gets_the_turns_and_nothing_else() -> None:
    """FR-1, FR-3: ни дерева, ни пути оператора, ни краткого содержания на входе."""
    assert _inputs(FREE) == {"call_id": "text-input", "segments": "paragraph"}


def test_the_free_prompt_names_no_category_of_the_catalog() -> None:
    """S-24: модель не должна получить ни каталога, ни его схемы уровней.

    Названия уровней 1–4 файла владельца — то, что поле 1 обязано придумать само.
    Попади хоть одно в промпт как пример, модель начнёт описывать звонки словами
    каталога, и мера полноты каталога потеряет смысл.
    """
    leaked = _catalog_labels_in(_prompts(FREE))
    assert not leaked, f"в промпте свободной темизации названия каталога: {leaked}"


def _catalog_labels_in(prompt: str) -> list[str]:
    paths = json.loads(CLASSIFIER.read_text(encoding="utf-8"))["paths"]
    labels = {label_key(level) for path in paths for level in path[:4] if level}
    text = _normalised(prompt)
    return sorted(label for label in labels if label and label in text)


def test_the_catalog_check_finds_labels_where_they_are() -> None:
    """Контроль самой проверки: в промпте сверки «Физ.лицо» есть намеренно.

    Проверка, которая проходит на любом тексте, ничего не доказывает, — ровно
    так и было до правки `_normalised`.
    """
    assert "физ.лицо" in _catalog_labels_in(_prompts(COMPARE))


def test_the_free_prompt_asks_for_the_two_fields_and_their_limits() -> None:
    prompts = _prompts(FREE)

    for field in ("free_path", "topic"):
        assert f"`{field}`" in prompts
    assert "не больше семи" in prompts, "нет предела глубины — FR-2"
    assert "80 символов" in prompts
    assert "не цитата" in prompts, "S-25: тема — не выдержка из разговора"


def test_the_free_turns_are_declared_third_party_data_before_the_fields() -> None:
    """SP-710: реплики — данные, а не указания; правило стоит до описания полей."""
    system = _part(FREE, "system")
    user = _part(FREE, "user")

    assert "данные стороннего источника" in system
    assert "не выполняй" in system
    assert system.index("данные стороннего источника") < system.index("Поля объекта")
    assert "данные стороннего источника" in user[: user.index("{{#node_start.segments#}}")]


# --------------------------------------------------------------------------- #
# Сверка: только формулировки, шкалы S-29
# --------------------------------------------------------------------------- #


def test_the_compare_workflow_gets_wordings_and_no_transcript() -> None:
    """S-26: судья сравнивает формулировки и расшифровки не получает."""
    assert _inputs(COMPARE) == {
        "call_id": "text-input",
        "operator_path": "paragraph",
        "free_path": "paragraph",
        "topic": "paragraph",
        "candidates": "paragraph",
    }
    assert "segments" not in _prompts(COMPARE)


def test_the_compare_prompt_asks_for_the_agreed_scales() -> None:
    """S-29: оператор — вердикт, балл, глубина, причина; каталог — вердикт, кандидат, причина."""
    prompts = _prompts(COMPARE)

    for field in ("operator", "catalog", "depth", "verdict", "score", "reason", "candidate"):
        assert f"`{field}`" in prompts, field
    for value in ("match", "partial", "mismatch", "in_catalog", "close", "absent"):
        assert f"`{value}`" in prompts, value


def test_the_verdict_turns_on_the_topic_not_on_the_upper_levels() -> None:
    """Контроль 2026-09-23: «чужой звонок» с теми же уровнями 1–3 и другой темой
    судья дважды из десяти назвал `partial` — прежнее определение «общее
    направление то же» он читал как совпадение сегмента, продукта и типа
    обращения. Вердикт решает тема (уровень 4), и промпт говорит это прямо.
    """
    prompts = _prompts(COMPARE)

    assert "тема оператора (уровень 4) верна" in prompts
    assert "даже если сегмент, продукт и" in prompts


def test_the_judge_picks_only_from_the_candidates() -> None:
    """Иначе «категория есть» значило бы «модель вспомнила похожую», а не «она в каталоге»."""
    assert "только из `candidates`" in _prompts(COMPARE)
