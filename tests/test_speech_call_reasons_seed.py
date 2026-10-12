"""Загрузка справочника причин обращения из файла (S-32, SP-325) — без базы."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.domain.calls.reasons import REASONS_PROMPT_LIMIT
from src.infrastructure.data.catalogues import REASONS_FILE, load_reasons


def _file(tmp_path: Path, reasons: list[dict[str, str]]) -> Path:
    path = tmp_path / "call_reasons.json"
    path.write_text(json.dumps({"reasons": reasons}, ensure_ascii=False), encoding="utf-8")
    return path


def test_the_owners_reference_loads_whole() -> None:
    reasons = load_reasons(REASONS_FILE)

    assert len(reasons) == 27
    assert reasons[0].caption == "K01 · Не работает мобильный интернет"
    assert all(reason.hint for reason in reasons), "у каждого кода есть указание модели"


def test_a_long_label_is_refused_not_cut(tmp_path: Path) -> None:
    """Обрезанная подпись молча разошлась бы с тем, что утвердил владелец."""
    path = _file(tmp_path, [{"code": "K01", "label": "Очень " * 60, "hint": ""}])

    with pytest.raises(ValueError, match="longer than 255"):
        load_reasons(path)


def test_a_long_hint_is_kept_whole(tmp_path: Path) -> None:
    hint = "Правило " * 60
    path = _file(tmp_path, [{"code": "K01", "label": "Подпись", "hint": hint}])

    (reason,) = load_reasons(path)

    assert reason.hint == hint.strip()


def test_a_reference_longer_than_the_workflow_accepts_is_refused(tmp_path: Path) -> None:
    long_hint = "Правило. " * 100
    reasons = [
        {"code": f"K{number:02d}", "label": f"Код {number}", "hint": long_hint}
        for number in range(1, 30)
    ]

    with pytest.raises(ValueError, match=str(REASONS_PROMPT_LIMIT)):
        load_reasons(_file(tmp_path, reasons))


@pytest.mark.parametrize("code", ["K1", "K001", "X01", "К01", ""])
def test_a_code_outside_the_pattern_is_refused(tmp_path: Path, code: str) -> None:
    with pytest.raises(ValueError, match="K and two digits"):
        load_reasons(_file(tmp_path, [{"code": code, "label": "Подпись"}]))


def test_a_code_listed_twice_is_refused(tmp_path: Path) -> None:
    path = _file(tmp_path, [{"code": "K01", "label": "А"}, {"code": "K01", "label": "Б"}])

    with pytest.raises(ValueError, match="twice"):
        load_reasons(path)
