"""Проверка справочников владельца до любых изменений в хранилище."""

from src.domain.calls.classifier import CLASSIFIER_DEPTH, classifier_levels, clean_label
from src.domain.calls.reasons import CODE_PATTERN, REASONS_PROMPT_LIMIT, CallReason, prompt_reasons

LABEL_LIMIT = 255


def _normalised(value: object) -> str:
    """Как `clean_label`, но без обрезки: длинное здесь — ошибка файла, а не мусор."""
    return clean_label(value, limit=None)


def validate_classifier(data: object) -> list[tuple[str, ...]]:
    """Прочитать пути и убедиться, что каждый — классификация, а не обрывок.

    Проверки здесь, а не в базе: ошибка в файле должна быть видна до того, как
    половина дерева уедет в таблицу.
    """
    raw_paths = data.get("paths") if isinstance(data, dict) else None
    if not isinstance(raw_paths, list) or not raw_paths:
        raise ValueError("classifier: no paths")

    paths: list[tuple[str, ...]] = []
    for number, raw in enumerate(raw_paths, start=1):
        if not isinstance(raw, list) or not all(isinstance(level, str) for level in raw):
            raise ValueError(f"path {number} is not a list of labels")
        if len(raw) > CLASSIFIER_DEPTH:
            raise ValueError(f"path {number} is deeper than {CLASSIFIER_DEPTH} levels")
        levels = classifier_levels(raw)
        if not levels:
            raise ValueError(f"path {number} is empty")
        if not all(levels):
            # В файле владельца пустых уровней внутри пути нет; пустое место здесь —
            # ошибка правки файла, а не классификация.
            raise ValueError(f"path {number} has an empty level inside: {raw}")
        paths.append(levels)
    return paths


def validate_reasons(data: object) -> list[CallReason]:
    """Прочитать коды и убедиться, что справочник годен, до того как он уедет в таблицу."""
    raw = data.get("reasons") if isinstance(data, dict) else None
    if not isinstance(raw, list) or not raw:
        raise ValueError("reasons: no reasons")

    reasons: list[CallReason] = []
    for number, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"reason {number} is not an object")
        code = str(item.get("code") or "")
        label = _normalised(item.get("label"))
        if not CODE_PATTERN.fullmatch(code):
            raise ValueError(f"reason {number}: code {code!r} is not K and two digits")
        if not label or len(label) > LABEL_LIMIT:
            raise ValueError(f"reason {code}: the label is empty or longer than {LABEL_LIMIT}")
        reasons.append(CallReason(code=code, label=label, hint=_normalised(item.get("hint"))))
    codes = [reason.code for reason in reasons]
    if len(set(codes)) != len(codes):
        raise ValueError("reasons: a code is listed twice")
    size = len(prompt_reasons(reasons))
    if size > REASONS_PROMPT_LIMIT:
        raise ValueError(
            f"reasons: the reference is {size} characters, the workflow accepts "
            f"{REASONS_PROMPT_LIMIT} — shorten the hints"
        )
    return reasons
