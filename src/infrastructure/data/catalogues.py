"""Чтение версионируемых JSON-справочников с проверкой через приложение."""

import json
from pathlib import Path

from src.application.catalogues.validation import validate_classifier, validate_reasons
from src.application.dto.catalogues import CallReason

CLASSIFIER_FILE = Path(__file__).with_name("classifier.json")
REASONS_FILE = Path(__file__).with_name("call_reasons.json")


def load_classifier(path: Path = CLASSIFIER_FILE) -> list[tuple[str, ...]]:
    """Прочитать JSON по пути и вернуть проверенные пути дерева; ошибки файла — ValueError."""
    return validate_classifier(json.loads(path.read_text(encoding="utf-8")))


def load_reasons(path: Path = REASONS_FILE) -> list[CallReason]:
    """Прочитать JSON по пути и вернуть проверенные коды причин; ошибки файла — ValueError."""
    return validate_reasons(json.loads(path.read_text(encoding="utf-8")))
