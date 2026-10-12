"""Границы слоёв: нарушение зависимости должно ломать проверки сразу."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.domain.retention.policy import RetentionPolicy

SOURCE = Path(__file__).resolve().parents[1] / "src"
ALLOWED = {
    "domain": {"domain"},
    "application": {"application", "domain"},
    "infrastructure": {"infrastructure", "application", "setting"},
    "presentation": {"presentation", "application"},
    "setting": {"setting"},
    "di": {"di", "presentation", "application", "infrastructure", "domain", "setting"},
}
IO_LIBRARIES = {"sqlalchemy", "asyncpg", "httpx", "fastapi", "openpyxl", "subprocess", "tempfile"}


def test_layers_import_only_their_declared_dependencies() -> None:
    """Проверить все импорты, включая локальные и относительные, без запуска модулей."""
    violations: list[str] = []
    for path in SOURCE.rglob("*.py"):
        relative = path.relative_to(SOURCE)
        if len(relative.parts) < 2:
            continue
        layer = relative.parts[0]
        tree = ast.parse(path.read_text(encoding="utf-8"))
        parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
        for node in ast.walk(tree):
            imports: list[str]
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if node.level:
                    package = ["src", *relative.parts[:-1]]
                    module = ".".join(package[: len(package) - node.level + 1] + module.split("."))
                imports = [module]
            elif isinstance(node, ast.Import):
                imports = [alias.name for alias in node.names]
            else:
                continue
            for imported in imports:
                if imported.startswith("src."):
                    dependency = imported.split(".")[1]
                    if (
                        dependency == "di"
                        and layer == "presentation"
                        and relative.parts[1] == "cli"
                    ):
                        parent = parents.get(node)
                        while parent is not None and not isinstance(parent, ast.FunctionDef):
                            parent = parents.get(parent)
                        if isinstance(parent, ast.FunctionDef) and parent.name == "main":
                            continue
                    if dependency not in ALLOWED[layer]:
                        violations.append(f"{relative}:{node.lineno}: {layer} imports {imported}")
                elif imported == "app" or imported.startswith("app."):
                    violations.append(f"{relative}:{node.lineno}: legacy import {imported}")
                elif layer in {"domain", "application"} and imported.split(".")[0] in IO_LIBRARIES:
                    violations.append(f"{relative}:{node.lineno}: IO library {imported} in {layer}")
    assert violations == [], "\n".join(violations)


@pytest.mark.parametrize("days", [0, -1])
def test_retention_cannot_accept_a_window_that_deletes_live_rows(days: int) -> None:
    """Pydantic-модель обязана применять ограничение срока при создании политики."""
    with pytest.raises(ValidationError, match="greater_than_equal"):
        RetentionPolicy(
            table="speech_jobs", completion_column="finished_at", days=days, reason="test"
        )
