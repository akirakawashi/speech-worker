"""Record which tables a process writes, without recording what it wrote.

the ownership matrix asks for the table/writer matrix to be confirmed by runtime traces, not
only by static search -- static search cannot see raw SQL or a write reached
through a dynamically built statement.

The obvious tool, SQLAlchemy's ``echo=True``, is the wrong one here: it logs
bound parameters, so switching it on to answer an ownership question would put
message bodies and credentials into the log, which is exactly what gate G1
forbids. This listener extracts the verb and the target table from the statement
text and logs nothing else -- no parameters, no predicates, no values.

Off unless ``DB_WRITE_TRACE_ENABLED`` is set. Intended for an acceptance run or
a debugging session, not for steady operation.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from sqlalchemy import event
from sqlalchemy.engine import Engine

logger = logging.getLogger("src.infrastructure.persistence.write_trace")

# `insert into "messages" (...)`, `update agents set ...`, `delete from users`.
# Quoted and schema-qualified names both appear; only the final identifier is
# kept, since that is the table.
_WRITE = re.compile(
    r"""^\s*
    (?P<verb>insert\s+into|update|delete\s+from)
    \s+
    (?P<target>[\w".]+)
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Reads matter as much as writes for a least-privilege grant, and are harder to
# find by reading code: a module belonging to one service is routinely imported
# and executed by another, so its `select()` runs under a role the module path
# does not name. `workflow-engine-ms` reading `agents` through a helper that
# lives in the backend package is exactly that case, and it only surfaced when
# the switched-over service was refused at runtime.
#
# Every table mentioned after FROM or JOIN is recorded; subqueries and CTEs are
# covered because their own FROM clauses appear in the same text.
_READ = re.compile(r"\b(?:from|join)\s+(?P<target>[\w\".]+)", re.IGNORECASE)


def _clean(target: str) -> str:
    """Подготовить безопасное представление метаданных SQL-записи."""
    return target.replace('"', "").rsplit(".", 1)[-1]


def _target_table(statement: str) -> tuple[str, str] | None:
    """Извлечь имя таблицы изменяющего SQL-запроса для трассировки."""
    match = _WRITE.match(statement)
    if match is None:
        return None
    verb = " ".join(match.group("verb").split()).upper()
    return verb.split()[0], _clean(match.group("target"))


def _read_tables(statement: str) -> list[str]:
    """Tables a statement reads. Empty for anything that is not a read."""
    if not statement.lstrip()[:6].lower().startswith(("select", "with")):
        return []
    seen: list[str] = []
    for match in _READ.finditer(statement):
        table = _clean(match.group("target"))
        # `FROM (SELECT ...)` and similar leave punctuation behind.
        if table.isidentifier() and table not in seen:
            seen.append(table)
    return seen


def _ensure_logger_emits() -> None:
    """Make the recorder's own logger deliver, whatever the host set up.

    Uvicorn leaves the root logger at WARNING, so an `INFO` line from a library
    logger is discarded. Left alone, switching tracing on would produce silence
    and read as "no writes" -- a switch that quietly does nothing is worse than
    no switch. Only this logger is touched, and only when tracing is on.
    """
    if any(getattr(h, "_write_trace", False) for h in logger.handlers):
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    handler._write_trace = True  # type: ignore[attr-defined]
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    # The host's root configuration must not be able to swallow these lines,
    # and they must not be duplicated into it either.
    logger.propagate = False


def install_write_trace(engine: Engine | Any, *, label: str) -> None:
    """Attach the recorder to ``engine``, tagging every line with ``label``.

    ``label`` names the connection the write went through -- the shared backend
    engine, or one of the per-service ones -- which is the half of the ownership
    question that the table name alone does not answer.
    """
    _ensure_logger_emits()
    target = getattr(engine, "sync_engine", engine)

    @event.listens_for(target, "before_cursor_execute")
    def _record(  # type: ignore[no-untyped-def]
        _conn, _cursor, statement, _parameters, _context, _executemany
    ):
        """Записать метаданные SQL-операции без значений персональных полей."""
        parsed = _target_table(statement)
        if parsed is not None:
            verb, table = parsed
            # Deliberately formatted as flat key=value so an acceptance run can
            # count writers per table with a grep.
            logger.info("db_write engine=%s op=%s table=%s", label, verb, table)
            return
        for table in _read_tables(statement):
            logger.info("db_write engine=%s op=SELECT table=%s", label, table)
