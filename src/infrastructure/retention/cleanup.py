"""Count and delete rows past their retention window: the two SQL primitives.

The command that drives them is `python -m src.presentation.cli.prune`. In the monorepo
this module was also the entry point that pruned the main application's
database; only the two functions Speech Analytics calls were carried over.

Two properties the implementation exists for:

**Pending work is never touched.** Every policy names the column that proves the
row's work is finished, and the delete is conditioned on that column being NOT
NULL. A row whose work was never finished is not old, it is late.

**Deletes are batched.** This runs against a cluster shared with LLM. A single
`DELETE` over a year of rows holds locks for as long as it takes; a bounded
batch, repeated, gives every other user of the cluster a gap to move in.
"""

from __future__ import annotations

from typing import Any, cast

from sqlalchemy import CursorResult, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.dto.retention import RetentionPolicy


async def count_expired(session: AsyncSession, policy: RetentionPolicy) -> int:
    """Посчитать завершённые строки, чей срок хранения истёк."""
    statement = text(
        f"SELECT count(*) FROM {policy.table} "  # noqa: S608 - table names are declared, not input
        f"WHERE {policy.completion_column} IS NOT NULL "
        f"AND {policy.completion_column} < now() - make_interval(days => :days)"
    )
    result = await session.execute(statement, {"days": policy.days})
    return int(result.scalar_one())


async def prune(session: AsyncSession, policy: RetentionPolicy, *, batch_size: int) -> int:
    """Delete in bounded batches until nothing expired remains."""
    statement = text(
        f"DELETE FROM {policy.table} WHERE ctid IN ("  # noqa: S608 - see above
        f"  SELECT ctid FROM {policy.table}"
        f"  WHERE {policy.completion_column} IS NOT NULL"
        f"    AND {policy.completion_column} < now() - make_interval(days => :days)"
        f"  LIMIT :batch"
        f")"
    )
    removed = 0
    while True:
        # `execute` is typed as returning `Result`, which has no `rowcount`; a
        # DML statement actually returns a `CursorResult`, which does.
        result = cast(
            "CursorResult[Any]",
            await session.execute(statement, {"days": policy.days, "batch": batch_size}),
        )
        await session.commit()
        if not result.rowcount:
            return removed
        removed += result.rowcount
