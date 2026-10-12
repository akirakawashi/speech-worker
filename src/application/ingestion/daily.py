"""Приём свежих звонков с дневной долей, очередностью и догоном истории."""

from __future__ import annotations

import random
import uuid
from datetime import UTC, date, datetime, time, timedelta

from src.application.dto.backfill import BackfillNotStarted
from src.application.dto.calls import CallSource, JobKind, JobLane
from src.application.dto.daily_ingest import DEFERRED_PRIORITY_STEP, DailyRunReport, DayShare
from src.application.ports.daily_ingest import BackfillRepository, DailyIngestRepository
from src.application.ports.queue import NOT_BEFORE_KEY, NewJob
from src.application.ports.sources import DailyBatchSource


def cursor_source(call_source: str) -> str:
    """По источнику вернуть ключ его независимого суточного курсора."""
    return f"{call_source}_daily"


async def run_day(
    day: date,
    *,
    source: DailyBatchSource,
    repository: DailyIngestRepository,
    lane: JobLane = JobLane.DAILY,
    limit: int | None = None,
    call_source: str = CallSource.NCC.value,
    day_share: DayShare | None = None,
    priority: int | None = None,
    now: datetime | None = None,
    rng: random.Random | None = None,
) -> DailyRunReport:
    """Забрать звонки за дату и поставить их в очередь.

    Весь прогон — одна транзакция. Так дата либо обработана целиком, либо не
    обработана вовсе: полторы тысячи вставок — это секунды, а частично
    заведённые сутки пришлось бы отличать от полных, чего никто делать не
    станет.

    `limit` берёт первые звонки в порядке источника. Если он отсёк хоть один,
    дата в курсор не записывается: заведена часть суток, а не сутки.

    `call_source` — чей это прогон: у каждого источника своя блокировка и свой
    курсор, и прогон Телфина не ждёт прогона NCC и не сдвигает его сутки.

    `day_share` — дневная доля: днём новый звонок идёт в обработку сразу с
    вероятностью `share`, иначе ждёт начала ночи. Жребий бросается один раз —
    когда звонок заводится: повторный прогон его не перебрасывает, заведённое
    задание он не трогает. `priority` — приоритет заданий прогона; `None` — по
    полосе. `now` и `rng` подставляют тесты.
    """
    refs = await source.list_calls(day)
    limited = limit is not None and limit < len(refs)
    selected = refs[:limit] if limited else refs
    async with repository.transaction() as transaction:
        if not await transaction.try_lock(call_source):
            return DailyRunReport(
                day=day,
                discovered=0,
                enqueued=0,
                already_known=0,
                skipped=True,
                skip_reason="другой суточный прогон уже идёт",
            )
        configured = await transaction.settings()
        max_attempts = configured.max_attempts
        directory = transaction
        latest_call: dict[uuid.UUID, datetime] = {}
        enqueued = 0
        already_known = 0
        collection_off = 0
        deferred = 0
        night = day_share.night_start(now or datetime.now(UTC)) if day_share else None
        draw = rng or random.Random()
        for ref in selected:
            project = await directory.project(ref.project_key, ref.project_hint, ref.direction)
            if project is not None and (not project.ingest_enabled):
                collection_off += 1
                continue
            project_id = project.id if project is not None else None
            operator_id = await directory.operator(ref.operator_login, ref.operator_name)
            operator_node_id = await directory.classification(ref.disposition_path)
            registered = await transaction.upsert(
                ref,
                operator_id=operator_id,
                project_id=project_id,
                operator_node_id=operator_node_id,
            )
            call_id, is_new = registered.call_id, registered.is_new
            if not is_new:
                already_known += 1
            if project_id is not None and ref.started_at is not None:
                seen = latest_call.get(project_id)
                if seen is None or ref.started_at > seen:
                    latest_call[project_id] = ref.started_at
            later = (
                day_share is not None and night is not None and (draw.random() >= day_share.share)
            )
            job_id = await transaction.enqueue(
                NewJob(
                    call_id=call_id,
                    kind=JobKind.INGEST,
                    lane=lane,
                    max_attempts=max_attempts[JobKind.INGEST.value],
                    run_after=night if later else None,
                    payload={NOT_BEFORE_KEY: night.isoformat()} if later and night else {},
                    priority=(priority or 0) + DEFERRED_PRIORITY_STEP if later else priority,
                )
            )
            if job_id is not None:
                enqueued += 1
                deferred += later
        for project_id, moment in latest_call.items():
            await transaction.touch_project(project_id, moment)
        if not limited:
            await transaction.record_day(day, cursor_source(call_source))
    return DailyRunReport(
        day=day,
        discovered=len(refs),
        enqueued=enqueued,
        already_known=already_known,
        limited_to=len(selected) if limited else None,
        collection_off=collection_off,
        classifier_created=directory.classifier_created,
        deferred=deferred,
    )


async def catch_up(
    *,
    source: DailyBatchSource,
    repository: DailyIngestRepository,
    call_source: str,
    today: date,
    history_from: date | None,
    history_days: int,
    day_share: DayShare | None = None,
    priority: int | None = None,
    history: BackfillRepository,
) -> list[str]:
    """Прогон таймера: вчера и сегодня, потом порция истории.

    История — тем же механизмом, что у NCC (`src.presentation.cli.backfill`), своей
    строкой состояния: окно открывается само при первом запуске, порция — не
    больше `history_days` суток за запуск, полосой `backfill`. Так свежие
    звонки не ждут истории, а история не требует ручного запуска.

    Дневная доля и приоритет относятся к вчера и сегодня. История идёт своей
    полосой и уступает всему и так.
    """
    from src.application.ingestion.backfill import (
        backfill_cursor,
        run_portion,
        start,
    )

    lines: list[str] = []
    for day in (today - timedelta(days=1), today):
        report = await run_day(
            day,
            source=source,
            repository=repository,
            call_source=call_source,
            day_share=day_share,
            priority=priority,
        )
        lines.append(report.describe())
    if history_from is None or history_from >= today:
        return lines
    cursor = backfill_cursor(call_source)
    try:
        await history.read_state(cursor)
    except BackfillNotStarted:
        opened = await start(
            history,
            window_days=(today - history_from).days,
            now=datetime.combine(today, time.min, tzinfo=UTC),
            cursor=cursor,
        )
        lines.append(f"история: окно открыто — {opened.describe()}")
    portion = await run_portion(
        source=source,
        repository=history,
        ingest_repository=repository,
        days=history_days,
        cursor=cursor,
        call_source=call_source,
    )
    lines.append(f"история: {portion.describe()}")
    return lines
