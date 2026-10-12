"""Открытие окна, пауза и обработка истории порциями с сохранением прогресса."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.application.dto.backfill import BackfillNotStarted, BackfillState, PortionReport
from src.application.dto.calls import JobLane
from src.application.dto.daily_ingest import DailyRunReport
from src.application.ingestion.daily import run_day
from src.application.ports.daily_ingest import BackfillRepository, DailyIngestRepository
from src.application.ports.sources import DailyBatchSource

CURSOR_SOURCE = "ncc_backfill"


def backfill_cursor(call_source: str) -> str:
    """Строка состояния разбора истории источника: `ncc_backfill`, `telphin_backfill`."""
    return f"{call_source}_backfill"


def _midnight(moment: datetime) -> datetime:
    """Полночь тех же суток в UTC.

    Окно выравнивается по суткам, потому что источник умеет только сутки:
    `list_calls` принимает дату. Курсор в середине дня означал бы, что половина
    суток обработана, а половина нет, — а различить это нечем.
    """
    return moment.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


async def start(
    repository: BackfillRepository,
    *,
    window_days: int,
    now: datetime | None = None,
    restart: bool = False,
    cursor: str = CURSOR_SOURCE,
) -> BackfillState:
    """Открыть окно и поставить курсор на его свежий край.

    Повторный запуск на незаконченном окне отклоняется. Разбор истории идёт
    сутками и неделями; «запустить ещё раз», не заметив, что он уже идёт,
    означало бы начать сначала и пройти то же самое второй раз. Дубликатов от
    этого не будет — идемпотентность держится на ключах, — но время будет
    потрачено, а прогресс потерян.
    """
    if window_days < 1:
        raise ValueError("window_days must be positive")
    if not restart:
        current: BackfillState | None
        try:
            current = await repository.read_state(cursor)
        except BackfillNotStarted:
            current = None
        if current is not None and (not current.done):
            raise RuntimeError(
                "разбор истории уже идёт ("
                f"{current.describe()}"
                "). Чтобы начать окно заново, добавьте --restart"
            )
    until = _midnight(now or datetime.now(UTC))
    window_from = until - timedelta(days=window_days)
    return await repository.open_window(cursor, window_from, until)


async def set_paused(repository: BackfillRepository, *, paused: bool) -> BackfillState:
    """Приостановить или продолжить перечисление дат — со своим сеансом.

    Пауза не отменяет уже поставленных заданий — они доработают. Отменить их
    значило бы выбросить сделанную работу: скачанная запись и расшифровка
    остаются полезными независимо от того, продолжается разбор истории или нет.
    """
    return await repository.pause(paused)


async def run_portion(
    *,
    source: DailyBatchSource,
    repository: BackfillRepository,
    days: int = 1,
    cursor: str = CURSOR_SOURCE,
    call_source: str = "ncc",
    ingest_repository: DailyIngestRepository,
) -> PortionReport:
    """Обработать несколько суток и сдвинуть курсор.

    Порция, а не всё окно за раз: шестьдесят суток по полторы тысячи звонков —
    это полтора часа перечисления, в течение которых прогон нельзя ни
    остановить, ни увидеть. Порциями пауза срабатывает между сутками, а
    состояние в админке меняется на глазах.
    """
    if days < 1:
        raise ValueError("days must be positive")
    reports: list[DailyRunReport] = []
    state = await repository.read_state(cursor)
    for _ in range(days):
        if state.done:
            return PortionReport(days=tuple(reports), state=state, stopped_because=state.describe())
        if state.paused:
            return PortionReport(days=tuple(reports), state=state, stopped_because=state.describe())
        day = state.next_day
        if day is None:
            state = await _finish(repository, state, cursor=cursor)
            break
        report = await run_day(
            day,
            source=source,
            repository=ingest_repository,
            lane=JobLane.BACKFILL,
            call_source=call_source,
        )
        if report.skipped:
            return PortionReport(
                days=tuple(reports), state=state, stopped_because=f"{day}: {report.skip_reason}"
            )
        reports.append(report)
        state = await _advance(
            repository, state, to=state.cursor - timedelta(days=1), cursor=cursor
        )
    return PortionReport(days=tuple(reports), state=state)


async def _advance(
    repository: BackfillRepository,
    state: BackfillState,
    *,
    to: datetime,
    cursor: str = CURSOR_SOURCE,
) -> BackfillState:
    """Сохранить прогресс окна истории после завершённых дней."""
    done = to <= state.window_from
    return await repository.advance(cursor, to, done)


async def _finish(
    repository: BackfillRepository, state: BackfillState, *, cursor: str = CURSOR_SOURCE
) -> BackfillState:
    """Пометить окно истории завершённым после обработки последнего дня."""
    return await repository.advance(cursor, state.window_from, True)
