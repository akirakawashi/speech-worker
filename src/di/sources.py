"""Источники звонков вместе: какой источник чем собирать (фича 14, 7.1).

Приём суток работает с одним источником за прогон (`daily_ingest --source`),
а воркер качает записи звонков всех источников сразу — и выбирает скачиватель
по источнику звонка.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.application.ports.sources import DailyBatchSource
from src.domain.calls.contracts import CallSource
from src.infrastructure.sources.routing import RoutingSource as RoutingSource

if TYPE_CHECKING:  # pragma: no cover
    from src.setting.config import Settings


def build_ingest_source(call_source: str, settings: Settings) -> DailyBatchSource:
    """Источник суточного прогона — с перечислением звонков."""
    if call_source == CallSource.TELPHIN:
        from src.infrastructure.sources.telphin.source import build_telphin_source  # noqa: PLC0415

        return build_telphin_source(settings)
    from src.infrastructure.sources.ncc.source import build_daily_batch_source  # noqa: PLC0415

    return build_daily_batch_source(settings)


def build_worker_source(settings: Settings) -> RoutingSource:
    """Источник воркера: записи NCC и Телфина.

    NCC обязателен, как и прежде: ненастроенный источник останавливает воркер
    на старте, а не выясняется на первом задании ночью. Телфину для записей
    настройки не нужны — ссылка хранилища приходит со звонком.
    """
    from src.infrastructure.sources.ncc.source import build_daily_batch_source  # noqa: PLC0415
    from src.infrastructure.sources.telphin.source import build_telphin_audio  # noqa: PLC0415

    return RoutingSource(
        {
            CallSource.NCC.value: build_daily_batch_source(settings),
            CallSource.TELPHIN.value: build_telphin_audio(settings),
        }
    )
