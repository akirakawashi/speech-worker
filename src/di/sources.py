"""Скачиватели записей по источнику технической команды бэкенда."""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.domain.calls.contracts import CallSource
from src.infrastructure.sources.routing import RoutingSource as RoutingSource

if TYPE_CHECKING:  # pragma: no cover
    from src.setting.config import Settings


def build_worker_source(settings: Settings) -> RoutingSource:
    """Источник воркера: записи NCC и Телфина.

    NCC обязателен, как и прежде: ненастроенный источник останавливает воркер
    на старте, а не выясняется на первом задании ночью. Телфину для записей
    настройки не нужны — ссылка хранилища приходит со звонком.
    """
    from src.infrastructure.sources.ncc.source import build_recording_source  # noqa: PLC0415
    from src.infrastructure.sources.telphin.source import build_telphin_audio  # noqa: PLC0415

    return RoutingSource(
        {
            CallSource.NCC.value: build_recording_source(settings),
            CallSource.TELPHIN.value: build_telphin_audio(settings),
        }
    )
