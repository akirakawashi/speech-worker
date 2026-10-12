"""Источник суточного пакета: представления отчётной базы NCC.

Список звонков берётся из представлений в отчётной базе поставщика, запись —
тем же REST, что и у остальных источников (`callrecord/download`). Вариант
предложил поставщик после того, как перечисление сессий через API было
опровергнуто (SP-001a); доступ на чтение выдан, решение владельца 2026-09-15.

Каждое правило ниже снято с живой базы 2026-09-15 (журнал разработки,
«SP-001a: представление поставщика»):

* `speakingtime` — секунды с долями: медиана 150.54, ни одной строки от 10000.
  Порог длительности поэтому в секундах;
* время — `timestamp without time zone` по часам сервера (`Europe/Moscow`):
  окно суток строится в этом поясе, и к нему же привязываются моменты;
* без условия по `enqueued_time` представление не читается и за 120 секунд —
  запрос всегда оконный;
* перед базой пулер: он отвергает параметры запуска и не держит подготовленных
  запросов. Кэш запросов выключен, «только чтение» задаётся транзакцией;
* TLS у сервера нет. Соединение по умолчанию пробует шифрование и, получив
  отказ, идёт открытым текстом; включит поставщик TLS — оно будет использовано
  без правки кода. Отсутствие TLS принято владельцем;
* в представлениях есть ПДн — `abonent`, `transfer_number`, `kommentarii`. Они
  не читаются: запрос называет только нужные поля;
* `session_id` бывает повторён в пределах суток (5 строк из 1514): порт
  запрещает дубли, повтор сворачивается;
* уровни классификации `uroven_1…5` заполнены не у всех: у «Кошелька» оператор
  оставляет пустым первый. Путь берётся по местам уровней — пустой в середине
  остаётся, иначе уровни сдвинулись бы (SP-310, 2026-09-15).
"""

from __future__ import annotations

import logging
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Final
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict

from src.infrastructure.persistence.classifier import classifier_levels
from src.infrastructure.sources.ncc.source import SESSION_ID_RE, CallRef

if TYPE_CHECKING:
    from src.infrastructure.sources.ncc.rest import NccRestClient

logger = logging.getLogger(__name__)

#: Выборка: текст запроса и позиционные параметры -> строки.
Fetch = Callable[[str, Sequence[object]], Awaitable[Sequence[Mapping[str, Any]]]]

#: Имя представления подставляется в текст запроса — параметром его не передать.
#: Поэтому только простое имя в нижнем регистре, и то в кавычках.
VIEW_NAME_RE: Final = re.compile(r"^[a-z_][a-z0-9_]*$")

#: Уровни классификации обращения, по порядку.
DISPOSITION_COLUMNS: Final = ("uroven_1", "uroven_2", "uroven_3", "uroven_4", "uroven_5")


class ReportingNotConfigured(RuntimeError):
    """Источник `reporting_view` настроен не полностью или с ошибкой."""


class ReportingView(BaseModel):
    """Представление со звонками и проект, которому они принадлежат."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    name: str
    project: str


def parse_views(raw: str) -> tuple[ReportingView, ...]:
    """`calls_detail=Волна,tss_7tel_calls_detail=7Телеком` -> представления.

    Имя проекта обязательно: оно попадает в карточку звонка, а поле проекта в
    самом представлении пустое у всех строк (проверено 2026-09-15) — без имени
    звонки двух проектов неразличимы.
    """
    views: list[ReportingView] = []
    seen: set[str] = set()
    for item in raw.split(","):
        entry = item.strip()
        if not entry:
            continue
        name, separator, project = entry.partition("=")
        name, project = name.strip(), project.strip()
        if not VIEW_NAME_RE.match(name):
            raise ReportingNotConfigured(
                f"SPEECH_NCC_REPORTING_VIEWS: {name!r} is not a plain view name"
            )
        if not separator or not project:
            raise ReportingNotConfigured(
                f"SPEECH_NCC_REPORTING_VIEWS: view {name!r} needs a project, as {name}=<project>"
            )
        if name in seen:
            raise ReportingNotConfigured(
                f"SPEECH_NCC_REPORTING_VIEWS: view {name!r} is listed twice"
            )
        seen.add(name)
        views.append(ReportingView(name=name, project=project))
    return tuple(views)


def _checked_name(view: ReportingView) -> str:
    """Проверить разрешённое имя представления до подстановки в SQL."""
    if not VIEW_NAME_RE.match(view.name):
        raise ReportingNotConfigured(f"{view.name!r} is not a plain view name")
    return view.name


def build_query(view: ReportingView) -> str:
    """Запрос звонков одного представления за окно.

    Поля названы поимённо, а не `*`: звёздочка потянула бы телефоны абонента и
    перевода и свободный комментарий оператора — ПДн, которые приёму не нужны.
    """
    name = _checked_name(view)
    fields = ", ".join(
        ("session_id", "enqueued_time", "nextended", "speakingtime", "nextid", *DISPOSITION_COLUMNS)
    )
    return (
        f'SELECT {fields} FROM public."{name}"'
        " WHERE enqueued_time >= $1 AND enqueued_time < $2 AND speakingtime >= $3"
        " ORDER BY enqueued_time, session_id"
    )


def build_classification_query(view: ReportingView) -> str:
    """Классификации операторов одного представления за окно — со счётом звонков.

    Только уровни и число, ни одного поля звонка: разовому пополнению
    классификатора (SP-310) звонки не нужны. Порога длительности нет —
    классификатор пополняется всем, что ставят операторы.
    """
    name = _checked_name(view)
    levels = ", ".join(DISPOSITION_COLUMNS)
    return (
        f'SELECT {levels}, count(*) AS calls FROM public."{name}"'
        " WHERE enqueued_time >= $1 AND enqueued_time < $2"
        f" GROUP BY {levels}"
    )


def _text(value: object) -> str | None:
    """Прочитать текстовое значение и нормализовать отсутствие."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _levels(row: Mapping[str, Any]) -> tuple[str, ...]:
    """Прочитать уровни классификации, сохранив пустые места внутри пути."""
    return classifier_levels(row.get(column) for column in DISPOSITION_COLUMNS)


class ReportingViewSource:
    """Звонки за сутки — из представлений отчётной базы, запись — из REST."""

    name = "ncc_reporting_view"

    def __init__(
        self,
        *,
        fetch: Fetch,
        client: NccRestClient,
        views: Sequence[ReportingView],
        timezone: str,
        min_talk_seconds: int,
    ) -> None:
        """Настроить ReportingViewSource из переданных параметров и зависимостей."""
        if not views:
            raise ReportingNotConfigured(
                "SPEECH_NCC_SOURCE_KIND=reporting_view requires SPEECH_NCC_REPORTING_VIEWS, "
                "as <view>=<project>"
            )
        self._fetch = fetch
        self._client = client
        self._views = tuple(views)
        self._tz = ZoneInfo(timezone)
        self._min_talk = Decimal(min_talk_seconds)

    async def list_calls(self, day: date) -> Sequence[CallRef]:
        """Звонки всех представлений за сутки по часам представления.

        Сбой базы поднимается исключением как есть: пустой список означает
        выходной день, и путать его с недоступной базой нельзя.
        """
        start, end = self._window(day)
        refs: dict[str, CallRef] = {}
        repeated = 0
        malformed = 0
        for view in self._views:
            rows = await self._fetch(build_query(view), (start, end, self._min_talk))
            for row in rows:
                session_id = _text(row.get("session_id")) or ""
                if not SESSION_ID_RE.match(session_id):
                    malformed += 1
                    continue
                if session_id in refs:
                    repeated += 1
                    continue
                refs[session_id] = self._ref(row, session_id=session_id, day=day, view=view)
        if repeated or malformed:
            logger.info(
                "reporting views, %s: %d calls; %d repeated rows folded, "
                "%d rows without a valid session id skipped",
                day.isoformat(),
                len(refs),
                repeated,
                malformed,
            )
        return tuple(refs.values())

    async def list_classifications(self, day: date) -> dict[tuple[str, ...], int]:
        """Пути классификации всех представлений за сутки и сколько звонков с каждым.

        Путь, записанный по-разному, остаётся здесь двумя строками: одним узлом
        его сделает дерево, по ключу (`src.infrastructure.persistence.classifier`).
        """
        start, end = self._window(day)
        counts: dict[tuple[str, ...], int] = {}
        for view in self._views:
            for row in await self._fetch(build_classification_query(view), (start, end)):
                levels = _levels(row)
                if levels:
                    counts[levels] = counts.get(levels, 0) + int(row.get("calls") or 0)
        return counts

    @staticmethod
    def _window(day: date) -> tuple[datetime, datetime]:
        """Преобразовать календарный день в границы выборки источника."""
        start = datetime.combine(day, time.min)
        return start, start + timedelta(days=1)

    def _ref(
        self, row: Mapping[str, Any], *, session_id: str, day: date, view: ReportingView
    ) -> CallRef:
        """Преобразовать строку источника в локатор и метаданные звонка."""
        talk = row.get("speakingtime")
        return CallRef(
            session_id=session_id,
            # Локатор и есть идентификатор: скачивание идёт по нему.
            audio_locator=session_id,
            day=day,
            started_at=self._localise(row.get("enqueued_time")),
            ended_at=self._localise(row.get("nextended")),
            duration_s=round(float(talk)) if talk is not None else None,
            operator_login=_text(row.get("nextid")),
            project_hint=view.project,
            # Ключ — имя представления, а не название из настройки: название
            # уточнится по ходу, и проект от этого раздваиваться не должен.
            project_key=f"reporting_view:{view.name}",
            disposition_path=_levels(row),
        )

    def _localise(self, moment: object) -> datetime | None:
        """Привести дату источника к явному часовому поясу."""
        if not isinstance(moment, datetime):
            return None
        if moment.tzinfo is None:
            return moment.replace(tzinfo=self._tz)
        return moment.astimezone(self._tz)

    async def open_audio(self, ref: CallRef) -> AsyncIterator[bytes]:
        """Запись по идентификатору сессии, тем же REST, что у `sequence`.

        `RecordingNotAvailable` из клиента проходит наверх как есть: отличать
        «записи нет» от «источник сломан» — обязанность этапа скачивания.
        """
        return self._client.download(ref.session_id)


def asyncpg_fetcher(
    dsn: str,
    *,
    password: str,
    connect_timeout_seconds: float = 15.0,
    statement_timeout_seconds: int = 120,
) -> Fetch:
    """Выборка через asyncpg: соединение на запрос, транзакция только на чтение.

    Соединение на запрос, а не пул: запросов по одному на представление в сутки,
    и держать открытым соединение с чужой базой через интернет между вечерними
    прогонами незачем.
    """

    async def fetch(query: str, args: Sequence[object]) -> Sequence[Mapping[str, Any]]:
        """Прочитать данные отчётного источника за указанный день."""
        import asyncpg  # type: ignore[import-untyped]  # noqa: PLC0415

        connection = await asyncpg.connect(
            dsn=dsn,
            password=password,
            timeout=connect_timeout_seconds,
            # Пулер перед базой не держит подготовленных запросов.
            statement_cache_size=0,
        )
        try:
            async with connection.transaction(readonly=True):
                await connection.execute(
                    f"SET LOCAL statement_timeout = '{int(statement_timeout_seconds)}s'"
                )
                rows: Sequence[Mapping[str, Any]] = await connection.fetch(query, *args)
        finally:
            await connection.close()
        return rows

    return fetch
