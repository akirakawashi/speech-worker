"""Откуда берётся суточный пакет звонков.

Перечисления звонков в реальном времени у NCC нет: REST API отдаёт запись
только по уже известному идентификатору сессии, а списка звонков в нём нет
вовсе — проверены все 194 эндпойнта руководства. Поэтому сбор суточный
(решение S-19), а **способ доставки пакета скрыт за портом**: он уже менялся
один раз и, судя по всему, поменяется ещё, когда появится доступ к объектному
хранилищу.

Порт умеет две вещи — перечислить звонки за дату и отдать байты одного
аудиофайла. Вторая здесь, а не в отдельном клиенте, потому что источник знает,
как достать своё: локальная выгрузка читает файл с диска, хранилище пойдёт в
S3, REST — за `sessionId`. Разделив эти две операции по разным модулям, мы бы
получили пары «этот список сочетается с этим скачивателем», которые нигде не
записаны.

Реализации:

* `LocalExportSource` — каталог с выгрузкой, полученной от колл-центра. Работает
  сейчас и на настоящих записях.
* хранилище объектов и REST — когда появится доступ.
"""

from __future__ import annotations

import logging
import re
import urllib.parse
from collections.abc import AsyncIterator, Iterable, Sequence
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final
from zoneinfo import ZoneInfo

from src.application.dto.sources import (
    CallRef as CallRef,
)
from src.application.errors.external import (
    RecordingNotAvailable as RecordingNotAvailable,
)
from src.application.ports.sources import (
    DailyBatchSource as DailyBatchSource,
)
from src.infrastructure.persistence.classifier import PATH_SEPARATOR, classifier_levels

logger = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma: no cover - только для типов, цикла импорта нет
    # `rest` импортирует этот модуль, поэтому во время выполнения импорт назад
    # замкнул бы круг. Под проверкой типов круга нет.
    from src.infrastructure.sources.ncc.rest import NccRestClient
    from src.setting.config import Settings

#: Известные виды источников. `storage` не реализован, `rest` опровергнут; оба
#: названы здесь, чтобы опечатка в настройке была отказом старта, а не молчаливым
#: выбором первого попавшегося. `reporting_view` — представления отчётной базы
#: NCC, основной источник с 2026-09-15.
SOURCE_KINDS: Final[frozenset[str]] = frozenset(
    {"local_export", "storage", "rest", "sequence", "reporting_view"}
)


#: Ключ объекта в хранилище NCC: `ncc-tm/ГГГГ/ММ/ДД/<sessionId>`. Известен из
#: имён файлов переданной выгрузки — они и есть URL-кодированные ключи.
OBJECT_KEY_RE = re.compile(
    "(?P<prefix>[\\w.-"
    "]+)/(?P<year>\\d{4})/(?P<month>\\d{2})/(?P<day>\\d{2})/(?P<session_i"
    "d>[\\w.-]+?)(?:\\.mp3)?$"
)

#: Формат идентификатора сессии NCC, например
#: `node_1_domain_2_nauss_0_1766092829_1138986`. Проверяется, чтобы каталог с
#: посторонними файлами не заводил в базу мусор под видом звонков.
#: Форма идентификатора сессии NCC.
#:
#: Номер узла **необязателен**, и это не придирка к регулярному выражению.
#: Прежний шаблон требовал его всегда, потому что был снят с имён файлов
#: переданной выгрузки (`node_1_domain_2_nauss_0_…`). Настоящая площадка
#: отдаёт идентификаторы без него — `node_domain_1_nauss_0_1778844752_15533498`,
#: — и руководство в своих примерах тоже: `node_domain_0_nauss_0_1659488423_2`.
#:
#: Цена ошибки была бы полной: `download` проверяет форму **до** запроса, и
#: каждый настоящий звонок отсеивался бы как «мы спросили ерунду», не дойдя до
#: сети. Проверено 2026-09-12 живым скачиванием: запись на 1,1 МБ по
#: идентификатору без номера узла.
SESSION_ID_RE = re.compile(r"^node(_\d+)?_domain_\d+_nauss_\d+_\d+_\d+$")


#: Как путь классификации (`CallRef.disposition_path`) пишется одной строкой:
#: в `speech_calls.disposition` и в отчёте (SP-604). Тот же знак, что между
#: уровнями пути в интерфейсе.
DISPOSITION_SEPARATOR: Final[str] = PATH_SEPARATOR


def parse_object_key(name: str) -> tuple[str, date] | None:
    """Разобрать имя файла выгрузки в идентификатор сессии и дату звонка.

    Имена в выгрузке — URL-кодированные ключи объектов хранилища NCC:

        ncc-tm%2F2026%2F01%2F14%2Fnode_1_domain_2_nauss_0_1766092829_1138986.mp3
              ↓
        ncc-tm/2026/01/14/node_1_domain_2_nauss_0_1766092829_1138986

    Возвращает `None` для всего, что на ключ не похоже: в выгрузке попадаются
    `.DS_Store` и прочий сор, и заводить его в базу как звонок нельзя.
    """
    decoded = urllib.parse.unquote(name)
    match = OBJECT_KEY_RE.search(decoded)
    if match is None:
        return None
    session_id = match.group("session_id")
    if not SESSION_ID_RE.match(session_id):
        return None
    try:
        day = date(int(match.group("year")), int(match.group("month")), int(match.group("day")))
    except ValueError:
        # Ключ с датой 2026/13/45 — это испорченное имя, а не звонок.
        return None
    return session_id, day


class LocalExportSource:
    """Каталог с выгрузкой, переданной колл-центром.

    Что источник знает и чего не знает — существенно, потому что от этого
    зависит, какие требования выполнимы:

    * знает: идентификатор сессии, дату звонка, само аудио и **классификацию
      обращения** — она закодирована деревом каталогов;
    * не знает: оператора, очередь, точное время начала и окончания,
      направление вызова, результат обработки.

    Отсюда прямое следствие для интерфейса: фильтр по оператору и колонка
    оператора в карточке на этом источнике пусты. Не сломаны — пусты, и это
    видно пользователю.

    **Каталоги могут быть любой глубины, включая нулевую.** Подборка, на
    которой идёт разработка, разложена по дереву тематик переменной глубины:

        В2В/Мобильная связь/Консультация/Финансы/Баланс_ расходы/<файл>.mp3
        Кошелек/Консультация/Не работает/ЛК_МП/<файл>.mp3

    В настоящей выгрузке этого дерева не будет — там ожидается плоский каталог
    файлов. Обе формы обрабатываются одинаково, потому что вся нужная
    информация лежит **в имени файла**: оно и есть ключ объекта хранилища с
    датой и идентификатором сессии. Каталоги, если они есть, записываются как
    есть и ни на что не влияют.
    """

    def __init__(self, root: Path | str, *, chunk_size: int = 1 << 20) -> None:
        """Настроить LocalExportSource из переданных параметров и зависимостей."""
        self._root = Path(root)
        self._chunk_size = chunk_size

    @property
    def name(self) -> str:
        """Вернуть короткое имя источника для метрик и журнала."""
        return "local-export"

    def _iter_files(self) -> Iterable[tuple[Path, str, date]]:
        """Перечислить файлы локальной выгрузки в согласованном порядке."""
        for path in sorted(self._root.rglob("*.mp3")):
            parsed = parse_object_key(path.name)
            if parsed is None:
                continue
            session_id, day = parsed
            yield path, session_id, day

    async def list_calls(self, day: date) -> Sequence[CallRef]:
        """Перечислить метаданные звонков указанного дня без загрузки записей."""
        if not self._root.is_dir():
            raise FileNotFoundError(f"export directory does not exist: {self._root}")

        refs: dict[str, CallRef] = {}
        for path, session_id, file_day in self._iter_files():
            if file_day != day:
                continue
            relative = path.parent.relative_to(self._root)
            disposition = classifier_levels(
                urllib.parse.unquote(part) for part in relative.parts if part not in (".", "")
            )
            # Тот же звонок дважды в одной выгрузке — не ошибка выгрузки, а
            # свойство дерева: запись может лежать сразу в двух тематиках.
            # Побеждает первый по алфавиту, чтобы результат не зависел от
            # порядка обхода файловой системы.
            refs.setdefault(
                session_id,
                CallRef(
                    session_id=session_id,
                    audio_locator=str(path),
                    day=day,
                    disposition_path=disposition,
                ),
            )
        return list(refs.values())

    async def available_days(self) -> Sequence[date]:
        """Какие даты вообще есть в выгрузке.

        Нужна на время работы с выгрузкой: суточный прогон запускается по дате,
        и без этого списка дату пришлось бы угадывать.
        """
        return sorted({day for _path, _session_id, day in self._iter_files()})

    async def open_audio(self, ref: CallRef) -> AsyncIterator[bytes]:
        """Открыть поток записи звонка; отсутствие записи выражается ошибкой порта."""
        path = Path(ref.audio_locator)
        if not path.is_file():
            # `RecordingNotAvailable`, а не `FileNotFoundError`: для этапа
            # скачивания это «записи нет у источника» — тот же исход, что и
            # `404` от REST, и обрабатывать его надо одинаково.
            raise RecordingNotAvailable(f"audio file is gone: {path}")

        async def _stream() -> AsyncIterator[bytes]:
            """Читать ответ внешнего сервиса порциями с гарантированным закрытием."""
            with path.open("rb") as handle:
                while True:
                    chunk = handle.read(self._chunk_size)
                    if not chunk:
                        return
                    yield chunk

        return _stream()


class RestCasesSource:
    """ОПРОВЕРГНУТ 2026-09-14. Оставлен как запись проверенной гипотезы.

    Замысел был: кейсы проекта за диапазон дат → `sessionId` в кейсе → запись.
    Основание — руководство, 4.10.2 «Формат данных анкеты», где `sessionId`
    в структуре есть.

    **Вывод оказался неверным.** Та структура описывает анкету, которую пишет
    `POST /project/{id}/interaction/{sessionId}`, и `sessionId` в ней — из URI
    запроса, а не то, что отдаёт API кейсов. Проверено на живых данных:

    * 100 настоящих кейсов из открытого проекта — полей ровно четырнадцать
      (`uuid`, `id`, `title`, `project`, `state`, `operator`, `phoneNumbers`,
      `invalidPhoneNumbers`, `priority`, `timeZone`, `creationDate`,
      `completionDate`, `lastModified`, `customForm`), идентификатора сессии
      среди них нет;
    * полная карточка `GET /cases/{uuid}` — те же четырнадцать полей;
    * поиск **по образцу** `node…_domain…_nauss…` по всему ответу — ноль
      совпадений на 100 кейсах, то есть его нет и под другим именем;
    * `customForm.default` несёт поля кампании MNP (`AP_bez_skidki`,
      `Prichina_uhoda`, `Rezultat_kontakta`…) — данные обзвона, не сессии.

    Вдобавок права: из двадцати проектов читаются пять, и все исходящие.
    Каждый входящий проект отвечает `403`, а речевой аналитике нужны именно
    входящие.

    Класс не удалён намеренно: разбор ответа и поиск идентификатора проверены
    тестами и пригодятся, если поставщик сделает обещанное представление со
    списком сессий. Но собрать источник этим способом нельзя — фабрика
    отказывает явно, а не возвращает молча пустые сутки.

    Прежнее описание замысла:

    Звонки за сутки — из кейсов проектов NCC.

    Цепочка целиком: кейсы проекта за диапазон дат → `sessionId` в кейсе →
    запись по `sessionId`. Перечисления звонков как такового в API нет, и это
    ближайшее к нему: в телесейлзе кейс и есть звонок.

    Проекты задаются настройкой, а не выясняются: перечисления проектов в API
    не существует ни в v1, ни в v2 — проверено на живой площадке, все
    идентификаторы, которые отдают `/employees` и `/ous`, принадлежат
    сотрудникам и оргединицам. UUID проекта берётся из адресной строки консоли
    (руководство, 6.1).
    """

    name = "ncc_rest"

    #: Сколько страниц готовы пролистать за одни сутки.
    #:
    #: Страница фиксирована и равна ста, полторы тысячи звонков в день — это
    #: пятнадцать страниц. Пятьдесят оставляют запас на порядок и одновременно
    #: не дают зациклиться, если сервер однажды начнёт отдавать непустую
    #: страницу бесконечно: такой цикл не виден ничем, кроме растущего времени
    #: прогона.
    MAX_PAGES = 50

    def __init__(self, client: NccRestClient, projects: Sequence[str], *, timezone: str) -> None:
        """Настроить RestCasesSource из переданных параметров и зависимостей."""
        if not projects:
            raise RuntimeError(
                "SPEECH_NCC_SOURCE_KIND=rest requires SPEECH_NCC_PROJECTS — "
                "UUID проектов через запятую. Взять их можно только из адресной "
                "строки консоли NCC: перечисления проектов в API нет."
            )
        self._client = client
        self._projects = tuple(projects)
        self._tz = ZoneInfo(timezone)

    async def list_calls(self, day: date) -> Sequence[CallRef]:
        """Звонки за сутки во всех объявленных проектах.

        Границы суток берутся в часовом поясе колл-центра, а не в UTC: «за
        вчера» для человека — это его вчера, и в UTC сутки съезжают на три часа,
        то есть три часа вечерних звонков уезжают в соседний день.
        """
        start = datetime.combine(day, time.min, tzinfo=self._tz)
        end = start + timedelta(days=1)

        refs: dict[str, CallRef] = {}
        foreign_days = 0
        for project in self._projects:
            for page in range(self.MAX_PAGES):
                rows = await self._client.cases(project, since=start, until=end, page=page)
                if not rows:
                    break
                for row in rows:
                    for session_id in _session_ids_in(row):
                        if not SESSION_ID_RE.match(session_id):
                            continue
                        created = _case_moment(row, self._tz)
                        if created is not None and created.date() != day:
                            # Диапазон фильтрует по последнему изменению кейса,
                            # а не по времени звонка. Расхождение возможно, и
                            # молчать о нём нельзя: по нему видно, годится ли
                            # этот фильтр для суточного приёма вообще.
                            foreign_days += 1
                            continue
                        refs[session_id] = CallRef(
                            session_id=session_id,
                            # Локатор и есть идентификатор: скачивание идёт по
                            # нему, отдельного адреса у записи нет.
                            audio_locator=session_id,
                            day=day,
                            started_at=created,
                            operator_login=_text(row.get("operator")),
                            project_hint=project,
                        )
        if foreign_days:
            logger.info(
                "ncc rest: %s кейсов за %s изменены в эти сутки, но созданы в другие",
                foreign_days,
                day.isoformat(),
            )
        return tuple(refs.values())

    async def open_audio(self, ref: CallRef) -> AsyncIterator[bytes]:
        """Запись по идентификатору сессии.

        `RecordingNotAvailable` приходит из клиента и наружу проходит как есть:
        отличать «записи нет» от «источник сломан» — обязанность этапа, и
        подменять здесь одно другим нельзя.
        """
        # Возвращается итератор, а не генератор: порт объявляет `open_audio`
        # корутиной, отдающей поток. Асинхронный генератор здесь выглядел бы
        # так же в вызове и не совпал бы с типом — та же форма, что у выгрузки
        # выше.
        return self._client.download(ref.session_id)


def _session_ids_in(row: dict[str, Any]) -> list[str]:
    """Поиск идентификаторов сессии в кейсе.

    Импорт внутри функции: сама функция живёт в `rest.py`, а он импортирует
    этот модуль. На уровне файла это замкнуло бы круг импортов — тот же приём,
    что и в фабрике источников ниже.
    """
    from src.infrastructure.sources.ncc.rest import session_ids_in  # noqa: PLC0415

    return session_ids_in(row)


def _text(value: Any) -> str | None:
    """Прочитать текстовое значение и нормализовать отсутствие."""
    return value.strip() or None if isinstance(value, str) else None


def _case_moment(row: dict[str, Any], tz: ZoneInfo) -> datetime | None:
    """Время создания кейса, если оно есть и разбирается.

    Неразобранное время — не отказ: карточка звонка покажет дату без времени,
    и это честнее, чем полночь у всех подряд.
    """
    raw = row.get("creationDate")
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        moment = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return moment.astimezone(tz) if moment.tzinfo else moment.replace(tzinfo=tz)


def build_daily_batch_source(settings: Settings) -> DailyBatchSource:
    """Собрать источник по настройкам.

    Отказ, а не умолчание. Неизвестный вид источника или ненастроенный каталог
    останавливают процесс: суточный прогон, тихо решивший, что звонков за дату
    нет, выглядит как выходной день и обнаруживается только тем, что за неделю
    в поиске ничего не появилось.
    """
    kind = settings.speech_ncc_source_kind.strip().lower()
    if kind not in SOURCE_KINDS:
        raise RuntimeError(
            f"SPEECH_NCC_SOURCE_KIND={settings.speech_ncc_source_kind!r} is unknown. "
            "Known: " + ", ".join(sorted(SOURCE_KINDS))
        )
    if kind == "local_export":
        directory = settings.speech_ncc_export_dir.strip()
        if not directory:
            raise RuntimeError(
                "SPEECH_NCC_SOURCE_KIND=local_export requires SPEECH_NCC_EXPORT_DIR, "
                "the directory holding the export received from the call centre."
            )
        return LocalExportSource(Path(directory))
    if kind == "rest":
        # Отказ, а не молчаливая пустота. Способ проверен на живых данных
        # 2026-09-14 и не работает: в кейсах нет `sessionId` ни под каким
        # именем, а входящие проекты закрыты `403`. Источник, который всегда
        # возвращает ноль звонков, выглядит как выходной день и обнаруживается
        # через неделю пустого поиска — ровно то, чего этот проект избегает
        # везде.
        raise RuntimeError(
            "SPEECH_NCC_SOURCE_KIND=rest опровергнут: API кейсов не отдаёт "
            "sessionId (проверено на 100 кейсах), а входящие проекты закрыты "
            "403. Пока поставщик не сделал представление со списком сессий, "
            "используй sequence или local_export. Подробности — в "
            "reference/2026-09-12-ncc-integration-pattern.md."
        )
    if kind == "sequence":
        # Обход сквозного счётчика сессий. Единственный способ получить список
        # звонков через API, который сейчас работает: перечисления сессий в NCC
        # нет, а кейсы не несут `sessionId` (проверено 2026-09-14).
        from src.infrastructure.sources.ncc.rest import build_ncc_rest_client  # noqa: PLC0415
        from src.infrastructure.sources.ncc.sequence import SequenceWalkSource  # noqa: PLC0415

        return SequenceWalkSource(
            build_ncc_rest_client(settings),
            anchor_session_id=settings.speech_ncc_session_anchor,
            timezone=settings.speech_display_timezone,
        )
    if kind == "reporting_view":
        # Представления отчётной базы NCC: список звонков оттуда, запись — через
        # REST. Вариант поставщика после опровержения перечисления через API
        # (SP-001a), проверен на живой базе 2026-09-15.
        from src.infrastructure.sources.ncc.reporting import (  # noqa: PLC0415
            ReportingViewSource,
            asyncpg_fetcher,
            parse_views,
        )
        from src.infrastructure.sources.ncc.rest import build_ncc_rest_client  # noqa: PLC0415

        dsn = settings.speech_ncc_reporting_database_url.strip()
        password = settings.speech_ncc_reporting_db_password.strip()
        views = parse_views(settings.speech_ncc_reporting_views)
        missing = [
            name
            for name, present in (
                ("SPEECH_NCC_REPORTING_DATABASE_URL", bool(dsn)),
                ("SPEECH_NCC_REPORTING_DB_PASSWORD", bool(password)),
                ("SPEECH_NCC_REPORTING_VIEWS", bool(views)),
            )
            if not present
        ]
        if missing:
            raise RuntimeError(
                "SPEECH_NCC_SOURCE_KIND=reporting_view requires " + ", ".join(missing)
            )
        return ReportingViewSource(
            fetch=asyncpg_fetcher(dsn, password=password),
            client=build_ncc_rest_client(settings),
            views=views,
            timezone=settings.speech_ncc_reporting_timezone,
            min_talk_seconds=settings.speech_ncc_min_talk_seconds,
        )
    raise RuntimeError(
        f"source kind {kind!r} is declared but not implemented yet: it waits for "
        "access to the NCC object storage. Use local_export until then."
    )
