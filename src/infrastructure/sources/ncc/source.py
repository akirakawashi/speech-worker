"""Получение списка звонков NCC и чтение аудио выбранного источника.

Фабрика поддерживает локальную выгрузку, обход сессий и reporting-представления.
Режим кейсов rest отклоняется: API кейсов не отдаёт список сессий.
"""

from __future__ import annotations

import logging
import re
import urllib.parse
from collections.abc import AsyncIterator, Iterable, Sequence
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Final

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
            "docs/configuration.md."
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
