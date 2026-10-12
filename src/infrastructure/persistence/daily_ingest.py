"""SQL регистрации звонков, сопоставление справочников и очередь одной транзакции."""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, time, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.application.dto.calls import (
    CallSource,
    ClassifierSource,
)
from src.application.dto.daily_ingest import IngestSettings, ProjectForIngest, RegisteredCall
from src.application.phone import hash_phone_number, mask_phone_number, mask_phones_in_snapshot
from src.application.ports.daily_ingest import IngestTransaction
from src.application.ports.queue import NewJob
from src.infrastructure.persistence.classifier import ClassifierPaths
from src.infrastructure.persistence.settings_store import read_runtime_settings
from src.infrastructure.queue.postgres import PostgresJobQueue, acquire_singleton
from src.infrastructure.sources.ncc.source import DISPOSITION_SEPARATOR, CallRef

DAILY_LOCK_NAME = "speech:daily-ingest"

CURSOR_SOURCE = "ncc_daily"


def lock_name(call_source: str) -> str:
    """Блокировка прогона источника.

    У NCC — прежнее имя: новое на время выкатки пустило бы старый и новый
    прогоны одновременно.
    """
    return DAILY_LOCK_NAME if call_source == CallSource.NCC else f"{DAILY_LOCK_NAME}:{call_source}"


def cursor_source(call_source: str) -> str:
    """Строка курсора источника: `ncc_daily`, `telphin_daily`."""
    return f"{call_source}_daily"


UNKNOWN_DIRECTION = "unknown"

_UPSERT_CALL_SQL = text(
    """
    INSERT INTO speech_calls (
        id, session_id, source, project_id, operator_id, operator_node_id, direction,
        started_at, ended_at, duration_s, disposition,
        client_number_mask, client_number_hash, source_audio_url,
        audio_status, processing_status, analysis_status,
        source_snapshot, first_seen_at, created_at, updated_at
    )
    VALUES (
        gen_random_uuid(), :session_id, :source, :project_id, :operator_id, :operator_node_id,
        :direction, :started_at, :ended_at, :duration_s, :disposition,
        :client_number_mask, :client_number_hash, :source_audio_url,
        'pending', 'queued', 'pending',
        CAST(:snapshot AS jsonb), now(), now(), now()
    )
    ON CONFLICT (session_id) DO UPDATE
       SET operator_id = COALESCE(speech_calls.operator_id, EXCLUDED.operator_id),
           project_id = COALESCE(speech_calls.project_id, EXCLUDED.project_id),
           operator_node_id = COALESCE(speech_calls.operator_node_id, EXCLUDED.operator_node_id),
           disposition = COALESCE(speech_calls.disposition, EXCLUDED.disposition),
           -- Ссылка на запись нужна, только пока запись не забрана: забранной
           -- она не нужна вовсе, и держать секрет дольше незачем.
           source_audio_url = CASE WHEN speech_calls.audio_status = 'pending'
                                   THEN COALESCE(speech_calls.source_audio_url,
                                                 EXCLUDED.source_audio_url)
                              END,
           updated_at = now()
     -- Известный звонок трогается, только если ему есть что добавить: ссылку или
     -- классификацию оператора, которых у него нет. Уже заполненное не
     -- переписывается — ни снимок, ни время, ни статусы: повторный прогон не
     -- должен менять обработанное.
     WHERE (speech_calls.operator_id IS NULL AND EXCLUDED.operator_id IS NOT NULL)
        OR (speech_calls.project_id IS NULL AND EXCLUDED.project_id IS NOT NULL)
        OR (speech_calls.operator_node_id IS NULL AND EXCLUDED.operator_node_id IS NOT NULL)
        OR (speech_calls.disposition IS NULL AND EXCLUDED.disposition IS NOT NULL)
        OR (speech_calls.source_audio_url IS NULL AND EXCLUDED.source_audio_url IS NOT NULL
            AND speech_calls.audio_status = 'pending')
    RETURNING id, (xmax = 0) AS inserted
    """
)

_EXISTING_CALL_SQL = text("SELECT id FROM speech_calls WHERE session_id = :session_id")

_ENSURE_OPERATOR_SQL = text(
    """
    INSERT INTO speech_operators (id, login, full_name, removed, created_at, updated_at)
    VALUES (gen_random_uuid(), :login, :full_name, FALSE, now(), now())
    ON CONFLICT (login) DO UPDATE
       SET full_name = EXCLUDED.full_name, updated_at = now()
     WHERE speech_operators.full_name = speech_operators.login
       AND EXCLUDED.full_name <> EXCLUDED.login
    """
)

_OPERATOR_ID_SQL = text("SELECT id FROM speech_operators WHERE login = :login")

_ENSURE_PROJECT_SQL = text(
    """
    INSERT INTO speech_ncc_projects (
        id, source_key, title, direction, ingest_enabled, created_at, updated_at
    )
    VALUES (gen_random_uuid(), :source_key, :title, :direction, TRUE, now(), now())
    ON CONFLICT (source_key) DO UPDATE
       SET title = EXCLUDED.title,
           updated_at = now()
     WHERE speech_ncc_projects.title IS DISTINCT FROM EXCLUDED.title
    """
)

_PROJECT_SQL = text(
    "SELECT id, ingest_enabled FROM speech_ncc_projects WHERE source_key = :source_key"
)

_TOUCH_PROJECT_SQL = text(
    """
    UPDATE speech_ncc_projects
       SET last_call_at = :last_call_at,
           updated_at = now()
     WHERE id = :id
       AND (last_call_at IS NULL OR last_call_at < :last_call_at)
    """
)


class _Directory:
    """Операторы, проекты и узлы классификатора одного прогона.

    Запрос к базе — на каждое новое имя, а не на звонок: в сутках около 1800
    звонков, полсотни операторов и несколько десятков путей классификации.
    """

    def __init__(self, session: AsyncSession) -> None:
        """Настроить _Directory из переданных параметров и зависимостей."""
        self._session = session
        self._operators: dict[str, uuid.UUID] = {}
        self._projects: dict[str, tuple[uuid.UUID, bool]] = {}
        self.classifier = ClassifierPaths(session, source=ClassifierSource.NCC)

    async def operator(self, login: str | None, name: str | None = None) -> uuid.UUID | None:
        """Найти или создать сотрудника источника и вернуть его UUID."""
        if not login:
            return None
        if login not in self._operators:
            await self._session.execute(
                _ENSURE_OPERATOR_SQL, {"login": login, "full_name": name or login}
            )
            found = await self._session.execute(_OPERATOR_ID_SQL, {"login": login})
            self._operators[login] = found.scalar_one()
        return self._operators[login]

    async def project(
        self, key: str | None, title: str | None, direction: str | None = None
    ) -> tuple[uuid.UUID, bool] | None:
        """Идентификатор проекта и включён ли его сбор; `None` — источник проекта не знает."""
        if not key:
            return None
        if key not in self._projects:
            await self._session.execute(
                _ENSURE_PROJECT_SQL,
                {
                    "source_key": key,
                    "title": title or key,
                    "direction": direction or UNKNOWN_DIRECTION,
                },
            )
            row = (await self._session.execute(_PROJECT_SQL, {"source_key": key})).one()
            self._projects[key] = (row.id, bool(row.ingest_enabled))
        return self._projects[key]

    async def classification(self, levels: Sequence[str]) -> uuid.UUID | None:
        """Узел классификатора для пути оператора; недостающие узлы заводятся (S-22)."""
        return await self.classifier.node(levels)


def disposition_text(ref: CallRef) -> str | None:
    """Классификация оператора одной строкой — «проблема» звонка (SP-604).

    Пустые уровни в строку не попадают: у «Кошелька» без уровня 1 проблема
    начинается с «Кошелек». Обрезается по ширине колонки (255): длиннее путей у
    NCC не встречалось, но отказ вставки из-за одной длинной классификации стоил
    бы суток приёма.
    """
    joined = DISPOSITION_SEPARATOR.join(level for level in ref.disposition_path if level)
    return joined[:255] or None


async def upsert_call(
    session: AsyncSession,
    ref: CallRef,
    *,
    operator_id: uuid.UUID | None = None,
    project_id: uuid.UUID | None = None,
    operator_node_id: uuid.UUID | None = None,
    phone_pepper: str = "",
) -> tuple[object, bool]:
    """Завести звонок. Возвращает идентификатор и признак «новый».

    Время. Источник может не знать ни начала, ни конца — локальная выгрузка
    знает только дату. Колонки при этом `NOT NULL`, поэтому подставляется
    полночь этой даты, а **отсутствие точного времени записывается в снимок**
    (`time_precision`). Интерфейс по этому признаку показывает дату без
    времени: полночь у полутора тысяч звонков подряд читается как поломка, а
    не как отсутствие данных.

    Известному звонку дописываются только недостающие оператор, проект и узел
    классификации.

    Номер клиента, если источник его знает (Телфин), — только маской и хешем
    (ING-02); без соли хеша нет, маска есть всегда. Ссылка на запись, которая
    сама по себе секрет, идёт в отдельную колонку, не в снимок.
    """
    started = ref.started_at or datetime.combine(ref.day, time.min, tzinfo=UTC)
    ended = ref.ended_at or started + timedelta(seconds=ref.duration_s or 0)
    snapshot: dict[str, object] = {
        "source": "daily-batch",
        "call_source": ref.source,
        "audio_locator": ref.audio_locator,
        "day": ref.day.isoformat(),
        "time_precision": "second" if ref.started_at else "day",
        "operator_login": ref.operator_login,
        "project_hint": ref.project_hint,
        "project_key": ref.project_key,
        "disposition_path": list(ref.disposition_path),
    }
    number = ref.client_number
    result = await session.execute(
        _UPSERT_CALL_SQL,
        {
            "session_id": ref.session_id,
            "source": ref.source,
            "project_id": project_id,
            "operator_id": operator_id,
            "operator_node_id": operator_node_id,
            "direction": ref.direction or "unknown",
            "started_at": started,
            "ended_at": ended,
            "duration_s": ref.duration_s or 0,
            "disposition": disposition_text(ref),
            "client_number_mask": mask_phone_number(number) if number else None,
            "client_number_hash": hash_phone_number(number, phone_pepper)
            if number and phone_pepper.strip()
            else None,
            "source_audio_url": ref.audio_url,
            "snapshot": json.dumps(mask_phones_in_snapshot(snapshot), ensure_ascii=False),
        },
    )
    row = result.first()
    if row is not None:
        return (row[0], bool(row[1]))
    existing = await session.execute(_EXISTING_CALL_SQL, {"session_id": ref.session_id})
    return (existing.scalar_one(), False)


async def _record_day(session: AsyncSession, day: date, source: str = CURSOR_SOURCE) -> None:
    """Запомнить, что дата обработана.

    Курсор нужен не для идемпотентности — её обеспечивают уникальные ключи, —
    а чтобы интерфейс мог отличить «за эту дату звонков не было» от «за эту
    дату прогон не запускался». Первое нормально, второе — пропущенные сутки.
    """
    await session.execute(
        text(
            "INSERT INTO speech_ingest_cursors (source, last_processed_day, "
            "updated_at) VALUES (:source, :day, now()) ON CONFLICT (source) DO"
            " UPDATE SET   last_processed_day = GREATEST(     "
            "COALESCE(speech_ingest_cursors.last_processed_day, :day), :day),"
            "   updated_at = now()"
        ),
        {"source": source, "day": day},
    )


class PostgresIngestTransaction:
    """Операции дня на одном соединении и одном кэше справочников."""

    def __init__(self, session: AsyncSession, queue: PostgresJobQueue, phone_pepper: str) -> None:
        """Принять общую сессию, очередь и соль записи телефонного хеша."""
        self._session = session
        self._queue = queue
        self._pepper = phone_pepper
        self._directory = _Directory(session)

    @property
    def classifier_created(self) -> int:
        """Вернуть число созданных в этой транзакции узлов классификатора."""
        return self._directory.classifier.created

    async def try_lock(self, call_source: str) -> bool:
        """Захватить advisory-блокировку источника до завершения дня."""
        return await acquire_singleton(self._session, lock_name(call_source))

    async def settings(self) -> IngestSettings:
        """Прочитать и проверить настройки, вернуть только пределы попыток."""
        values = await read_runtime_settings(self._session)
        return IngestSettings(max_attempts=values["max_attempts"])

    async def project(
        self, key: str | None, title: str | None, direction: str | None = None
    ) -> ProjectForIngest | None:
        """Сопоставить проект через кэш и преобразовать SQL-данные в DTO."""
        found = await self._directory.project(key, title, direction)
        return ProjectForIngest(id=found[0], ingest_enabled=found[1]) if found else None

    async def operator(self, login: str | None, name: str | None = None) -> uuid.UUID | None:
        """Получить идентификатор оператора через кэш этого дня."""
        return await self._directory.operator(login, name)

    async def classification(self, levels: Sequence[str]) -> uuid.UUID | None:
        """Сопоставить путь источника с деревом классификатора."""
        return await self._directory.classification(levels)

    async def upsert(
        self,
        ref: CallRef,
        *,
        operator_id: uuid.UUID | None,
        project_id: uuid.UUID | None,
        operator_node_id: uuid.UUID | None,
    ) -> RegisteredCall:
        """Идемпотентно записать метаданные звонка и вернуть идентификатор."""
        call_id, is_new = await upsert_call(
            self._session,
            ref,
            operator_id=operator_id,
            project_id=project_id,
            operator_node_id=operator_node_id,
            phone_pepper=self._pepper,
        )
        return RegisteredCall(call_id=uuid.UUID(str(call_id)), is_new=is_new)

    async def enqueue(self, job: NewJob) -> uuid.UUID | None:
        """Поставить этап в общей транзакции дня."""
        return await self._queue.enqueue(self._session, job)

    async def touch_project(self, project_id: uuid.UUID, moment: datetime) -> None:
        """Записать самый поздний звонок проекта в обработанном дне."""
        await self._session.execute(_TOUCH_PROJECT_SQL, {"id": project_id, "last_call_at": moment})

    async def record_day(self, day: date, source: str) -> None:
        """Продвинуть суточный курсор только для полного прогона."""
        await _record_day(self._session, day, source)


class PostgresDailyIngestRepository:
    """Управлять транзакцией регистрации дня и её временем жизни."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        queue: PostgresJobQueue,
        *,
        phone_pepper: str = "",
    ) -> None:
        """Принять фабрику сессий и очередь, принадлежащие одному бэкенду."""
        self._sessions = sessions
        self._queue = queue
        self._pepper = phone_pepper

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[IngestTransaction]:
        """Открыть транзакцию дня; при исключении откатить звонки, задания и курсор."""
        async with self._sessions() as session, session.begin():
            yield PostgresIngestTransaction(session, self._queue, self._pepper)
