"""Этап `ingest`: три исхода и их последствия.

Без базы и без сети: проверяется решение этапа, а не то, умеет ли Postgres
писать строки. Источник и хранилище подставные — и подставными они здесь
уместны ровно потому, что настоящие уже проверены отдельно (SigV4 на живом
MinIO, выгрузка на 61 настоящей записи).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.pipeline.ingest import IngestHandler, IngestWork
from src.application.pipeline.stages import StageDeps, StageError
from src.application.ports.queue import ClaimedJob
from src.di.pipeline import build_stage_deps
from src.domain.calls.contracts import (
    SPEECH_TERMINAL_PROCESSING_STATUSES,
    AudioStatus,
    JobErrorClass,
    JobKind,
    JobLane,
    ProcessingStatus,
)
from src.infrastructure.media.audio import AudioProbe
from src.infrastructure.persistence.stages.repository import PostgresStagePersistence
from src.infrastructure.sources.ncc.source import RecordingNotAvailable

CALL_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
SESSION_ID = "node_1_domain_2_nauss_0_1766092829_1138986"
STARTED = datetime(2026, 1, 14, tzinfo=UTC)
EXPECTED_KEY = f"calls/2026/01/14/{SESSION_ID}.mp3"


# --------------------------------------------------------------------------- #
# Подставные зависимости
# --------------------------------------------------------------------------- #


class _Result:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row

    def mappings(self) -> _Result:
        return self

    def first(self) -> dict[str, Any] | None:
        return self._row


class _Session:
    """Сессия, отдающая одну заготовленную строку и запоминающая записи."""

    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row
        self.writes: list[dict[str, Any]] = []

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _Result:
        if params is not None and "audio_status" in params:
            self.writes.append(params)
            return _Result(None)
        return _Result(self._row)

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *_exc: object) -> bool:
        return False


class _Source:
    def __init__(self, chunks: list[bytes] | None = None, error: Exception | None = None) -> None:
        self._chunks = chunks or []
        self._error = error
        self.opened = 0

    async def open_audio(self, ref: Any) -> Any:
        self.opened += 1
        if self._error is not None:
            raise self._error

        async def _stream() -> Any:
            for chunk in self._chunks:
                yield chunk

        return _stream()


class _Storage:
    def __init__(self, *, has: bool = False) -> None:
        self.puts: list[tuple[str, int]] = []
        self._has = has
        self.exists_calls = 0

    async def put(self, key: str, chunks: Any) -> int:
        size = 0
        async for chunk in chunks:
            size += len(chunk)
        self.puts.append((key, size))
        return size

    async def exists(self, key: str) -> bool:
        self.exists_calls += 1
        return self._has


def _call_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "session_id": SESSION_ID,
        "duration_s": 0,
        "started_at": STARTED,
        "audio_status": AudioStatus.PENDING.value,
        "audio_object_key": None,
        "source_snapshot": {"audio_locator": "/export/recording.mp3"},
        # Проект не сопоставлен: сегмент и продукт выведет этап анализа.
        "project_segment": None,
        "project_product": None,
    }
    row.update(overrides)
    return row


def _deps(session: _Session, source: _Source, storage: _Storage) -> StageDeps:
    return build_stage_deps(
        settings=None,
        sessionmaker=lambda: cast(AsyncSession, session),
        source=source,
        storage=storage,
    )


def _job(*, attempts: int = 1, max_attempts: int = 6) -> ClaimedJob:
    return ClaimedJob(
        id=uuid.uuid4(),
        call_id=CALL_ID,
        kind=JobKind.INGEST,
        lane=JobLane.DAILY,
        attempts=attempts,
        max_attempts=max_attempts,
        payload={},
        lease_seconds=300,
    )


@pytest.fixture()
def probe(monkeypatch) -> AudioProbe:
    """`ffprobe` подменён: здесь проверяется этап, а не разбор файла."""
    measured = AudioProbe(
        duration_s=208.0, channels=2, sample_rate=8000, bit_rate=24000, codec="mp3"
    )

    async def _fake(_path: Any) -> AudioProbe:
        return measured

    monkeypatch.setattr("src.infrastructure.media.audio.probe_audio", _fake)
    return measured


# --------------------------------------------------------------------------- #
# Исход первый: запись скачана
# --------------------------------------------------------------------------- #


async def test_a_recording_travels_from_the_source_into_our_storage(probe) -> None:
    session = _Session(_call_row())
    source = _Source(chunks=[b"ID3", b"audio-bytes"])
    storage = _Storage()

    work = await IngestHandler().work(_job(), _deps(session, source, storage))

    assert work.outcome == IngestWork.STORED
    assert storage.puts == [(EXPECTED_KEY, 14)], "ключ детерминированный (ING-03)"
    assert work.audio_bytes == 14
    assert work.channels == 2
    assert work.duration_s == 208


async def test_storing_writes_the_measurements_and_queues_transcription(probe) -> None:
    session = _Session(_call_row())
    work = await IngestHandler().work(_job(), _deps(session, _Source([b"x" * 32]), _Storage()))

    result = await IngestHandler().store(PostgresStagePersistence(session), _job(), work)

    assert result.processing_status is ProcessingStatus.TRANSCRIBING
    assert result.next_kind is JobKind.TRANSCRIBE
    written = session.writes[-1]
    assert written["audio_status"] == AudioStatus.STORED.value
    assert written["object_key"] == EXPECTED_KEY
    assert written["channels"] == 2


# --------------------------------------------------------------------------- #
# Исход второй: звонок короткий
# --------------------------------------------------------------------------- #


async def test_a_short_call_is_not_downloaded_at_all() -> None:
    """ING-05: аудио для коротких звонков не забирается.

    Не оптимизация трафика: расшифровывать девятисекундный гудок нечего, а
    место в ночном окне он занимает наравне с настоящим разговором.
    """
    session = _Session(_call_row(duration_s=9))
    source, storage = _Source([b"never-read"]), _Storage()

    work = await IngestHandler().work(_job(), _deps(session, source, storage))
    result = await IngestHandler().store(PostgresStagePersistence(session), _job(), work)

    assert work.outcome == IngestWork.SKIPPED_SHORT
    assert source.opened == 0, "источник не должен быть даже открыт"
    assert storage.puts == []
    assert result.processing_status is ProcessingStatus.SKIPPED_SHORT
    assert result.next_kind is None, "цепочка кончается здесь"


async def test_an_unknown_duration_is_not_mistaken_for_a_short_call(probe) -> None:
    """Ноль означает «источник не сказал», а не «звонок нулевой длины».

    Локальная выгрузка длительности не знает вовсе, и толкование нуля как
    короткого звонка выбросило бы **весь** суточный пакет, не расшифровав ни
    одной записи.
    """
    session = _Session(_call_row(duration_s=0))

    work = await IngestHandler().work(_job(), _deps(session, _Source([b"a" * 16]), _Storage()))

    assert work.outcome == IngestWork.STORED


# --------------------------------------------------------------------------- #
# Исход третий: записи у источника нет
# --------------------------------------------------------------------------- #


async def test_a_missing_recording_is_retried_while_attempts_remain() -> None:
    """Пока окно не исчерпано, это повод подождать, а не вывод."""
    session = _Session(_call_row())
    source = _Source(error=RecordingNotAvailable("нет файла"))

    with pytest.raises(StageError) as raised:
        await IngestHandler().work(_job(attempts=1), _deps(session, source, _Storage()))

    assert raised.value.error_class is JobErrorClass.NOT_FOUND


async def test_the_last_attempt_concludes_that_there_is_no_recording() -> None:
    """ING-04: исчерпание окна — вывод, а не отказ.

    Отказ убил бы задание, а мёртвое задание переводит звонок в `failed` —
    «сломалось». Ничего не сломалось: сбор идёт вечером, когда записи за сутки
    давно готовы, и её отсутствие в это время означает, что её не будет.
    """
    session = _Session(_call_row())
    source = _Source(error=RecordingNotAvailable("нет файла"))

    work = await IngestHandler().work(
        _job(attempts=6, max_attempts=6), _deps(session, source, _Storage())
    )
    result = await IngestHandler().store(PostgresStagePersistence(session), _job(), work)

    assert work.outcome == IngestWork.NO_AUDIO
    assert session.writes[-1]["audio_status"] == AudioStatus.UNAVAILABLE_AT_SOURCE.value
    assert result.processing_status is ProcessingStatus.SKIPPED_NO_AUDIO
    assert result.next_kind is None


def test_every_deliberate_skip_takes_the_call_out_of_the_queue() -> None:
    """Инвариант, который действительно может сломаться.

    Прежняя редакция сравнивала `SKIPPED_NO_AUDIO` с `FAILED` и `DONE` и
    объявляла их разными. Это статический факт, а не свойство времени
    выполнения: два разных члена перечисления не совпадают никогда, ни по
    тождеству, ни по значению, и написать такую проверку осмысленно нельзя ни в
    каком виде — mypy указал на это дважды подряд.

    Ломается здесь другое: статус, добавленный в перечисление и забытый в
    `SPEECH_TERMINAL_PROCESSING_STATUSES`. Тогда звонок, обработку которого
    намеренно прекратили, навсегда остаётся в счётчике «в очереди на
    расшифровку», который смотрит пользователь (SRC-08). Ошибка тихая: ни один
    тест этапа её не увидит, потому что сам этап отработает верно.
    """
    skips = {status for status in ProcessingStatus if status.name.startswith("SKIPPED_")}
    assert skips, "проверка потеряла смысл, если ни одного такого статуса не осталось"

    assert skips <= SPEECH_TERMINAL_PROCESSING_STATUSES, (
        "намеренно прекращённая обработка обязана выводить звонок из очереди"
    )
    assert ProcessingStatus.QUEUED not in SPEECH_TERMINAL_PROCESSING_STATUSES


# --------------------------------------------------------------------------- #
# Повторный прогон
# --------------------------------------------------------------------------- #


async def test_an_already_stored_recording_is_not_fetched_again(probe) -> None:
    """ING-01: повторный прогон за ту же дату не перекачивает сохранённое."""
    session = _Session(
        _call_row(audio_status=AudioStatus.STORED.value, audio_object_key=EXPECTED_KEY)
    )
    source, storage = _Source([b"never-read"]), _Storage(has=True)

    work = await IngestHandler().work(_job(), _deps(session, source, storage))

    assert work.outcome == IngestWork.STORED
    assert source.opened == 0
    assert storage.puts == []


async def test_a_column_claiming_storage_is_verified_against_the_storage(probe) -> None:
    """Колонке на слово не верят.

    `audio_status='stored'` при отсутствующем объекте — ровно тот случай, ради
    которого этап перезапускают. Поверив колонке, мы отправили бы в расшифровку
    звонок без записи.
    """
    session = _Session(
        _call_row(audio_status=AudioStatus.STORED.value, audio_object_key=EXPECTED_KEY)
    )
    source, storage = _Source([b"real-bytes"]), _Storage(has=False)

    work = await IngestHandler().work(_job(), _deps(session, source, storage))

    assert work.audio_bytes == 10, "измерения настоящие, а не унаследованные от колонки"
    assert storage.exists_calls == 1
    assert source.opened == 1, "объекта нет — запись забирается заново"
    assert storage.puts == [(EXPECTED_KEY, 10)]


async def test_re_confirming_a_stored_object_does_not_erase_its_measurements(probe) -> None:
    """Проверка наличия объекта ничего не измеряет.

    Без `COALESCE` в записи такая проверка обнулила бы число каналов и байт, а
    `duration_s` объявлен `NOT NULL` — то есть попросту отказала бы.
    """
    session = _Session(
        _call_row(audio_status=AudioStatus.STORED.value, audio_object_key=EXPECTED_KEY)
    )
    work = await IngestHandler().work(_job(), _deps(session, _Source(), _Storage(has=True)))

    await IngestHandler().store(PostgresStagePersistence(session), _job(), work)

    written = session.writes[-1]
    assert written["duration_s"] is None, "None означает «не трогать», COALESCE сохранит прежнее"
    assert written["channels"] is None
    assert written["audio_bytes"] is None


# --------------------------------------------------------------------------- #
# Отказы, которые отказы
# --------------------------------------------------------------------------- #


async def test_an_empty_response_is_not_a_recording() -> None:
    """Пустой файл дальше по конвейеру выглядел бы как беззвучный разговор."""
    session = _Session(_call_row())

    with pytest.raises(StageError) as raised:
        await IngestHandler().work(_job(), _deps(session, _Source([]), _Storage()))

    assert raised.value.error_class is JobErrorClass.PROVIDER_ERROR


async def test_a_broken_source_is_not_reported_as_a_missing_recording() -> None:
    """Сломанный источник и отсутствие записи — разные исходы.

    Смешать их значит после шести попыток объявить «записи нет» там, где
    источник просто лежал.
    """
    session = _Session(_call_row())
    source = _Source(error=ConnectionResetError("сеть отвалилась"))

    with pytest.raises(StageError) as raised:
        await IngestHandler().work(
            _job(attempts=6, max_attempts=6), _deps(session, source, _Storage())
        )

    assert raised.value.error_class is JobErrorClass.PROVIDER_ERROR


async def test_a_call_without_a_locator_says_so_instead_of_guessing() -> None:
    session = _Session(_call_row(source_snapshot={}))

    with pytest.raises(StageError) as raised:
        await IngestHandler().work(_job(), _deps(session, _Source(), _Storage()))

    assert raised.value.error_class is JobErrorClass.NOT_FOUND


async def test_a_call_that_is_gone_is_not_retried_forever() -> None:
    session = _Session(None)

    with pytest.raises(StageError) as raised:
        await IngestHandler().work(_job(), _deps(session, _Source(), _Storage()))

    assert raised.value.error_class is JobErrorClass.NOT_FOUND
