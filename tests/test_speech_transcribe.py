"""Этап `transcribe`: реплики, роли и отказы.

Без сети и без базы: проверяется решение этапа. Whisper подставной — настоящий
уже замерен отдельно (SP-003), и гонять его в тестах значит проверять чужой
GPU вместо своей логики.
"""

from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.dto.transcription import (
    TranscriptionError,
    TranscriptionResult,
    TranscriptionSegment,
)
from src.application.pipeline.stages import StageDeps, StageError
from src.application.pipeline.transcribe import TranscribeHandler
from src.application.ports.queue import ClaimedJob
from src.di.pipeline import build_stage_deps
from src.domain.calls.contracts import (
    AudioStatus,
    JobErrorClass,
    JobKind,
    JobLane,
    ProcessingStatus,
    RoleMethod,
    Speaker,
)
from src.infrastructure.clients import whisper as whisper_module
from src.infrastructure.clients.whisper import WhisperNotConfigured
from src.infrastructure.media.audio import ChannelEnvelope
from src.infrastructure.persistence.stages.repository import PostgresStagePersistence

CALL_ID = uuid.UUID("22222222-3333-4444-5555-666666666666")
KEY = "calls/2026/01/14/node_1_domain_2_nauss_0_1766092829_1138986.mp3"


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

    def scalar_one(self) -> uuid.UUID:
        return uuid.UUID("77777777-8888-9999-aaaa-bbbbbbbbbbbb")


class _Session:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row
        self.statements: list[tuple[str, dict[str, Any]]] = []

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _Result:
        self.statements.append((str(statement).strip().split()[0].upper(), params or {}))
        return _Result(self._row)

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *_exc: object) -> bool:
        return False


class _Response:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    async def aiter_bytes(self) -> Any:
        for chunk in self._chunks:
            yield chunk


class _Storage:
    def __init__(self, chunks: list[bytes] | None = None) -> None:
        self._chunks = chunks if chunks is not None else [b"ID3", b"audio"]

    def open(self, key: str, **_kw: Any) -> Any:
        chunks = self._chunks

        class _Ctx:
            async def __aenter__(self) -> _Response:
                return _Response(chunks)

            async def __aexit__(self, *_exc: object) -> bool:
                return False

        return _Ctx()


class _Whisper:
    def __init__(
        self, segments: tuple[TranscriptionSegment, ...] = (), error: Exception | None = None
    ) -> None:
        self._segments = segments
        self._error = error
        self.calls = 0

    async def transcribe(self, path: Any) -> TranscriptionResult:
        self.calls += 1
        if self._error is not None:
            raise self._error
        return TranscriptionResult(
            text=" ".join(s.text for s in self._segments),
            provider_kind="openai_compatible",
            model="large-v3",
            language="ru",
            duration_seconds=42.0,
            segments=self._segments,
        )


def _segment(start: float, end: float, text: str) -> TranscriptionSegment:
    return TranscriptionSegment(start_s=start, end_s=end, text=text)


def _call_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "session_id": "node_1_domain_2_nauss_0_1766092829_1138986",
        "audio_status": AudioStatus.STORED.value,
        "audio_object_key": KEY,
        "audio_channels": 2,
        "duration_s": 208,
    }
    row.update(overrides)
    return row


def _job() -> ClaimedJob:
    return ClaimedJob(
        id=uuid.uuid4(),
        call_id=CALL_ID,
        kind=JobKind.TRANSCRIBE,
        lane=JobLane.DAILY,
        attempts=1,
        max_attempts=3,
        payload={},
        lease_seconds=900,
    )


def _envelope(*windows: tuple[float, ...]) -> ChannelEnvelope:
    """Огибающая из готовых уровней: каналы по столбцам, окна по строкам."""
    channels = len(windows[0])
    return ChannelEnvelope(
        window_s=0.1,
        levels=tuple(tuple(row[ch] for row in windows) for ch in range(channels)),
    )


@pytest.fixture()
def prepared(monkeypatch):
    """`ffmpeg` подменён: здесь проверяется этап, а не медиа-утилиты.

    Подмена идёт через `monkeypatch`, а не присваиванием в модуль: присвоенное
    вручную осталось бы в модуле после теста и молча поменяло бы поведение
    всякого, кто выполнится следом.
    """

    async def _downmix(_src: Any, dst: Any) -> Any:
        dst.write_bytes(b"RIFFfake")
        return dst

    monkeypatch.setattr("src.infrastructure.media.audio.downmix_to_mono_16k", _downmix)

    def _build(session: _Session, whisper: _Whisper, envelope: ChannelEnvelope | None) -> StageDeps:
        async def _envelope_of(_path: Any, **_kw: Any) -> ChannelEnvelope:
            assert envelope is not None, "огибающая не должна запрашиваться у моно-записи"
            return envelope

        monkeypatch.setattr("src.infrastructure.media.audio.channel_envelope", _envelope_of)
        return build_stage_deps(
            settings=None,
            sessionmaker=lambda: cast(AsyncSession, session),
            storage=_Storage(),
            transcription=whisper,
        )

    return _build


# --------------------------------------------------------------------------- #
# Роли
# --------------------------------------------------------------------------- #


async def test_replies_are_split_between_operator_and_client(prepared, monkeypatch) -> None:
    """Основной путь: роль реплики — по каналу с большей энергией.

    Канал 0 громче в первом окне, канал 1 — во втором; оператор в канале 0
    (замер SP-003 на десяти приветствиях).
    """
    session = _Session(_call_row())
    whisper = _Whisper((_segment(0.0, 0.5, "Добрый день"), _segment(1.0, 1.5, "Здравствуйте")))
    envelope = _envelope(
        (-20.0, -60.0),
        (-20.0, -60.0),
        (-20.0, -60.0),
        (-20.0, -60.0),
        (-20.0, -60.0),
        (-20.0, -60.0),
        (-20.0, -60.0),
        (-20.0, -60.0),
        (-20.0, -60.0),
        (-20.0, -60.0),
        (-60.0, -20.0),
        (-60.0, -20.0),
        (-60.0, -20.0),
        (-60.0, -20.0),
        (-60.0, -20.0),
        (-60.0, -20.0),
        (-60.0, -20.0),
        (-60.0, -20.0),
        (-60.0, -20.0),
        (-60.0, -20.0),
    )

    work = await TranscribeHandler().work(_job(), prepared(session, whisper, envelope))

    assert work.role_method is RoleMethod.STEREO_ENERGY
    assert [r.speaker for r in work.replies] == [Speaker.OPERATOR, Speaker.CLIENT]
    assert [(r.start_ms, r.end_ms) for r in work.replies] == [(0, 500), (1000, 1500)]


async def test_a_reply_nobody_clearly_owns_stays_unknown(prepared) -> None:
    """Говорят одновременно — роль не выдумывается.

    Приписать такую реплику оператору значит испортить статистику разговоров
    тем сильнее, чем хуже запись.
    """
    session = _Session(_call_row())
    whisper = _Whisper((_segment(0.0, 0.4, "перебивают"),))
    envelope = _envelope((-20.0, -21.0), (-20.5, -20.0), (-20.0, -20.5), (-21.0, -20.0))

    work = await TranscribeHandler().work(_job(), prepared(session, whisper, envelope))

    assert work.replies[0].speaker is Speaker.UNKNOWN
    assert work.role_method is RoleMethod.STEREO_ENERGY, "метод применён, просто не дал ответа"


async def test_the_operator_channel_is_a_setting_and_not_a_constant(prepared) -> None:
    """Запись с другой микшировкой обязана чиниться настройкой, а не правкой кода."""
    session = _Session(_call_row())
    whisper = _Whisper((_segment(0.0, 0.5, "Добрый день"),))
    envelope = _envelope(*[(-20.0, -60.0)] * 10)

    deps = prepared(session, whisper, envelope)
    swapped = build_stage_deps(
        settings=None,
        sessionmaker=deps.sessionmaker,
        storage=deps.storage,
        transcription=deps.transcription,
        runtime_settings={"stereo_operator_channel": 1},
    )

    work = await TranscribeHandler().work(_job(), swapped)

    assert work.replies[0].speaker is Speaker.CLIENT, "громкий канал 0 теперь клиент"


async def test_a_mono_recording_yields_unattributed_replies(prepared) -> None:
    """Моно — роли не получены, и метод об этом говорит.

    `none`, а не `llm_attribution`: последний означал бы уже сделанную работу,
    а её сделает этап анализа.
    """
    session = _Session(_call_row(audio_channels=1))
    whisper = _Whisper((_segment(0.0, 1.0, "один канал"),))

    work = await TranscribeHandler().work(_job(), prepared(session, whisper, None))

    assert work.role_method is RoleMethod.NONE
    assert work.replies[0].speaker is Speaker.UNKNOWN


# --------------------------------------------------------------------------- #
# Запись результата
# --------------------------------------------------------------------------- #


async def test_storing_replaces_the_previous_transcript_and_queues_analysis(prepared) -> None:
    """Повтор этапа переписывает результат целиком.

    Дописывание дало бы удвоенный разговор, а `ON CONFLICT DO UPDATE` оставило
    бы реплики предыдущего прогона рядом с новыми.
    """
    session = _Session(_call_row())
    whisper = _Whisper((_segment(0.0, 0.5, "раз"), _segment(1.0, 1.5, "два")))
    envelope = _envelope(*[(-20.0, -60.0)] * 20)
    work = await TranscribeHandler().work(_job(), prepared(session, whisper, envelope))

    session.statements.clear()
    result = await TranscribeHandler().store(PostgresStagePersistence(session), _job(), work)

    verbs = [verb for verb, _params in session.statements]
    assert verbs[0] == "DELETE", "старый транскрипт удаляется первым"
    assert verbs.count("INSERT") == 3, "транскрипт и две реплики"
    assert result.processing_status is ProcessingStatus.ANALYZING
    assert result.next_kind is JobKind.ANALYZE


async def test_the_text_is_stored_once_and_as_it_is(prepared) -> None:
    """Обезличивания нет: у реплики один текст, и это то, что дал Whisper.

    По нему же строится индекс поиска, поэтому звонок находится сразу после
    расшифровки — второго этапа перед поиском нет.
    """
    session = _Session(_call_row())
    whisper = _Whisper((_segment(0.0, 0.5, "мой номер +7 (999) 123-45-67"),))
    envelope = _envelope(*[(-20.0, -60.0)] * 10)
    work = await TranscribeHandler().work(_job(), prepared(session, whisper, envelope))

    session.statements.clear()
    await TranscribeHandler().store(PostgresStagePersistence(session), _job(), work)

    inserted = [params for verb, params in session.statements if "text_raw" in params]
    assert inserted, "реплика записана"
    assert "9991234567" in "".join(ch for ch in inserted[0]["text_raw"] if ch.isdigit()), (
        "текст сохраняется как есть"
    )
    assert "text_masked" not in inserted[0], "второй копии текста нет"


# --------------------------------------------------------------------------- #
# Отказы
# --------------------------------------------------------------------------- #


async def test_a_call_without_stored_audio_is_terminal(prepared) -> None:
    """`not_found` на расшифровке терминален: повтор аудио не вернёт."""
    session = _Session(_call_row(audio_status=AudioStatus.UNAVAILABLE_AT_SOURCE.value))

    with pytest.raises(StageError) as raised:
        await TranscribeHandler().work(_job(), prepared(session, _Whisper(), None))

    assert raised.value.error_class is JobErrorClass.NOT_FOUND


async def test_a_timeout_keeps_its_class(prepared) -> None:
    """Таймаут отличается от отказа провайдера: по классу видно, что Whisper не ответил вовремя.

    Задержка повтора у них одинаковая (`_BACKOFF_DEFAULT`), различается только
    то, что видит администратор в классе отказа.
    """
    session = _Session(_call_row())
    whisper = _Whisper(error=TranscriptionError("timeout"))

    with pytest.raises(StageError) as raised:
        await TranscribeHandler().work(
            _job(), prepared(session, whisper, _envelope((-20.0, -60.0)))
        )

    assert raised.value.error_class is JobErrorClass.TIMEOUT


#: Сколько «молчит» подставной сервер. Заведомо дольше срока вызова в тестах
#: ниже: ответ до срока означал бы, что проверяется не молчание.
SILENCE_S = 30.0


def _real_whisper(
    monkeypatch: pytest.MonkeyPatch, handler: Any, *, timeout_s: float
) -> whisper_module.SpeechTranscriptionClient:
    """Настоящий клиент конвейера, у которого подменён только транспорт.

    `_Whisper` здесь не годится: он сам бросает готовую `TranscriptionError`,
    то есть проверяет этап при условии, что клиент уже перевёл отказ в класс.
    Перевод и проверяется.
    """
    monkeypatch.setattr(
        whisper_module,
        "httpx",
        SimpleNamespace(
            AsyncClient=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler))
        ),
    )
    return whisper_module.build_whisper_client(
        SimpleNamespace(
            speech_whisper_base_url="https://whisper.invalid/v1",
            speech_whisper_timeout_seconds=timeout_s,
        )
    )


async def test_a_silent_whisper_is_a_timeout_and_not_an_unexpected_error(
    prepared, monkeypatch
) -> None:
    """Whisper не ответил за `SPEECH_WHISPER_TIMEOUT_SECONDS` — отказ `timeout`.

    Общий срок вызова срабатывает при выходе из контекста `AdapterExecution`,
    то есть уже снаружи провайдера, и оттуда выходит голый `TimeoutError`.
    Этап его не узнавал, и воркер записывал `unexpected_error` — класс, по
    которому таймаут не отличить от ошибки в нашем коде.
    """
    requests: list[httpx.Request] = []

    async def silent(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        await asyncio.sleep(SILENCE_S)
        return httpx.Response(200, json={})

    whisper = _real_whisper(monkeypatch, silent, timeout_s=0.2)
    deps = prepared(_Session(_call_row()), whisper, _envelope((-20.0, -60.0)))

    # Страховка от зависания: если срок вызова перестанет срабатывать, тест
    # упадёт здесь, а не будет ждать всё молчание сервера.
    async with asyncio.timeout(SILENCE_S / 3):
        with pytest.raises(StageError) as raised:
            await TranscribeHandler().work(_job(), deps)

    assert len(requests) == 1, "запрос дошёл до сервера: отказ именно от молчания"
    assert raised.value.error_class is JobErrorClass.TIMEOUT
    cause = raised.value.__cause__
    assert isinstance(cause, TranscriptionError)
    assert cause.reason == "timeout"


async def test_an_outside_cancellation_mid_call_stays_a_cancellation(monkeypatch, tmp_path) -> None:
    """Отмена снаружи — не таймаут Whisper, и переводиться в него не должна.

    Штатная остановка воркера (SIGTERM, SIGINT) вызов не отменяет:
    `request_stop` даёт досчитать текущее задание. Отмена приходит, когда
    закрывают сам цикл событий или задачу отменяет вызывающий код.

    Воркер пропускает `CancelledError` наверх, и задание возвращается в
    очередь по истечении аренды. Переведённая в `timeout`, отмена была бы
    проглочена: воркер записал бы отказ и продолжил брать задания вместо
    остановки.
    """
    entered = asyncio.Event()

    async def silent(_request: httpx.Request) -> httpx.Response:
        entered.set()
        await asyncio.sleep(SILENCE_S)
        return httpx.Response(200, json={})

    whisper = _real_whisper(monkeypatch, silent, timeout_s=SILENCE_S)
    audio = tmp_path / "mono.wav"
    audio.write_bytes(b"RIFFfake")

    call = asyncio.create_task(whisper.transcribe(audio))
    async with asyncio.timeout(SILENCE_S / 3):
        await entered.wait()
    call.cancel()

    with pytest.raises(asyncio.CancelledError):
        await call


async def test_an_unconfigured_whisper_is_not_retried_forever(prepared) -> None:
    """Ошибка развёртывания, а не провайдера: повторять нечего до починки."""
    session = _Session(_call_row())
    whisper = _Whisper(error=WhisperNotConfigured("нет адреса"))

    with pytest.raises(StageError) as raised:
        await TranscribeHandler().work(
            _job(), prepared(session, whisper, _envelope((-20.0, -60.0)))
        )

    assert raised.value.error_class is JobErrorClass.INVALID_OUTPUT


async def test_an_empty_transcript_is_refused(prepared) -> None:
    """Ноль сегментов у непустого файла — это отказ, а не тихий разговор."""
    session = _Session(_call_row())

    with pytest.raises(StageError) as raised:
        await TranscribeHandler().work(
            _job(), prepared(session, _Whisper(()), _envelope((-20.0, -60.0)))
        )

    assert raised.value.error_class is JobErrorClass.INVALID_OUTPUT


# --------------------------------------------------------------------------- #
# Ограничение параллелизма
# --------------------------------------------------------------------------- #


def test_the_stage_declares_its_own_concurrency_limit() -> None:
    """Whisper на стенде один, и предел объявляет этап, а не воркер.

    Замер SP-003: 1 поток — 23,8×, 2 — 24,5×, 3 — 19,8×. Значение по умолчанию
    равно измеренному потолку.
    """
    assert TranscribeHandler().slot_limit(build_stage_deps(settings=None, sessionmaker=None)) == 2


def test_the_limit_can_be_lowered_by_configuration() -> None:
    assert (
        TranscribeHandler().slot_limit(
            build_stage_deps(
                settings=None, sessionmaker=None, runtime_settings={"transcribe_concurrency": 1}
            )
        )
        == 1
    )
