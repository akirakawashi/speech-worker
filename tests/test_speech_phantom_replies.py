"""Реплики за концом записи не сохраняются.

Whisper разбирает звук окнами по тридцать секунд и дополняет последнее окно
тишиной. На этой тишине он выдумывает текст и отдаёт его сегментами с метками
за концом файла.

Замер на корпусе из шестидесяти одного настоящего разговора: 119 реплик из 2919
(4%) начинались позже конца записи. Тексты узнаваемы — «Спасибо.» 65 раз,
«Продолжение следует.» 20 раз, «я не знаю,» 20 раз, «.» 13 раз. Ни одной из них
никто не произносил, и все они лежали в поиске.

Проверяется именно это: что выдуманный хвост не доезжает до расшифровки.
"""

from __future__ import annotations

from typing import Any

import pytest

from src.application.dto.pipeline_input import CallForTranscription
from src.application.pipeline.transcribe import TranscribeHandler
from src.infrastructure.media.audio import ChannelEnvelope


class _Segment:
    """Сегмент в том виде, в каком его отдаёт клиент распознавания."""

    def __init__(self, start_s: float, end_s: float, text: str) -> None:
        self.start_s = start_s
        self.end_s = end_s
        self.text = text


class _Job:
    call_id = "11111111-2222-3333-4444-555555555555"


def _envelope(duration_s: float, *, channels: int = 2, window_s: float = 0.1) -> ChannelEnvelope:
    """Огибающая нужной длины. Уровни одинаковые — роль здесь не проверяется."""
    # `channels` у огибающей выводится из числа рядов, а не задаётся: ряд на
    # канал — это и есть канал.
    windows = int(duration_s / window_s)
    return ChannelEnvelope(
        window_s=window_s,
        levels=tuple(tuple([-20.0] * windows) for _ in range(channels)),
    )


def _cut(
    segments: list[_Segment],
    *,
    envelope: ChannelEnvelope | None,
    call: dict[str, Any] | None = None,
) -> list[str]:
    kept = TranscribeHandler()._drop_past_the_end(  # noqa: SLF001
        tuple(segments),  # type: ignore[arg-type]
        envelope,
        CallForTranscription(
            session_id="test",
            audio_status="stored",
            audio_object_key="test",
            duration_s=(call or {}).get("duration_s"),
        ),
        _Job(),  # type: ignore[arg-type]
    )
    return [segment.text for segment in kept]


def test_the_padded_tail_is_dropped() -> None:
    """Ровно тот случай, что найден на корпусе: хвост на тридцать секунд вперёд."""
    segments = [
        _Segment(130.0, 133.0, "До свидания."),
        _Segment(140.0, 142.0, "Спасибо."),
        _Segment(150.0, 152.0, "Продолжение следует."),
    ]

    assert _cut(segments, envelope=_envelope(135.5)) == ["До свидания."]


def test_a_reply_that_straddles_the_end_survives() -> None:
    """Речь могла идти до последнего мгновения записи.

    Отсекается по началу, а не по концу: реплика, начавшаяся до конца файла,
    была произнесена — обрезана у неё только запись.
    """
    segments = [_Segment(134.0, 138.0, "…всего доброго")]

    assert _cut(segments, envelope=_envelope(135.5)) == ["…всего доброго"]


def test_everything_inside_the_recording_survives() -> None:
    segments = [_Segment(0.0, 2.0, "Здравствуйте"), _Segment(10.0, 12.0, "Слушаю вас")]

    assert len(_cut(segments, envelope=_envelope(135.5))) == 2


def test_the_ingest_measurement_is_used_when_there_is_no_envelope() -> None:
    """Моно-запись огибающей не даёт, а длительность у нас всё равно измерена.

    Без запасного источника моно-записи сохраняли бы выдуманный хвост целиком —
    а именно у них ролей нет и без того.
    """
    segments = [_Segment(50.0, 52.0, "реальное"), _Segment(70.0, 72.0, "Спасибо.")]

    assert _cut(segments, envelope=None, call={"duration_s": 60}) == ["реальное"]


def test_whisper_own_duration_is_never_trusted() -> None:
    """Он сообщает длину **дополненного** окна — ту самую границу, которую мы отвергаем.

    Проверяется тем, что ответ Whisper сюда не передаётся вовсе: у метода два
    источника, и оба наши.
    """
    import inspect  # noqa: PLC0415

    signature = inspect.signature(TranscribeHandler._measured_duration)  # noqa: SLF001

    assert set(signature.parameters) == {"envelope", "call"}


def test_without_any_measurement_nothing_is_cut_off_and_it_is_said_aloud(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Молчаливый пропуск проверки хуже: выдуманный хвост обнаружится в поиске.

    Резать при этом не по чему — выбросить всё было бы хуже, чем сохранить
    лишнее.
    """
    segments = [_Segment(0.0, 2.0, "речь"), _Segment(70.0, 72.0, "Спасибо.")]

    with caplog.at_level("WARNING"):
        kept = _cut(segments, envelope=None, call={"duration_s": None})

    assert len(kept) == 2
    assert "phantom" in caplog.text


def test_an_empty_envelope_falls_back_to_the_stored_duration() -> None:
    """Огибающая нулевой длины — это отказ измерения, а не запись без звука."""
    segments = [_Segment(10.0, 12.0, "речь"), _Segment(70.0, 72.0, "Спасибо.")]

    kept = _cut(segments, envelope=_envelope(0.0), call={"duration_s": 60})

    assert kept == ["речь"]
