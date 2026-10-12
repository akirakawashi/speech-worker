"""Разбор аудиофайла.

Две части, и разделены они намеренно. Толкование ответа `ffprobe` — чистая
функция, и проверяется всегда, на этой машине в том числе. Сам `ffprobe` стоит
только в образе воркера; тесты, которым он нужен, здесь помечены и выполняются
в контейнере — там же, где этот код и работает.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
from pathlib import Path

import pytest

from src.infrastructure.media.audio import (
    MediaError,
    _parse,
    _parse_envelope,
    channel_envelope,
    downmix_to_mono_16k,
    probe_audio,
    split_channels,
)

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")

_HAS_FFMPEG = shutil.which("ffprobe") is not None and shutil.which("ffmpeg") is not None
needs_ffmpeg = pytest.mark.skipif(
    not _HAS_FFMPEG,
    reason="ffprobe/ffmpeg live in the worker image; run this file there",
)


def _probe_json(**stream: object) -> bytes:
    payload = {
        "streams": [{"channels": 2, "duration": "12.5", **stream}],
        "format": {"duration": "12.5", "bit_rate": "24000"},
    }
    return json.dumps(payload).encode()


def test_a_stereo_recording_is_recognised_as_stereo() -> None:
    """Признак, на котором держится определение ролей.

    Моно-запись, проехавшая незамеченной, дала бы транскрипт, где все реплики
    принадлежат одному собеседнику — а выглядело бы это как плохое качество
    распознавания.
    """
    probe = _parse(_probe_json(sample_rate="8000", codec_name="mp3"))

    assert probe.is_stereo is True
    assert probe.channels == 2
    assert probe.duration_s == pytest.approx(12.5)
    assert probe.sample_rate == 8000
    assert probe.codec == "mp3"


def test_a_mono_recording_is_not_called_stereo() -> None:
    assert _parse(_probe_json(channels=1)).is_stereo is False


def test_the_container_duration_covers_a_stream_that_has_none() -> None:
    """У MP3 без заголовка `Xing` длительности потока нет.

    Контейнерная досчитывается по размеру и битрейту — и это не запасной
    вариант «на всякий случай», а обычный случай для телефонных записей.
    """
    raw = json.dumps({"streams": [{"channels": 2}], "format": {"duration": "7.25"}}).encode()

    assert _parse(raw).duration_s == pytest.approx(7.25)


@pytest.mark.parametrize(
    ("raw", "because"),
    [
        (b"{}", "нет ни одного потока"),
        (b'{"streams": []}', "нет ни одного потока"),
        (b'{"streams": [{"channels": 2}]}', "длительность неизвестна"),
        (b'{"streams": [{"duration": "5"}]}', "число каналов неизвестно"),
        (b'{"streams": [{"channels": 0, "duration": "5"}]}', "нулевых каналов не бывает"),
        (b"not json at all", "ответ не разбирается"),
    ],
    ids=lambda v: str(v)[:40],
)
def test_a_file_that_cannot_be_measured_is_refused(raw: bytes, because: str) -> None:
    """Отказ, а не умолчания.

    Моно-запись на ноль секунд выглядит как измерение и поедет дальше по
    конвейеру наравне с настоящими — а это хуже отказа, потому что незаметно.
    """
    with pytest.raises(MediaError):
        _parse(raw)


def test_the_absence_of_ffprobe_is_an_error_and_not_a_guess(monkeypatch) -> None:
    """Образ без `ffprobe` обязан отказать на первом же звонке.

    Иначе этап тихо соберёт записи, о которых ничего не известно, и обнаружится
    это на расшифровке — через несколько часов и уже на всём суточном пакете.
    """
    monkeypatch.setattr("src.infrastructure.media.audio.shutil.which", lambda _name: None)

    with pytest.raises(MediaError, match="ffprobe"):
        asyncio.run(probe_audio(Path("whatever.mp3")))


@needs_ffmpeg
def test_a_real_stereo_file_measures_as_stereo(tmp_path: Path) -> None:
    """Настоящий `ffprobe` над настоящим файлом.

    Файл синтезируется здесь же, в параметрах измеренной выгрузки NCC
    (8 кГц, 24 кбит/с, два канала), а не берётся из подборки: в подборке лежат
    разговоры реальных клиентов, и тестам они не принадлежат.
    """
    path = tmp_path / "sample.mp3"
    subprocess.run(
        [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:duration=3",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=660:duration=3",
            "-filter_complex",
            "[0:a][1:a]amerge=inputs=2[a]",
            "-map",
            "[a]",
            "-ar",
            "8000",
            "-b:a",
            "24k",
            str(path),
        ],
        check=True,
        capture_output=True,
    )

    probe = asyncio.run(probe_audio(path))

    assert probe.is_stereo is True
    assert probe.channels == 2
    assert probe.duration_s == pytest.approx(3.0, abs=0.3)
    assert probe.sample_rate == 8000


@needs_ffmpeg
def test_a_file_that_is_not_audio_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "not-audio.mp3"
    path.write_bytes(b"this is not a recording, it is a sentence")

    with pytest.raises(MediaError):
        asyncio.run(probe_audio(path))


# --------------------------------------------------------------------------- #
# Огибающая: разбор и арифметика — без ffmpeg
# --------------------------------------------------------------------------- #


def _astats(*windows: tuple[float, ...]) -> bytes:
    """Вывод `ametadata=print` для заданных окон.

    Каналы в `astats` нумеруются **с единицы** — так и здесь, потому что
    проверяется в том числе пересчёт нумерации.
    """
    lines = []
    for index, levels in enumerate(windows):
        lines.append(f"frame:{index}    pts:{index * 1600}       pts_time:{index * 0.1}")
        for channel, level in enumerate(levels, start=1):
            lines.append(f"lavfi.astats.{channel}.RMS_level={level}")
    return "\n".join(lines).encode()


def test_channel_numbering_starts_at_zero_for_us_and_at_one_for_ffmpeg() -> None:
    """Смещение на единицу здесь означало бы перепутанные роли во всём архиве.

    Номер канала уезжает в настройку `stereo_operator_channel`, и по нему
    реплики раскладываются на оператора и клиента.
    """
    envelope = _parse_envelope(_astats((-20.0, -60.0)), 0.1)

    assert envelope.channels == 2
    assert envelope.levels[0] == (-20.0,), "первый канал ffmpeg — нулевой у нас"
    assert envelope.levels[1] == (-60.0,)


def test_silence_arrives_as_minus_infinity_and_is_not_an_error() -> None:
    """`-inf` — правильное значение, а не отсутствующее: сигнала в окне нет."""
    envelope = _parse_envelope(_astats((-20.0, float("-inf"))), 0.1)

    assert envelope.levels[1][0] == float("-inf")
    assert envelope.mean_power(1, 0.0, 0.1) == 0.0


def test_a_ragged_tail_is_padded_with_silence() -> None:
    """У последнего окна каналы могут разойтись на одно значение.

    Рваные ряды сделали бы индекс окна неоднозначным: одно и то же время
    указывало бы на разные окна разных каналов.
    """
    raw = b"\n".join(
        [
            b"frame:0    pts:0       pts_time:0",
            b"lavfi.astats.1.RMS_level=-20.000000",
            b"lavfi.astats.2.RMS_level=-30.000000",
            b"frame:1    pts:1600    pts_time:0.1",
            b"lavfi.astats.1.RMS_level=-25.000000",
        ]
    )

    envelope = _parse_envelope(raw, 0.1)

    assert len(envelope.levels[0]) == len(envelope.levels[1]) == 2
    assert envelope.levels[1][1] == float("-inf")


def test_an_empty_measurement_is_refused() -> None:
    with pytest.raises(MediaError):
        _parse_envelope(b"", 0.1)


def test_power_is_averaged_and_not_decibels() -> None:
    """Среднее децибел — среднее геометрическое, и это не то, что нужно.

    Одно окно тишины (`-inf`) посреди речи обратило бы среднее в минус
    бесконечность, то есть реплика с паузой посередине стала бы «тишиной».
    """
    envelope = _parse_envelope(_astats((-20.0, -20.0), (float("-inf"), -20.0), (-20.0, -20.0)), 0.1)

    loud = envelope.mean_power(0, 0.0, 0.3)

    assert loud > 0.0, "пауза не должна обнулять реплику целиком"
    assert loud == pytest.approx(2 / 3 * 10 ** (-2.0))


def test_the_louder_channel_wins_a_reply() -> None:
    envelope = _parse_envelope(_astats((-20.0, -60.0), (-21.0, -58.0)), 0.1)

    assert envelope.dominant_channel(0.0, 0.2) == 0


def test_channels_too_close_to_call_return_no_answer() -> None:
    """Говорят одновременно — честнее не ответить.

    Подставив нулевой канал по умолчанию, мы отдали бы весь такой разговор
    оператору. Вызывающий обязан перейти к запасному методу.
    """
    envelope = _parse_envelope(_astats((-20.0, -21.0), (-20.5, -20.0)), 0.1)

    assert envelope.dominant_channel(0.0, 0.2) is None


def test_a_mono_recording_has_no_dominant_channel() -> None:
    """Не «канал ноль», а «ответа нет»: делить в моно нечего."""
    envelope = _parse_envelope(_astats((-20.0,), (-22.0,)), 0.1)

    assert envelope.channels == 1
    assert envelope.dominant_channel(0.0, 0.2) is None


def test_silence_in_both_channels_has_no_dominant_channel() -> None:
    envelope = _parse_envelope(_astats((float("-inf"), float("-inf"))), 0.1)

    assert envelope.dominant_channel(0.0, 0.1) is None


def test_a_reply_starting_mid_window_still_counts_that_window() -> None:
    """Окно, в котором реплика началась, в неё входит.

    Иначе короткое «да» на границе окна теряло бы единственное окно, в котором
    оно вообще звучало.
    """
    envelope = _parse_envelope(_astats((-20.0, -60.0), (-20.0, -60.0), (-20.0, -60.0)), 0.1)

    assert envelope.window_range(0.05, 0.15) == (0, 2)
    assert envelope.window_range(0.0, 0.0) == (0, 1), "нулевая длина — всё равно одно окно"


def test_asking_for_a_channel_that_does_not_exist_is_an_error() -> None:
    envelope = _parse_envelope(_astats((-20.0, -60.0)), 0.1)

    with pytest.raises(IndexError):
        envelope.mean_power(5, 0.0, 0.1)


# --------------------------------------------------------------------------- #
# Огибающая и конвертация на настоящем ffmpeg
# --------------------------------------------------------------------------- #


def _stereo_file(path: Path, *, left: str, right: str, seconds: int = 2) -> Path:
    """Стерео, где каналы заданы по отдельности.

    `sine` — тон, `anullsrc` — тишина. Это и есть «синтетическое стерео» из
    критерия готовности SP-301.
    """
    subprocess.run(
        [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"{left}:duration={seconds}" if "sine" in left else left,
            "-f",
            "lavfi",
            "-i",
            f"{right}:duration={seconds}" if "sine" in right else right,
            "-filter_complex",
            "[0:a][1:a]amerge=inputs=2[a]",
            "-map",
            "[a]",
            "-t",
            str(seconds),
            "-ar",
            "8000",
            "-b:a",
            "24k",
            str(path),
        ],
        check=True,
        capture_output=True,
    )
    return path


@needs_ffmpeg
@pytest.mark.parametrize(("tone_channel", "expected"), [(0, 0), (1, 1)])
def test_the_channel_carrying_the_tone_is_the_one_attributed(
    tmp_path: Path, tone_channel: int, expected: int
) -> None:
    """Критерий готовности SP-301: тон в одном канале, тишина в другом.

    Проверяются оба положения, а не одно: метод, всегда возвращающий нулевой
    канал, прошёл бы проверку с тоном слева и провалил бы её с тоном справа.
    """
    tone, silence = "sine=frequency=440", "anullsrc=r=8000:cl=mono"
    path = _stereo_file(
        tmp_path / "split.mp3",
        left=tone if tone_channel == 0 else silence,
        right=silence if tone_channel == 0 else tone,
    )

    envelope = asyncio.run(channel_envelope(path))

    assert envelope.channels == 2
    assert envelope.dominant_channel(0.5, 1.5) == expected


@needs_ffmpeg
def test_the_envelope_covers_the_whole_recording(tmp_path: Path) -> None:
    path = _stereo_file(
        tmp_path / "both.mp3", left="sine=frequency=440", right="sine=frequency=660", seconds=3
    )

    envelope = asyncio.run(channel_envelope(path))

    assert envelope.duration_s == pytest.approx(3.0, abs=0.3)
    assert envelope.dominant_channel(0.5, 2.5) is None, "оба канала звучат — победителя нет"


@needs_ffmpeg
def test_the_downmix_is_what_whisper_expects(tmp_path: Path) -> None:
    path = _stereo_file(tmp_path / "src.mp3", left="sine=frequency=440", right="sine=frequency=660")

    mono = asyncio.run(downmix_to_mono_16k(path, tmp_path / "mono.wav"))
    probe = asyncio.run(probe_audio(mono))

    assert probe.channels == 1
    assert probe.sample_rate == 16000
    assert probe.duration_s == pytest.approx(2.0, abs=0.3)


@needs_ffmpeg
def test_splitting_puts_each_leg_of_the_call_in_its_own_file(tmp_path: Path) -> None:
    """Запасной метод ролей: принадлежность известна из файла, а не выведена."""
    path = _stereo_file(
        tmp_path / "src.mp3", left="sine=frequency=440", right="anullsrc=r=8000:cl=mono"
    )

    left, right = asyncio.run(split_channels(path, tmp_path / "l.wav", tmp_path / "r.wav"))

    assert asyncio.run(probe_audio(left)).channels == 1
    assert asyncio.run(probe_audio(right)).channels == 1
    # Тон был слева: в левом файле сигнал есть, в правом — тишина.
    assert asyncio.run(channel_envelope(left)).mean_power(0, 0.5, 1.5) > 0.0
    assert asyncio.run(channel_envelope(right)).mean_power(0, 0.5, 1.5) == pytest.approx(
        0.0, abs=1e-9
    )
