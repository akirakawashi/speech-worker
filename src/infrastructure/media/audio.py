"""Что на самом деле лежит в записи и как подать её дальше.

Четыре операции, и все — через `ffmpeg`, потому что он уже стоит в образе
воркера и потому что альтернатива требует numpy или soundfile ради того, что
внешний бинарь делает быстрее:

* `probe_audio` — сколько каналов, какая длительность (нужно этапу скачивания);
* `downmix_to_mono_16k` — вход для Whisper;
* `split_channels` — две ноги звонка по отдельным файлам, для двухпроходной
  расшифровки;
* `channel_envelope` — энергия по каналам во времени, по которой реплика
  относится к оператору или к клиенту.

Почему вообще нужен `ffprobe`, если источник сообщает длительность: он её
сообщает **не всегда** — локальная выгрузка знает только дату, — а число
каналов не сообщает никогда. Между тем определение ролей построено на том, что
запись стерео с двумя ногами звонка (замер SP-002). Моно-запись, приехавшая
незамеченной, дала бы транскрипт, в котором все реплики принадлежат одному
собеседнику, и это выглядело бы как плохое качество распознавания, а не как
запись другого формата.

Огибающая считается **одним** запуском `ffmpeg` на звонок, а не одним на
реплику. Реплик в часовом разговоре сотни; процесс на каждую превратил бы
дешёвую операцию в основную статью расходов этапа.
"""

from __future__ import annotations

import asyncio
import json
import re
import shutil
from pathlib import Path
from typing import Final

from src.application.dto.media import (
    AudioProbe as AudioProbe,
    ChannelEnvelope as ChannelEnvelope,
)
from src.application.errors.external import (
    MediaError as MediaError,
)

#: Достаточно, чтобы `ffprobe` разобрал заголовки и досчитал длительность
#: MP3 с постоянным битрейтом. Дольше — значит `ffprobe` завис на битом файле,
#: а этап должен отказать, а не удерживать воркер.
PROBE_TIMEOUT_SECONDS: Final[float] = 30.0

#: Перекодирование читает файл целиком, поэтому окно шире. Часовая запись
#: 24 кбит/с пересобирается секунды, но запас нужен на загруженный узел.
CONVERT_TIMEOUT_SECONDS: Final[float] = 300.0

FFPROBE: Final[str] = "ffprobe"
FFMPEG: Final[str] = "ffmpeg"

#: Частота, которую ждёт Whisper. Записи телефонные, 8 кГц, и повышение до 16
#: информации не добавляет — оно лишь избавляет модель от собственного
#: ресемплинга с неизвестными параметрами.
WHISPER_SAMPLE_RATE: Final[int] = 16000

#: Окно огибающей. Сто миллисекунд — компромисс: реплики измеряются секундами,
#: поэтому мельче не нужно, а крупнее начинает смазывать короткие «да»/«ага»
#: на границе смены говорящего.
ENVELOPE_WINDOW_S: Final[float] = 0.1

#: На сколько децибел один канал должен превосходить другой, чтобы реплику
#: отнесли к нему. Три децибела — двукратная разница по мощности.
#:
#: Порог существует не ради строгости, а ради честного отказа: когда говорят
#: одновременно или когда запись оказалась моно, разница исчезает, и назвать
#: победителя нельзя. Тогда возвращается `None`, и вызывающий переходит к
#: запасному методу (`stereo_two_pass`, `llm_attribution`), а не выдаёт
#: угаданную роль за измеренную.
DOMINANCE_MARGIN_DB: Final[float] = 3.0

_METADATA_LINE = re.compile(r"^lavfi\.astats\.(?P<channel>\d+)\.RMS_level=(?P<value>\S+)$")
_FRAME_LINE = re.compile(r"^frame:\d+\s+pts:\S+\s+pts_time:(?P<time>\S+)")


async def probe_audio(path: Path) -> AudioProbe:
    """Разобрать файл `ffprobe`.

    Отказ, а не умолчания. Файл, о котором ничего не известно, нельзя объявить
    моно-записью на ноль секунд: такое значение выглядит как измерение и
    поедет дальше по конвейеру наравне с настоящими.
    """
    stdout = await _run(
        FFPROBE,
        "-v",
        "error",
        "-show_streams",
        "-show_format",
        "-select_streams",
        "a:0",
        "-of",
        "json",
        str(path),
        timeout=PROBE_TIMEOUT_SECONDS,
    )
    return _parse(stdout)


async def _run(tool: str, *args: str, timeout: float) -> bytes:
    """Запустить `ffmpeg`/`ffprobe` и вернуть его stdout.

    Отсутствие бинаря — отдельная ошибка с объяснением, а не `FileNotFoundError`
    из недр `asyncio`: образ без `ffmpeg` собирается зелёным, и узнать об этом
    надо на первом же звонке, а не на разборе трейсбека.
    """
    if shutil.which(tool) is None:
        raise MediaError(
            f"{tool} is not installed in this image: the pipeline cannot tell a stereo "
            "recording from a mono one, and role attribution depends on that"
        )

    process = await asyncio.create_subprocess_exec(
        tool,
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except TimeoutError:
        process.kill()
        await process.wait()
        raise MediaError(f"{tool} did not finish within {timeout:g}s") from None

    if process.returncode != 0:
        # Код и текст, но текст обрезан: `ffmpeg` на битом файле выливает
        # килобайты, а содержательного там — первая строка. Ни ключей, ни
        # адресов в его выводе не бывает: он видит только путь к файлу.
        raise MediaError(f"{tool} refused the file: {stderr.decode('utf-8', 'replace')[:200]}")
    return stdout


def _parse(raw: bytes) -> AudioProbe:
    """Разобрать ответ в проверенную структуру по контракту текущего модуля."""
    try:
        payload = json.loads(raw or b"{}")
    except json.JSONDecodeError as exc:
        raise MediaError("ffprobe returned something that is not JSON") from exc

    streams = payload.get("streams") or []
    if not streams:
        raise MediaError("file has no audio stream")
    stream = streams[0]
    fmt = payload.get("format") or {}

    # Длительность стоит и у потока, и у контейнера, и совпадают они не всегда.
    # Берётся первое непустое: у MP3 без заголовка `Xing` длительность потока
    # отсутствует, а контейнерная досчитывается по размеру и битрейту.
    duration = _first_float(stream.get("duration"), fmt.get("duration"))
    if duration is None:
        raise MediaError("ffprobe reported no duration for the audio stream")

    channels = _as_int(stream.get("channels"))
    if channels is None or channels < 1:
        raise MediaError("ffprobe reported no channel count for the audio stream")

    return AudioProbe(
        duration_s=duration,
        channels=channels,
        sample_rate=_as_int(stream.get("sample_rate")),
        bit_rate=_as_int(stream.get("bit_rate")) or _as_int(fmt.get("bit_rate")),
        codec=stream.get("codec_name") or None,
    )


def _first_float(*values: object) -> float | None:
    """Выбрать первое доступное числовое значение измерения."""
    for value in values:
        try:
            number = float(str(value))
        except (TypeError, ValueError):
            continue
        if number > 0:
            return number
    return None


def _as_int(value: object) -> int | None:
    """Прочитать целое значение измерения; отсутствие обозначить None."""
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# Подготовка входа для распознавания
# --------------------------------------------------------------------------- #


async def downmix_to_mono_16k(src: Path, dst: Path) -> Path:
    """Свести запись в моно 16 кГц — так её ждёт Whisper.

    Сведение, а не выбор одного канала: в моно-дорожке слышны оба собеседника,
    и распознаётся весь разговор. Роли на этом файле не определяются — для
    этого есть огибающая, которая считается по исходному стерео.
    """
    await _run(
        FFMPEG,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(src),
        "-ac",
        "1",
        "-ar",
        str(WHISPER_SAMPLE_RATE),
        str(dst),
        timeout=CONVERT_TIMEOUT_SECONDS,
    )
    return _written(dst, "downmix")


async def split_channels(src: Path, left: Path, right: Path) -> tuple[Path, Path]:
    """Разложить стерео на два файла — по ноге звонка в каждом.

    Нужно запасному методу ролей (`stereo_two_pass`): каждый канал
    распознаётся отдельно, и принадлежность реплики известна из того, какой
    файл её дал, а не выведена из энергии. Дороже вдвое по времени GPU, поэтому
    основным не является.

    Оба файла пишутся **одним** запуском: два запуска декодировали бы один и
    тот же MP3 дважды.
    """
    await _run(
        FFMPEG,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(src),
        "-filter_complex",
        "[0:a]channelsplit=channel_layout=stereo[l][r]",
        "-map",
        "[l]",
        "-ac",
        "1",
        "-ar",
        str(WHISPER_SAMPLE_RATE),
        str(left),
        "-map",
        "[r]",
        "-ac",
        "1",
        "-ar",
        str(WHISPER_SAMPLE_RATE),
        str(right),
        timeout=CONVERT_TIMEOUT_SECONDS,
    )
    return _written(left, "left channel"), _written(right, "right channel")


def _written(path: Path, what: str) -> Path:
    """`ffmpeg` вернул ноль — но файл всё равно надо увидеть.

    Нулевой код с пустым файлом на выходе бывает: фильтр отработал, писать было
    нечего. Дальше по конвейеру это выглядит как беззвучная запись, а не как
    несделанная работа.
    """
    if not path.is_file() or path.stat().st_size == 0:
        raise MediaError(f"ffmpeg produced no {what}: {path}")
    return path


# --------------------------------------------------------------------------- #
# Энергия каналов во времени
# --------------------------------------------------------------------------- #


async def channel_envelope(src: Path, *, window_s: float = ENVELOPE_WINDOW_S) -> ChannelEnvelope:
    """Посчитать огибающую по каналам одним проходом `ffmpeg`.

    Окна задаются `asetnsamples`, а не берутся из кадров кодека: размер кадра
    MP3 зависит от частоты дискретизации, и окно «как получится» невозможно
    сопоставить с таймкодами реплик.
    """
    if window_s <= 0:
        raise ValueError("window_s must be positive")
    samples = max(1, round(window_s * WHISPER_SAMPLE_RATE))
    stdout = await _run(
        FFMPEG,
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(src),
        "-af",
        (
            f"aresample={WHISPER_SAMPLE_RATE},asetnsamples=n={samples},"
            "astats=metadata=1:reset=1:measure_perchannel=RMS_level:measure_overall=none,"
            "ametadata=print:file=-"
        ),
        "-f",
        "null",
        "-",
        timeout=CONVERT_TIMEOUT_SECONDS,
    )
    return _parse_envelope(stdout, window_s)


def _parse_envelope(raw: bytes, window_s: float) -> ChannelEnvelope:
    """Преобразовать измерения энергии каналов в огибающую записи."""
    rows: dict[int, list[float]] = {}
    seen_frame = False
    for line in raw.decode("utf-8", "replace").splitlines():
        if _FRAME_LINE.match(line):
            seen_frame = True
            continue
        match = _METADATA_LINE.match(line)
        if match is None:
            continue
        # Каналы `astats` нумеруются с единицы, наши — с нуля: номер канала
        # уезжает в настройку `stereo_operator_channel`, и смещение на единицу
        # там означало бы перепутанные роли во всём архиве.
        channel = int(match.group("channel")) - 1
        rows.setdefault(channel, []).append(float(match.group("value")))

    if not rows:
        raise MediaError(
            "ffmpeg produced no per-channel levels"
            + ("" if seen_frame else ": no frames at all, the file may be empty")
        )
    width = max(len(row) for row in rows.values())
    return ChannelEnvelope(
        window_s=window_s,
        # Хвост добивается тишиной: у последнего окна канала могут разойтись на
        # одно значение, и рваные ряды сделали бы индекс окна неоднозначным.
        levels=tuple(
            tuple(rows[ch] + [float("-inf")] * (width - len(rows[ch]))) for ch in sorted(rows)
        ),
    )
