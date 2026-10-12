"""Источник звонков Телфина — порт `DailyBatchSource` (фича 14, 7.2).

Что берётся (S-36): исходящие вызовы АТС, соединённые с клиентом, с записью и
разговором не короче порога; кроме служебных добавочных и группы
«Переадресации». Всё это — настройки (`SPEECH_TELPHIN_*`), а не код.

Запись качается по `storage_url` из списка — постоянной ссылке хранилища без
аутентификации: так советует руководство (с. 163), и так скачивание не тратит
лимит API, общий на все приложения клиента. Ссылка — секрет: приём кладёт её в
отдельную колонку, этап `ingest` стирает после скачивания, в журналы и тексты
ошибок она не попадает.

Сутки — московские: `call_history` фильтрует по времени начала вызова в UTC,
и граница суток переводится так же, как у NCC. Проверено разведкой: за 25.09
вызовов столько же, сколько в `calls/stats` тех же суток.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from collections.abc import AsyncIterator, Iterable, Sequence
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any, Final
from zoneinfo import ZoneInfo

import httpx

from src.application.dto.calls import CallSource
from src.infrastructure.sources.ncc.source import CallRef, RecordingNotAvailable
from src.infrastructure.sources.telphin.client import TelphinClient, TelphinError

if TYPE_CHECKING:  # pragma: no cover
    from src.setting.config import Settings

logger = logging.getLogger(__name__)

#: Проект звонков Телфина: ключ — устойчивый, название — для человека.
PROJECT_KEY: Final[str] = "telphin:shpd_outbound"
PROJECT_TITLE: Final[str] = "Телфин · исходящие ШПД"

#: Префикс `session_id`: не пересекается с `node_…` NCC и читается глазами.
SESSION_PREFIX: Final[str] = "telphin:"

#: Приставка в названиях добавочных операторов («ГИК Фамилия Имя Отчество»):
#: название АТС, а не часть имени.
_NAME_PREFIX = re.compile(r"^\s*ГИК\s+")

_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def day_bounds(day: date, timezone: str) -> tuple[str, str]:
    """Сутки часового пояса колл-центра — интервалом `call_history` в UTC, включительно."""
    start = datetime.combine(day, time.min, tzinfo=ZoneInfo(timezone)).astimezone(UTC)
    end = start + timedelta(days=1) - timedelta(seconds=1)
    return start.strftime(_TIME_FORMAT), end.strftime(_TIME_FORMAT)


def parse_utc(raw: object) -> datetime | None:
    """`2026-09-25 06:04:06` (UTC, дробная часть возможна) — момент с поясом."""
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        return datetime.strptime(raw.strip().split(".")[0], _TIME_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        return None


def client_number(raw: object) -> str | None:
    """Номер клиента к одному виду: 10 цифр без кода страны — российский мобильный.

    Телфин отдаёт номер то `9XXXXXXXXX`, то `+7XXXXXXXXXX` (разведка: 85 % и
    15 %). Без приведения один клиент получил бы два псевдонима, а маска —
    чужой код страны.
    """
    if not isinstance(raw, str):
        return None
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10 and digits.startswith("9"):
        return "7" + digits
    return raw.strip() or None


def operator_name(raw: object) -> str | None:
    """Прочитать подпись сотрудника из метаданных звонка источника."""
    if not isinstance(raw, str):
        return None
    return _NAME_PREFIX.sub("", raw).strip() or None


def _names(raw: str) -> frozenset[str]:
    """Прочитать доступные подписи участников из метаданных источника."""
    return frozenset(part.strip() for part in raw.split(",") if part.strip())


def _ids(raw: str) -> frozenset[int]:
    """Прочитать идентификаторы записей из ответа источника."""
    return frozenset(int(part) for part in _names(raw))


def _recording(parts: Iterable[Any]) -> dict[str, Any] | None:
    """Часть вызова с записью; у вызова с переводами — самая длинная."""
    recorded = [
        part
        for part in parts
        if isinstance(part, dict) and part.get("record_uuid") and part.get("storage_url")
    ]
    if not recorded:
        return None
    return max(
        recorded,
        key=lambda part: (int(part.get("duration") or 0), int(part.get("record_file_size") or 0)),
    )


class TelphinSource:
    """Исходящие ШПД с ВАТС Телфина.

    Без клиента API источник умеет только отдавать записи — так его держит
    воркер: перечислять звонки ему не нужно, и App Secret ему не нужен тоже.
    """

    name = "telphin"

    #: Страница — тысяча вызовов (разведка: принимается), сутки — до трёх
    #: страниц. Пятьдесят — запас на порядок и защита от сервера, однажды
    #: начавшего отдавать непустую страницу бесконечно.
    PER_PAGE = 1000
    MAX_PAGES = 50

    def __init__(
        self,
        client: TelphinClient | None,
        *,
        timezone: str = "Europe/Moscow",
        min_talk_s: int = 10,
        excluded_extensions: frozenset[str] = frozenset(),
        excluded_groups: frozenset[int] = frozenset(),
        timeout_s: float = 60.0,
        download: httpx.AsyncClient | None = None,
    ) -> None:
        """Настроить TelphinSource из переданных параметров и зависимостей."""
        self._client = client
        self._timezone = timezone
        self._min_talk_s = min_talk_s
        self._excluded_extensions = excluded_extensions
        self._excluded_groups = excluded_groups
        self._timeout_s = timeout_s
        self._download = download

    async def list_calls(self, day: date) -> Sequence[CallRef]:
        """Перечислить метаданные звонков указанного дня без загрузки записей."""
        if self._client is None:
            raise RuntimeError(
                "telphin source has no API client: SPEECH_TELPHIN_APP_ID and "
                "SPEECH_TELPHIN_APP_SECRET are required to list calls"
            )
        start, end = day_bounds(day, self._timezone)
        refs: dict[str, CallRef] = {}
        skipped: Counter[str] = Counter()
        seen = 0
        for page in range(1, self.MAX_PAGES + 1):
            items = await self._client.call_history(
                start=start, end=end, page=page, per_page=self.PER_PAGE
            )
            seen += len(items)
            for item in items:
                ref, reason = self.call_ref(item, day)
                if ref is None:
                    skipped[reason or "other"] += 1
                else:
                    refs.setdefault(ref.session_id, ref)
            if len(items) < self.PER_PAGE:
                break
        else:
            raise TelphinError(f"telphin call_history: more than {self.MAX_PAGES} pages for {day}")
        logger.info(
            "telphin %s: %s outbound calls, taken %s, left out %s",
            day.isoformat(),
            seen,
            len(refs),
            dict(skipped),
        )
        return list(refs.values())

    def call_ref(self, item: dict[str, Any], day: date) -> tuple[CallRef | None, str | None]:
        """Вызов `call_history` — звонок приёма, или причина, почему нет."""
        if item.get("flow") != "out":
            return None, "not_outbound"
        if item.get("result") != "bridged":
            return None, "not_bridged"
        talk = int(item.get("bridged_duration") or 0)
        if talk < self._min_talk_s:
            return None, "short"
        extension = str(item.get("extension_name") or "").split("@")[0]
        if extension in self._excluded_extensions:
            return None, "excluded_extension"
        group = item.get("extension_group_owner_id")
        if group is not None and int(group) in self._excluded_groups:
            return None, "excluded_group"
        uuid = str(item.get("call_uuid") or "").strip()
        started = parse_utc(item.get("init_time_gmt")) or parse_utc(item.get("start_time_gmt"))
        if not uuid or started is None:
            return None, "malformed"
        recording = _recording(item.get("cdr") or ())
        if recording is None:
            return None, "no_record"
        ended = parse_utc(item.get("hangup_time_gmt"))
        if ended is None or ended < started:
            ended = started + timedelta(seconds=talk)
        return (
            CallRef(
                session_id=SESSION_PREFIX + uuid,
                audio_locator=str(recording["record_uuid"]),
                day=day,
                started_at=started,
                ended_at=ended,
                duration_s=talk,
                operator_login=extension or None,
                operator_name=operator_name(item.get("from_screen_name")),
                project_hint=PROJECT_TITLE,
                project_key=PROJECT_KEY,
                source=CallSource.TELPHIN.value,
                direction="outbound",
                client_number=client_number(item.get("to_username")),
                audio_url=str(recording["storage_url"]),
            ),
            None,
        )

    async def open_audio(self, ref: CallRef) -> AsyncIterator[bytes]:
        """Запись по ссылке хранилища — без токена, через прокси окружения.

        Ссылки нет — `RecordingNotAvailable`: она живёт до скачивания записи, и
        вернуть её может повторный приём суток. `404`/`410` — записи у
        хранилища нет. Остальное — поломка, а не отсутствие записи.
        """
        url = ref.audio_url
        if not url:
            raise RecordingNotAvailable(
                f"call {ref.session_id}: no storage link; run the ingest of its day again"
            )
        if self._download is None:
            self._download = httpx.AsyncClient(timeout=self._timeout_s)
        try:
            response = await self._download.send(
                self._download.build_request("GET", url), stream=True
            )
        except httpx.HTTPError as exc:
            # `from None`: цепочка исключения несла бы адрес записи в журнал.
            raise TelphinError(f"telphin storage: {type(exc).__name__}") from None
        if response.status_code in (404, 410):
            await response.aclose()
            raise RecordingNotAvailable(f"telphin storage has no recording of {ref.session_id}")
        if response.status_code != 200:
            await response.aclose()
            raise TelphinError(f"telphin storage: HTTP {response.status_code} for {ref.session_id}")
        return _chunks(response)


async def _chunks(response: httpx.Response) -> AsyncIterator[bytes]:
    """Выдать содержимое ответа источника порциями байтов."""
    try:
        async for chunk in response.aiter_bytes(1 << 16):
            yield chunk
    except httpx.HTTPError as exc:
        raise TelphinError(f"telphin storage stream: {type(exc).__name__}") from None
    finally:
        await response.aclose()


def build_telphin_source(settings: Settings) -> TelphinSource:
    """Источник для приёма: с клиентом API. Без ключей — отказ, а не пустые сутки."""
    missing = [
        name
        for name, value in (
            ("SPEECH_TELPHIN_APP_ID", settings.speech_telphin_app_id),
            ("SPEECH_TELPHIN_APP_SECRET", settings.speech_telphin_app_secret),
        )
        if not value.strip()
    ]
    if missing:
        raise RuntimeError("the telphin source requires " + ", ".join(missing))
    return TelphinSource(
        TelphinClient(
            base_url=settings.speech_telphin_base_url,
            app_id=settings.speech_telphin_app_id.strip(),
            app_secret=settings.speech_telphin_app_secret.strip(),
            timeout_s=settings.speech_telphin_timeout_seconds,
        ),
        timezone=settings.speech_display_timezone,
        min_talk_s=settings.speech_telphin_min_talk_seconds,
        excluded_extensions=_names(settings.speech_telphin_excluded_extensions),
        excluded_groups=_ids(settings.speech_telphin_excluded_groups),
        timeout_s=settings.speech_telphin_timeout_seconds,
    )


def build_telphin_audio(settings: Settings) -> TelphinSource:
    """Источник для воркера: только записи, без клиента API и без его ключей."""
    return TelphinSource(None, timeout_s=settings.speech_telphin_timeout_seconds)
