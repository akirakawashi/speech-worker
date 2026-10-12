"""REST-клиент NCC: поток записей, справочники сотрудников и проектов.

Список звонков поставляет выбранный источник; API кейсов для этого не подходит.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any, Final

import httpx
from pydantic import BaseModel, ConfigDict, Field

from src.infrastructure.sources.ncc.source import SESSION_ID_RE, RecordingNotAvailable
from src.setting.config import Settings

#: Руководство требует объявлять тип возвращаемых данных заголовком
#: `Content-Type` — даже у `GET`, где тела запроса нет. Это их правило, а не
#: наше прочтение HTTP; посылаем как написано и дублируем `Accept`, который
#: означает то же самое для всех остальных.
JSON_TYPE: Final[str] = "application/json"
AUDIO_TYPE: Final[str] = "audio/mpeg"

#: Размер куска при скачивании. Запись на час — десятки мегабайт, и читать её
#: в память ради того, чтобы тут же выложить в хранилище, незачем.
CHUNK_SIZE: Final[int] = 1 << 20

#: Типы входящих и исходящих проектов (`dataChannel`). `autoinformer` есть в
#: обоих списках руководства, поэтому по нему направление не определяется —
#: и выдумывать его нельзя: направление попадает в карточку звонка.
INBOUND_CHANNELS: Final[frozenset[str]] = frozenset({"queue", "im", "sms", "email", "socialnet"})
OUTBOUND_CHANNELS: Final[frozenset[str]] = frozenset({"call", "mailing"})

#: Какая доля справочника обязана оказаться пригодной, иначе это не справочник.
#:
#: Пропустить строку без логина правильно: одна испорченная запись не должна
#: ронять справочник целиком. Но если логина нет почти ни у кого — это не
#: испорченные строки, а другое устройство ответа, и «пропустить молча» тогда
#: означает записать справочник из одного человека и отчитаться об успехе.
#:
#: Измерено на живом API 2026-09-11: из 241 сотрудника логин есть ровно у
#: одного — у той учётной записи, которой сделан запрос. Остальным NCC отдаёт
#: короткую проекцию (`uuid`, `title`, `roles`), а полную — только себе.
#: Связывать звонок с оператором при таком ответе нечем, и узнать об этом надо
#: на месте, а не по пустому фильтру в интерфейсе через неделю.
MIN_USABLE_SHARE: Final[float] = 0.5


class NccNotConfigured(RuntimeError):
    """Адрес, логин или токен не заданы — ошибка развёртывания, а не отказ этапа.

    Отдельный тип, потому что повторять её бессмысленно: класс отказа задания
    она получает другой.
    """


class NccError(Exception):
    """NCC не ответил или ответил не тем.

    Тело ответа наружу не выносится: у `download` там запись разговора, у
    остальных запросов — справочник с именами сотрудников.
    """

    def __init__(self, reason: str, *, retryable: bool, detail: str | None = None) -> None:
        """Настроить NccError из переданных параметров и зависимостей."""
        super().__init__(reason if detail is None else f"{reason}: {detail}")
        self.reason = reason
        self.retryable = retryable
        #: Подробности для человека. Отдельно от `reason`, потому что `reason`
        #: попадает в класс ошибки задания и обязан быть из счётного множества:
        #: числа и имена в нём превратили бы метрику отказов в россыпь.
        self.detail = detail


class NccEmployee(BaseModel):
    """Сотрудник, каким его отдаёт справочник.

    Логин обязателен, остальное нет: связывать звонок с оператором мы будем по
    логину, а имя — то, что показываем человеку. Уволенные приходят по
    отдельному запросу и помечены: их записи в звонках остаются, и подписывать
    их «неизвестный оператор» значило бы терять историю.

    На живом API логин приходит **не всегда** — см. `MIN_USABLE_SHARE`. Строка
    без логина здесь не появляется: она отсеивается раньше, а массовый отсев
    объявляется отказом, а не тишиной.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    login: str
    uuid: str | None = None
    display_name: str | None = None
    email: str | None = None
    department: str | None = None
    removed: bool = False


class RecordingHead(BaseModel):
    """Ответ `HEAD` по записи: есть ли она и когда дописана.

    `exists=False` — записи нет (`404`), обычная дырка в сквозной нумерации.
    `exists=True, modified=None` — запись есть, но `Last-Modified` не пришёл или
    не разобрался (на практике сервер его всегда шлёт; ветка на случай, когда
    перестанет). Дату отсутствия и отсутствие даты нельзя путать: первое
    двигает обход дальше, второе — нет.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    exists: bool
    modified: datetime | None = None


class NccProject(BaseModel):
    """Проект (очередь) NCC."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    uuid: str
    title: str
    state: str | None = None
    data_channel: str | None = None

    @property
    def direction(self) -> str | None:
        """`inbound`, `outbound` или `None`, если по типу канала не определить.

        `None`, а не умолчание: `autoinformer` числится и входящим, и
        исходящим, и выбрать за NCC мы не можем. Направление попадает в
        карточку звонка, и угаданное там неотличимо от известного.
        """
        channel = (self.data_channel or "").strip().lower()
        if channel in INBOUND_CHANNELS:
            return "inbound"
        if channel in OUTBOUND_CHANNELS:
            return "outbound"
        return None

    @property
    def is_active(self) -> bool:
        """Вернуть признак доступности записи справочника источника."""
        return (self.state or "").strip().upper() == "ACTIVE"


def _classify(status: int, *, where: str) -> NccError:
    """Отказ по коду ответа.

    Разделение проходит не по «ошибка или нет», а по «поможет ли повтор»:

    * `401`/`403` — не тот токен или не выданы права. Повтор даст то же самое;
      чинить надо учётную запись, и делать это будет человек;
    * `404` — объекта нет. Для записи это отдельный случай (см. `download`), для
      справочников — обычное «не найдено»;
    * `5xx` — сторона NCC. Повторять стоит;
    * прочее — неожиданно, и молчать об этом нельзя.
    """
    if status in (401, 403):
        return NccError(f"{where}_http_{status}", retryable=False)
    if status == 404:
        return NccError(f"{where}_not_found", retryable=False)
    if status >= 500:
        return NccError(f"{where}_http_{status}", retryable=True)
    return NccError(f"{where}_http_{status}", retryable=False)


class NccRestClient(BaseModel):
    """Три операции над REST API NCC.

    Авторизация ключом API, а не BASIC: у ключа своя жизнь — его выпускают и
    отзывают на карточке пользователя, — а BASIC потребовал бы держать в
    окружении **пароль** учётной записи, тот самый, которым в NCC входят руками.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    base_url: str
    username: str
    api_token: str = Field(repr=False)
    timeout_seconds: float = 60.0

    def _headers(self, content_type: str) -> dict[str, str]:
        """Собрать заголовки технического запроса к внешнему сервису."""
        return {
            "Username": self.username,
            "X-API-Key": self.api_token,
            "Content-Type": content_type,
            "Accept": content_type,
        }

    # ----------------------------------------------------------------- #
    # Справочники
    # ----------------------------------------------------------------- #

    async def employees(self, *, removed: bool = False) -> Sequence[NccEmployee]:
        """Справочник сотрудников.

        `removed=True` отдаёт **только** уволенных — так устроен параметр в
        руководстве, а не «включить уволенных в общий список». Чтобы получить
        всех, запросов нужно два, и это стоит знать заранее.
        """
        body = await self._get_json("/employees", {"removed": "true" if removed else "false"})
        items = body.get("items") if isinstance(body, dict) else None
        if not isinstance(items, list):
            raise NccError("employees_malformed", retryable=True)
        staff = tuple(
            employee for employee in (_employee(item) for item in items) if employee is not None
        )
        if items and len(staff) < len(items) * MIN_USABLE_SHARE:
            raise NccError(
                "employees_have_no_logins",
                retryable=False,
                detail=(
                    f"логин есть у {len(staff)} из {len(items)}. Связывать звонок с "
                    "оператором нечем: суточный пакет называет оператора логином. "
                    "Похоже на нехватку прав у учётной записи API — короткая проекция "
                    "сотрудника (uuid, title, roles) выдаётся вместо полной."
                ),
            )
        return staff

    async def project(self, uuid: str) -> NccProject:
        """Проект по UUID."""
        identifier = uuid.strip()
        if not identifier:
            raise NccError("project_uuid_is_empty", retryable=False)
        body = await self._get_json(f"/projects/{identifier}", None)
        if not isinstance(body, dict):
            raise NccError("project_malformed", retryable=True)
        title = body.get("title")
        if not isinstance(title, str) or not title.strip():
            # Проект без названия показывать в фильтре нечем, а подставить
            # UUID значило бы выдать служебный ключ за имя очереди.
            raise NccError("project_has_no_title", retryable=False)
        return NccProject(
            uuid=identifier,
            title=title.strip(),
            state=_text(body.get("state")),
            data_channel=_text(body.get("dataChannel")),
        )

    async def _get_json(self, path: str, params: dict[str, str] | None) -> Any:
        """Выполнить GET к NCC и вернуть JSON либо нормализованный отказ."""
        where = path.strip("/").split("/", 1)[0] or "ncc"
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.get(
                    f"{self.base_url}{path}",
                    params=params,
                    headers=self._headers(JSON_TYPE),
                )
        except httpx.TimeoutException as exc:
            raise NccError(f"{where}_timeout", retryable=True) from exc
        except httpx.HTTPError as exc:
            raise NccError(f"{where}_unreachable", retryable=True) from exc

        if response.status_code == 204:
            # «Нет данных» — законный ответ постраничного списка, а не отказ
            # (руководство, 6.2). Тела у него нет, разбирать нечего; решение,
            # что это значит, принимает вызывающий.
            return None
        if response.status_code != 200:
            raise _classify(response.status_code, where=where)
        try:
            return response.json()
        except (ValueError, json.JSONDecodeError) as exc:
            raise NccError(f"{where}_malformed", retryable=True) from exc

    async def head_recording(self, session_id: str) -> RecordingHead:
        """Существование записи и её дата — без скачивания тела.

        `HEAD`, а не `GET`: нужны только факт существования и `Last-Modified`
        (момент, когда запись была дописана, то есть время звонка с точностью до
        конца разговора). Тело — десятки мегабайт — при этом не качается, и
        перечисление тысячи звонков стоит тысячи дешёвых запросов, а не тысячи
        загрузок.

        Это и есть перечисление, которого нет отдельным эндпойнтом: справочника
        сессий API не отдаёт, но существование конкретной сессии подтверждает.
        Идентификаторы идут сквозным счётчиком (`…_<epoch>_<seq>`), и обход
        счётчика с `HEAD` восстанавливает список звонков за период.

        Возвращается `RecordingHead`, а не `datetime | None`: «записи нет» и
        «запись есть, но без разбираемой даты» — разные ответы, и свести их к
        одному `None` значило бы либо терять существующий звонок, либо считать
        несуществующий за настоящий. Отказ сети — исключение: не отличив «записи
        нет» от «не смог спросить», обход молча оборвётся на первом сбое и решит,
        что звонки кончились.
        """
        identifier = session_id.strip()
        if not SESSION_ID_RE.match(identifier):
            raise NccError("head_bad_session_id", retryable=False)
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.head(
                    f"{self.base_url}/callrecord/download",
                    params={"sessionId": identifier},
                    headers=self._headers(AUDIO_TYPE),
                )
        except httpx.TimeoutException as exc:
            raise NccError("head_timeout", retryable=True) from exc
        except httpx.HTTPError as exc:
            raise NccError("head_unreachable", retryable=True) from exc

        if response.status_code == 404:
            return RecordingHead(exists=False)
        if response.status_code != 200:
            raise _classify(response.status_code, where="head")
        stamp = response.headers.get("last-modified")
        if not stamp:
            return RecordingHead(exists=True)
        try:
            return RecordingHead(exists=True, modified=parsedate_to_datetime(stamp))
        except (TypeError, ValueError):
            return RecordingHead(exists=True)

    # ----------------------------------------------------------------- #
    # Запись разговора
    # ----------------------------------------------------------------- #

    async def download(self, session_id: str) -> AsyncIterator[bytes]:
        """Запись разговора потоком.

        `404` превращается в `RecordingNotAvailable` — ту самую ошибку, которую
        объявляет порт источника. Этап приёма обязан отличать «записи нет» от
        «источник сломан»: первое после нескольких попыток означает, что записи
        не будет вовсе (ING-04), второе — что надо повторять. Если бы `404`
        приезжал сюда обычным `NccError`, этап считал бы отсутствием записи
        любую поломку.

        Идентификатор проверяется до запроса. Мусор всё равно вернул бы `404`,
        но «записи нет» и «мы спросили ерунду» — разные события, и второе не
        должно выглядеть как первое.
        """
        identifier = session_id.strip()
        if not SESSION_ID_RE.match(identifier):
            raise NccError("download_bad_session_id", retryable=False)

        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            try:
                request = client.build_request(
                    "GET",
                    f"{self.base_url}/callrecord/download",
                    params={"sessionId": identifier},
                    headers=self._headers(AUDIO_TYPE),
                )
                response = await client.send(request, stream=True)
            except httpx.TimeoutException as exc:
                raise NccError("download_timeout", retryable=True) from exc
            except httpx.HTTPError as exc:
                raise NccError("download_unreachable", retryable=True) from exc

            try:
                if response.status_code == 404:
                    raise RecordingNotAvailable(f"NCC has no recording for {identifier}")
                if response.status_code != 200:
                    raise _classify(response.status_code, where="download")
                async for chunk in response.aiter_bytes(CHUNK_SIZE):
                    yield chunk
            finally:
                await response.aclose()


def _text(value: Any) -> str | None:
    """Прочитать текстовое значение и нормализовать отсутствие."""
    return value.strip() or None if isinstance(value, str) else None


def _employee(item: Any) -> NccEmployee | None:
    """Сотрудник из строки справочника, или `None`, если строка бесполезна.

    Без логина связать сотрудника со звонком нечем, и запись без него — не
    «частично заполненный сотрудник», а строка, которой в справочнике не место.
    Молча пропускается: справочник из тысячи строк не должен падать целиком
    из-за одной.
    """
    if not isinstance(item, dict):
        return None
    login = _text(item.get("login"))
    if not login:
        return None
    parts = [
        _text(item.get("lastName")),
        _text(item.get("firstName")),
        _text(item.get("middleName")),
    ]
    display = " ".join(part for part in parts if part)
    return NccEmployee(
        login=login,
        uuid=_text(item.get("uuid")),
        display_name=display or None,
        email=_text(item.get("email")),
        department=_text(item.get("department")),
        removed=bool(item.get("removed")),
    )


def build_ncc_rest_client(settings: Settings) -> NccRestClient:
    """Собрать клиент по настройкам `SPEECH_NCC_*`.

    Отказ при любом пустом из трёх, а не молчаливое отключение: справочник, тихо
    решивший, что операторов нет, оставит фильтр по оператору пустым — и это
    неотличимо от «за период никто не работал».
    """
    base_url = str(getattr(settings, "speech_ncc_base_url", "") or "").strip().rstrip("/")
    username = str(getattr(settings, "speech_ncc_username", "") or "").strip()
    token = str(getattr(settings, "speech_ncc_api_token", "") or "").strip()
    missing = [
        name
        for name, value in (
            ("SPEECH_NCC_BASE_URL", base_url),
            ("SPEECH_NCC_USERNAME", username),
            ("SPEECH_NCC_API_TOKEN", token),
        )
        if not value
    ]
    if missing:
        raise NccNotConfigured(
            "NCC REST needs " + ", ".join(missing) + "; without them the directories stay "
            "empty and an empty directory is indistinguishable from an empty period"
        )
    timeout = float(getattr(settings, "speech_ncc_timeout_seconds", 60.0) or 60.0)
    return NccRestClient(
        base_url=base_url,
        username=username,
        api_token=token,
        timeout_seconds=timeout,
    )
