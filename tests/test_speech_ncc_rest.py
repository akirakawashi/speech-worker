"""Клиент REST API NCC.

Транспорт подставной, а форма запроса и разбор ответа — настоящие. Проверяется
то, что видит другая сторона: заголовки авторизации, путь, параметры — и то,
как отказы превращаются в решение «повторять или нет». Ошибка здесь выглядит
снаружи как «NCC сломался», а не как наша опечатка, поэтому проверяется до
первого боевого вызова, а не после.

Живого доступа к NCC у разработки нет: площадка отвечает `401` без учётных
данных, они выданы только контуру. Всё, что можно проверить без неё, проверено
здесь.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from src.infrastructure.sources.ncc.rest import (
    NccError,
    NccNotConfigured,
    NccProject,
    NccRestClient,
    build_ncc_rest_client,
)
from src.infrastructure.sources.ncc.source import RecordingNotAvailable

SESSION_ID = "node_1_domain_2_nauss_0_1766092829_1138986"


class _Settings:
    def __init__(self, url: str = "https://ncc/api/v2", user: str = "u", token: str = "t") -> None:
        self.speech_ncc_base_url = url
        self.speech_ncc_username = user
        self.speech_ncc_api_token = token
        self.speech_ncc_timeout_seconds = 30.0


#: Настоящий конструктор, снятый **один раз при импорте модуля**.
#:
#: Снимать его внутри помощника нельзя: второй вызов помощника в том же тесте
#: снял бы уже подменённый конструктор и получил бесконечную рекурсию. Мина
#: тихая — сработала бы на первом тесте, которому нужны два клиента.
_PRISTINE_INIT = httpx.AsyncClient.__init__


def _client(handler: Any) -> NccRestClient:
    """Клиент, у которого весь транспорт подменён на переданный обработчик."""
    transport = httpx.MockTransport(handler)

    def _init(self: httpx.AsyncClient, **kwargs: Any) -> None:
        kwargs["transport"] = transport
        _PRISTINE_INIT(self, **kwargs)

    httpx.AsyncClient.__init__ = _init  # type: ignore[method-assign]
    return NccRestClient(base_url="https://ncc/api/v2", username="wsrest", api_token="secret")


@pytest.fixture(autouse=True)
def _restore_httpx() -> Any:
    """Подмена не должна пережить тест: следующий пойдёт в сеть по-настоящему."""
    yield
    httpx.AsyncClient.__init__ = _PRISTINE_INIT  # type: ignore[method-assign]


# --------------------------------------------------------------------------- #
# Авторизация и форма запроса
# --------------------------------------------------------------------------- #


async def test_the_request_carries_the_api_key_headers() -> None:
    """Ключ API, а не BASIC.

    У ключа своя жизнь: его выпускают и отзывают на карточке пользователя.
    BASIC потребовал бы держать в окружении **пароль** учётной записи — тот
    самый, которым в NCC входят руками.
    """
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["headers"] = request.headers
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"items": []})

    await _client(handler).employees()

    assert seen["headers"]["Username"] == "wsrest"
    assert seen["headers"]["X-API-Key"] == "secret"
    # Пароля в запросе нет ни в каком виде.
    assert "Authorization" not in seen["headers"]
    assert seen["url"] == "https://ncc/api/v2/employees?removed=false"


async def test_the_audio_request_declares_the_audio_type() -> None:
    """Руководство требует объявлять тип возвращаемых данных заголовком.

    Даже у `GET`, где тела запроса нет: это их правило, а не наше прочтение
    HTTP. Посылаем как написано и дублируем `Accept`.
    """
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["headers"] = request.headers
        seen["url"] = str(request.url)
        return httpx.Response(200, content=b"\x00\x01")

    chunks = [chunk async for chunk in _client(handler).download(SESSION_ID)]

    assert seen["headers"]["Content-Type"] == "audio/mpeg"
    assert seen["headers"]["Accept"] == "audio/mpeg"
    assert f"sessionId={SESSION_ID}" in seen["url"]
    assert b"".join(chunks) == b"\x00\x01"


# --------------------------------------------------------------------------- #
# Запись разговора
# --------------------------------------------------------------------------- #


async def test_a_missing_recording_speaks_the_language_of_the_port() -> None:
    """`404` превращается в `RecordingNotAvailable`, а не в общий отказ.

    Этап приёма обязан отличать «записи нет» от «источник сломан»: первое после
    нескольких попыток означает, что записи не будет вовсе (ING-04), второе —
    что надо повторять. Приезжай `404` обычным `NccError`, этап считал бы
    отсутствием записи любую поломку.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "not found"})

    with pytest.raises(RecordingNotAvailable):
        async for _ in _client(handler).download(SESSION_ID):
            pass


async def test_a_malformed_session_id_is_refused_before_the_request() -> None:
    """Мусор вернул бы тот же `404`, но это разные события.

    «Записи нет» и «мы спросили ерунду» не должны выглядеть одинаково: первое
    закрывает звонок навсегда, второе — наша ошибка.
    """
    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(200, content=b"")

    with pytest.raises(NccError) as failure:
        async for _ in _client(handler).download("../../etc/passwd"):
            pass

    assert failure.value.reason == "download_bad_session_id"
    assert failure.value.retryable is False
    assert not called, "запрос ушёл, хотя идентификатор заведомо не тот"


async def test_a_large_recording_arrives_in_pieces() -> None:
    """Потоком, а не целиком: запись на час — десятки мегабайт."""
    payload = b"x" * (3 << 20)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=payload)

    chunks = [chunk async for chunk in _client(handler).download(SESSION_ID)]

    assert b"".join(chunks) == payload
    assert len(chunks) > 1, "запись приехала одним куском — читалась в память целиком"


# --------------------------------------------------------------------------- #
# Классы отказов
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("status", "retryable"),
    [(401, False), (403, False), (400, False), (500, True), (502, True), (503, True)],
)
async def test_failures_are_split_by_whether_a_retry_would_help(
    status: int, retryable: bool
) -> None:
    """Разделение не по «ошибка или нет», а по «поможет ли повтор».

    `401` и `403` — не тот токен или не выданы права: чинит человек, а не
    очередь. `5xx` — сторона NCC, и повторять стоит.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={})

    with pytest.raises(NccError) as failure:
        await _client(handler).employees()

    assert failure.value.retryable is retryable
    assert str(status) in failure.value.reason


async def test_an_unreachable_host_is_worth_repeating() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route", request=request)

    with pytest.raises(NccError) as failure:
        await _client(handler).project("corebo00000000000n63prtlf4p883fo")

    assert failure.value.retryable is True


async def test_the_failure_carries_no_response_body() -> None:
    """У `download` в теле запись разговора, у справочника — имена сотрудников."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"detail": "Иванов Иван Иванович"})

    with pytest.raises(NccError) as failure:
        await _client(handler).employees()

    assert "Иванов" not in str(failure.value)


# --------------------------------------------------------------------------- #
# Справочник сотрудников
# --------------------------------------------------------------------------- #


async def test_an_employee_is_read_with_a_human_name() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "uuid": "u1",
                        "login": "ivanov",
                        "lastName": "Иванов",
                        "firstName": "Иван",
                        "middleName": "Иванович",
                        "email": "ivanov@example.com",
                        "department": "Поддержка",
                        "removed": False,
                    }
                ]
            },
        )

    employees = await _client(handler).employees()

    assert len(employees) == 1
    assert employees[0].login == "ivanov"
    assert employees[0].display_name == "Иванов Иван Иванович"
    assert employees[0].removed is False


async def test_a_row_without_a_login_is_skipped_not_fatal() -> None:
    """Связать сотрудника со звонком без логина нечем.

    Справочник из тысячи строк не должен падать целиком из-за одной, но и
    заводить сотрудника без ключа связи незачем.

    Пригодных здесь большинство — это и есть случай «несколько битых строк».
    Когда их большинство, ответ устроен иначе, и это отдельный тест ниже.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "items": [
                    {"uuid": "u1", "firstName": "Без"},
                    "мусор",
                    {"login": "  "},
                    {"login": "petrov"},
                    {"login": "sidorov"},
                    {"login": "kuznetsov"},
                    {"login": "orlov"},
                ]
            },
        )

    employees = await _client(handler).employees()

    assert [e.login for e in employees] == ["petrov", "sidorov", "kuznetsov", "orlov"]


async def test_a_directory_where_almost_nobody_has_a_login_is_refused() -> None:
    """Так отвечает настоящий NCC, и молчать об этом нельзя.

    Измерено 2026-09-11: из 241 сотрудника логин есть у одного — у той учётной
    записи, которой сделан запрос; остальным отдаётся короткая проекция
    (`uuid`, `title`, `roles`). Прежний разбор пропускал такие строки молча и
    возвращал справочник из одного человека — с виду успешная синхронизация,
    после которой фильтр по оператору пуст, а причины не видно нигде.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "items": [{"login": "VolnaAPI", "lastName": "Волна"}]
                + [
                    {"uuid": f"corebo{index}", "title": "Абрамов Николай", "roles": ["Operator"]}
                    for index in range(240)
                ]
            },
        )

    with pytest.raises(NccError) as refused:
        await _client(handler).employees()

    assert refused.value.reason == "employees_have_no_logins"
    # Повтор даст тот же ответ: чинить надо права учётной записи, и это человек.
    assert refused.value.retryable is False
    # Обе величины названы: «1 из 241» — это диагноз, «мало» — нет.
    assert "1 из 241" in (refused.value.detail or "")


async def test_an_empty_directory_is_not_a_refusal() -> None:
    """Пустой ответ — не то же самое, что ответ не той формы.

    Делить на ноль нечего, и объявлять отказ не за что: пустой справочник
    бывает на пустом стенде.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"items": []})

    assert await _client(handler).employees() == ()


async def test_dismissed_employees_come_from_a_separate_request() -> None:
    """`removed=true` отдаёт **только** уволенных, а не «включая уволенных».

    Так устроен параметр в руководстве. Чтобы получить всех, запросов нужно
    два, и знать это надо заранее — иначе половина справочника молча пропадёт.
    """
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, json={"items": []})

    client = _client(handler)
    await client.employees()
    await client.employees(removed=True)

    assert "removed=false" in seen[0]
    assert "removed=true" in seen[1]


async def test_a_directory_that_is_not_a_list_is_a_broken_answer() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"items": "нет"})

    with pytest.raises(NccError) as failure:
        await _client(handler).employees()

    assert failure.value.retryable is True


# --------------------------------------------------------------------------- #
# Проект
# --------------------------------------------------------------------------- #


async def test_a_project_is_read_with_its_channel() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/projects/corebo1")
        return httpx.Response(
            200,
            json={
                "title": "Поддержка В2В",
                "partner": "p1",
                "state": "ACTIVE",
                "dataChannel": "queue",
            },
        )

    project = await _client(handler).project("corebo1")

    assert project.title == "Поддержка В2В"
    assert project.is_active is True
    assert project.direction == "inbound"


@pytest.mark.parametrize(
    ("channel", "direction"),
    [
        ("queue", "inbound"),
        ("email", "inbound"),
        ("socialnet", "inbound"),
        ("call", "outbound"),
        ("mailing", "outbound"),
        # `autoinformer` числится и входящим, и исходящим — выбрать за NCC
        # нельзя, а направление попадает в карточку звонка.
        ("autoinformer", None),
        ("", None),
        (None, None),
    ],
)
def test_the_direction_is_never_guessed(channel: str | None, direction: str | None) -> None:
    assert NccProject(uuid="p", title="t", data_channel=channel).direction == direction


async def test_a_project_without_a_title_is_refused() -> None:
    """Подставить UUID значило бы выдать служебный ключ за имя очереди."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"title": "  ", "state": "ACTIVE"})

    with pytest.raises(NccError) as failure:
        await _client(handler).project("corebo1")

    assert failure.value.reason == "project_has_no_title"


async def test_an_empty_project_uuid_never_reaches_the_wire() -> None:
    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(200, json={})

    with pytest.raises(NccError):
        await _client(handler).project("   ")

    assert not called


# --------------------------------------------------------------------------- #
# Сборка
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "missing", ["speech_ncc_base_url", "speech_ncc_username", "speech_ncc_api_token"]
)
def test_an_unconfigured_client_refuses_loudly(missing: str) -> None:
    """Справочник, тихо решивший, что операторов нет, оставит фильтр пустым.

    А пустой фильтр неотличим от «за период никто не работал».
    """
    settings = _Settings()
    setattr(settings, missing, "")

    with pytest.raises(NccNotConfigured) as failure:
        build_ncc_rest_client(settings)

    assert missing.upper() in str(failure.value)


def test_a_trailing_slash_does_not_double_up() -> None:
    client = build_ncc_rest_client(_Settings(url="https://ncc/api/v2/"))

    assert client.base_url == "https://ncc/api/v2"


def test_the_token_stays_out_of_the_repr() -> None:
    """Клиент попадает в трассировки и в отладочный вывод."""
    client = build_ncc_rest_client(_Settings(token="p6yH91D.bwMBQGiq8GWXUXc5TkM0eCemiUmjEJxf"))

    assert "p6yH91D" not in repr(client)
