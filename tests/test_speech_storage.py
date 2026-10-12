"""Хранилище аудиозаписей: подпись, ключи, коды ответов.

Настоящий MinIO здесь не нужен: проверяется, что клиент отправляет — метод,
адрес, подписанные заголовки — и как он трактует ответы. Поведение самого
MinIO проверяется отдельно, на стенде.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest

from src.infrastructure.storage.recordings import (
    MAX_OBJECT_BYTES,
    SpeechAudioStorage,
    StorageConfig,
    StorageError,
    StorageNotConfiguredError,
    object_key,
)

CONFIG = StorageConfig(
    base_url="http://speech-minio:9000",
    bucket="speech-recordings",
    region="us-east-1",
    access_key_id="key",
    secret_key="secret",
)


async def _chunks(*parts: bytes) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


#: Поля `CONFIG` россыпью: тесты ниже подменяют один адрес и оставляют
#: остальное как есть.
_CONFIG_FIELDS = {
    "base_url": CONFIG.base_url,
    "bucket": CONFIG.bucket,
    "region": CONFIG.region,
    "access_key_id": CONFIG.access_key_id,
    "secret_key": CONFIG.secret_key,
}


def _storage(handler) -> SpeechAudioStorage:
    transport = httpx.MockTransport(handler)
    return SpeechAudioStorage(CONFIG, client=httpx.AsyncClient(transport=transport))


def test_object_key_mirrors_the_layout_ncc_itself_uses() -> None:
    """`calls/ГГГГ/ММ/ДД/<sessionId>.mp3` — та же раскладка, что у NCC."""
    key = object_key("node_1_domain_2_nauss_0_1766092829_1138986", date_path="2026/01/14")
    assert key == "calls/2026/01/14/node_1_domain_2_nauss_0_1766092829_1138986.mp3"


def test_object_key_falls_back_to_a_given_moment() -> None:
    key = object_key("node_1_x", datetime(2026, 2, 3, 12, 0, tzinfo=UTC))
    assert key == "calls/2026/02/03/node_1_x.mp3"


def test_storage_without_configuration_refuses_to_be_built() -> None:
    """Ненастроенное хранилище — незаконченная настройка, а не сбой в рантайме."""
    with pytest.raises(StorageNotConfiguredError):
        SpeechAudioStorage(
            StorageConfig(
                base_url="", bucket="", region="us-east-1", access_key_id="k", secret_key="s"
            )
        )
    with pytest.raises(StorageNotConfiguredError):
        SpeechAudioStorage(
            StorageConfig(
                base_url="http://x:9000",
                bucket="b",
                region="us-east-1",
                access_key_id="",
                secret_key="",
            )
        )


async def test_put_signs_the_request_and_reports_the_written_size() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("Authorization", "")
        seen["sha"] = request.headers.get("x-amz-content-sha256", "")
        seen["type"] = request.headers.get("Content-Type", "")
        seen["body"] = request.content
        return httpx.Response(200)

    written = await _storage(handler).put("calls/2026/01/14/x.mp3", _chunks(b"ID3", b"audio"))

    assert written == 8
    assert seen["method"] == "PUT"
    # Путь-стиль: без подстановочного DNS `bucket.host` не резолвится.
    assert seen["url"] == "http://speech-minio:9000/speech-recordings/calls/2026/01/14/x.mp3"
    assert str(seen["auth"]).startswith("AWS4-HMAC-SHA256 Credential=key/")
    assert seen["type"] == "audio/mpeg"
    assert seen["body"] == b"ID3audio"
    # Хеш тела подписан — целостность проверяется, а не объявляется.
    assert seen["sha"] != "UNSIGNED-PAYLOAD"
    assert len(str(seen["sha"])) == 64


async def test_an_absurdly_large_object_is_refused_before_it_is_sent() -> None:
    """Замер: средняя запись 730 КБ, часовой разговор — около 11 МБ.

    Объект в сотни мегабайт означает, что источник отдал не запись. Потолок
    существует, чтобы это не съело память воркера целиком.
    """

    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - не должен вызваться
        raise AssertionError("запрос не должен был уйти")

    oversized = _chunks(b"x" * (MAX_OBJECT_BYTES + 1))
    with pytest.raises(StorageError, match="exceeds"):
        await _storage(handler).put("calls/x.mp3", oversized)


async def test_a_refused_write_does_not_leak_the_storage_reply() -> None:
    """Ответ хранилища может содержать имя бакета, ключи и внутренние адреса."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, text="<Error><Key>secret-bucket/creds</Key></Error>")

    with pytest.raises(StorageError) as caught:
        await _storage(handler).put("calls/x.mp3", _chunks(b"a"))

    assert "403" in str(caught.value)
    assert "secret-bucket" not in str(caught.value)


async def test_range_requests_are_passed_through_untouched() -> None:
    """Диапазон просит плеер, когда пользователь перематывает."""
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["range"] = request.headers.get("Range", "")
        return httpx.Response(206, content=b"partial")

    storage = _storage(handler)
    async with storage.open("calls/x.mp3", byte_range="bytes=100-199") as response:
        assert response.status_code == 206

    assert seen["range"] == "bytes=100-199"


async def test_a_missing_object_is_an_error_not_an_empty_stream() -> None:
    """Пустой поток вместо записи выглядит как тихий разговор."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    with pytest.raises(StorageError, match="was not found") as caught:
        async with _storage(handler).open("calls/gone.mp3"):
            pass

    # Причина классифицирована, а не только описана словами: по ней ручка
    # отличает «записи нет» от «хранилище отказало», и текст сообщения для
    # этого не годится — он меняется.
    assert caught.value.reason == "not_found"
    assert caught.value.status_code == 404


async def test_deleting_something_that_is_already_gone_is_success() -> None:
    """Удаление по сроку хранения обязано быть повторяемым.

    Иначе один прерванный прогон навсегда оставляет задание, которое падает на
    объекте, удалённом предыдущей попыткой.

    Ответ здесь **204**, а не 404, и это не деталь оформления теста: проверено
    на живом MinIO — удаление отсутствующего ключа даёт 204. Прежняя редакция
    этого теста подавала 404 и объявляла результат идемпотентностью, чем и
    закрепляла дефект, описанный в тесте ниже.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    assert await _storage(handler).delete("calls/gone.mp3") is True


async def test_delete_does_not_mistake_a_missing_bucket_for_a_deleted_object() -> None:
    """404 на удалении — это `NoSuchBucket`, и успехом он быть не может.

    Проверено на живом MinIO: отсутствующий объект даёт 204, а 404 приходит
    только на несуществующую корзину. Пока код считал 404 успехом, опечатка в
    `SPEECH_STORAGE_BUCKET` превращала удаление по сроку хранения в отчёт об
    удалении разговоров реальных клиентов: ни одной ошибки в журнале, ни одной
    удалённой записи, и обнаружилось бы это только при проверке ИБ.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    with pytest.raises(StorageError, match="bucket"):
        await _storage(handler).delete("calls/gone.mp3")


async def test_delete_reports_a_refusal() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403)

    with pytest.raises(StorageError, match="403"):
        await _storage(handler).delete("calls/x.mp3")


async def test_exists_distinguishes_present_from_absent() -> None:
    def present(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200)

    def absent(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    assert await _storage(present).exists("calls/x.mp3") is True
    assert await _storage(absent).exists("calls/x.mp3") is False


async def test_exists_does_not_report_a_refused_probe_as_absence() -> None:
    """«Нет» здесь принимают за факт, поэтому наугад его давать нельзя.

    Просроченный ключ или отозванный доступ дают 403. Пока это значило
    «объекта нет», скачивание начиналось заново, а удаление по сроку хранения
    считало запись уже уничтоженной — и записывало это в отчёт.
    """

    def refused(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403)

    with pytest.raises(StorageError, match="403"):
        await _storage(refused).exists("calls/x.mp3")


async def test_an_unreachable_storage_says_so_without_the_transport_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused to 10.0.0.5:9000")

    with pytest.raises(StorageError) as caught:
        await _storage(handler).put("calls/x.mp3", _chunks(b"a"))

    assert "unreachable" in str(caught.value)
    assert "10.0.0.5" not in str(caught.value)


# --------------------------------------------------------------------------- #
# Ключ объекта собирается из данных источника
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "session_id",
    ["..", "../..", "a/../b", "a/b", "", "x\\y", "."],
)
def test_a_session_identifier_cannot_move_the_object_out_of_its_prefix(session_id: str) -> None:
    """Идентификатор приходит от источника, а в ключ попадает дословно.

    `canonical_uri` оставляет `/` и точки как есть — в ключе S3 они законны, —
    поэтому `..` доезжает до хранилища `..`. Объект ложится не туда, куда мы
    считаем, и находится потом не там, где ищем: удаление по сроку хранения
    работает по префиксу.
    """
    with pytest.raises(StorageError) as refused:
        object_key(session_id, date_path="2026/01/14")

    assert refused.value.reason == "bad_object_key"


@pytest.mark.parametrize("date_path", ["..", "2026/../..", "2026//14", "2026/./14"])
def test_a_date_path_cannot_move_the_object_either(date_path: str) -> None:
    """Путь даты — такие же чужие данные, как идентификатор."""
    with pytest.raises(StorageError) as refused:
        object_key("node_1_domain_2_nauss_0_1766092829_1138986", date_path=date_path)

    assert refused.value.reason == "bad_object_key"


def test_an_ordinary_key_is_still_built() -> None:
    """Запрет не должен отсекать законный ключ — иначе он отсекает приём целиком."""
    assert (
        object_key("node_1_domain_2_nauss_0_1766092829_1138986", date_path="2026/01/14")
        == "calls/2026/01/14/node_1_domain_2_nauss_0_1766092829_1138986.mp3"
    )


# --------------------------------------------------------------------------- #
# Подписывается то, что будет отправлено
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "base_url,expected",
    [
        ("http://speech-minio:9000", "speech-minio:9000"),
        # Порт, стандартный для схемы, httpx в заголовок не ставит.
        ("http://storage.example:80", "storage.example"),
        ("https://storage.example:443", "storage.example"),
        # Регистр httpx приводит к нижнему.
        ("HTTP://MinIO:9000", "minio:9000"),
        ("https://Storage.Example", "storage.example"),
        # Префикс пути к хосту не относится.
        ("http://storage.example:8080/s3", "storage.example:8080"),
    ],
)
def test_the_signed_host_is_written_the_way_it_will_be_sent(base_url: str, expected: str) -> None:
    """Подпись SigV4 накрывает заголовок `Host`.

    httpx перед отправкой приводит хост к нижнему регистру и убирает
    стандартный порт. Подписав `MinIO:80`, мы отправим `minio` и получим
    `403 SignatureDoesNotMatch` — ответ, по которому ищут опечатку в ключах, а
    не в написании адреса.
    """
    assert StorageConfig(**{**_CONFIG_FIELDS, "base_url": base_url}).host == expected


async def test_the_host_we_sign_is_the_host_httpx_puts_on_the_wire() -> None:
    """То же утверждение, но проверенное клиентом, а не пересказом его правил.

    Совпадение с нашим представлением о httpx ничего не стоит, если httpx
    поступает иначе.
    """
    config = StorageConfig(**{**_CONFIG_FIELDS, "base_url": "HTTP://MinIO:80"})
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["host"] = request.headers["host"]
        return httpx.Response(200)

    storage = SpeechAudioStorage(
        config, client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
    )
    await storage.put("calls/2026/01/14/x.mp3", _chunks(b"a"))

    assert seen["host"] == config.host


async def test_a_storage_behind_a_prefix_keeps_the_prefix() -> None:
    """Адрес хранилища может нести путь, и он часть каждого запроса.

    Отброшенный префикс даёт `404` на объект, который лежит на месте, — и
    выглядит это как потерянная запись, а не как неверный адрес.
    """
    config = StorageConfig(**{**_CONFIG_FIELDS, "base_url": "http://storage.example:8080/s3/"})
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200)

    storage = SpeechAudioStorage(
        config, client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
    )
    await storage.put("calls/2026/01/14/x.mp3", _chunks(b"a"))

    assert seen["url"] == (
        "http://storage.example:8080/s3/speech-recordings/calls/2026/01/14/x.mp3"
    )
