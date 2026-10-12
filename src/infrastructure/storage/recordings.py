"""Хранилище аудиозаписей: собственный MinIO в нашем контуре.

Почему не переиспользуется `app/file_storage/`: тот клиент **только читает**, и
это не случайность его реализации — сервисная учётная запись корпоративного
хранилища read-only на стороне провайдера, запись там отказана. Speech Analytics
пишет, значит у него своё хранилище, свой бакет и свои ключи.

Подпись запросов переиспользуется целиком (`src/infrastructure/storage/signing.py`):
SigV4 написан руками, чтобы не тянуть botocore ради одного протокола, и
переписывать его второй раз незачем.

Раскладка ключей повторяет то, как это делает сам NCC:

    calls/ГГГГ/ММ/ДД/<sessionId>.mp3

Ключ детерминирован, поэтому повторное скачивание перезаписывает объект, а не
плодит копии — на этом стоит идемпотентность этапа скачивания.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Final
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict

from src.application.dto.recording import object_key as object_key
from src.application.errors.external import (
    StorageError as StorageError,
    StorageNotConfiguredError as StorageNotConfiguredError,
)
from src.infrastructure.storage.signing import canonical_uri, sign_request

#: Разумный потолок на размер одной записи.
#:
#: Замер 61 настоящей записи: средний размер 730 КБ, самая длинная (14 минут
#: 46 секунд) — 2,6 МБ, битрейт 24 кбит/с. Часовой разговор при таком битрейте
#: весит около 11 МБ. Потолок в 256 МБ выше всего мыслимого на порядок и стоит
#: здесь не ради экономии памяти, а чтобы источник, отдавший вместо записи
#: что-то другое, не съел память воркера целиком.
MAX_OBJECT_BYTES = 256 * 1024 * 1024

#: Тип содержимого записей NCC. Проверено на выгрузке: MPEG-2 Layer III.
AUDIO_CONTENT_TYPE = "audio/mpeg"


class StorageConfig(BaseModel):
    """Параметры доступа к корзине записей и подписи запросов."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    base_url: str
    bucket: str
    region: str
    access_key_id: str
    secret_key: str
    timeout_seconds: float = 60.0

    @property
    def host(self) -> str:
        """Хост для подписи — ровно в том виде, в каком его отправит клиент.

        Подпись SigV4 накрывает заголовок `Host`, а httpx перед отправкой
        приводит хост к нижнему регистру и убирает порт, если он для схемы
        стандартный. Подписав `MinIO:80`, мы отправим `minio` — и получим
        `403 SignatureDoesNotMatch`, то есть ответ, по которому ищут опечатку в
        ключах, а не в написании адреса.

        Настройка приходит от человека, и написать `HTTP://MinIO:80` он вправе:
        для всех остальных это тот же адрес.
        """
        split = urlsplit(self.base_url)
        if not split.netloc:
            raise StorageNotConfiguredError("storage base URL has no host")
        host = (split.hostname or "").lower()
        if not host:
            raise StorageNotConfiguredError("storage base URL has no host")
        # Литеральный IPv6 в заголовке `Host` остаётся в скобках.
        if ":" in host:
            host = f"[{host}]"
        port = split.port
        default = 443 if (split.scheme or "http").lower() == "https" else 80
        return host if port in (None, default) else f"{host}:{port}"

    @property
    def base_path(self) -> str:
        """Префикс пути из адреса хранилища, без завершающей косой.

        Хранилище может стоять за префиксом (`http://узел/s3`), и тогда он
        часть каждого запроса — и подписи тоже.
        """
        return urlsplit(self.base_url).path.rstrip("/")

    @property
    def scheme(self) -> str:
        """Вернуть протокол адреса хранилища из проверенной конфигурации."""
        return urlsplit(self.base_url).scheme or "http"


#: Сегменты пути, которых в ключе быть не может.
#:
#: Пустой сегмент даёт `//` и ключ, отличный от задуманного; `.` и `..` — выход
#: за префикс. Проверка структурная, а не «правильный ли это идентификатор
#: NCC»: хранилищу всё равно, чей источник, и правило обязано работать для
#: любого.
_UNSAFE_SEGMENTS: Final[frozenset[str]] = frozenset({"", ".", ".."})


def _safe_part(value: str, *, what: str) -> str:
    """Кусок ключа, в котором не спрятан переход по каталогам.

    Ключ собирается из данных **источника** — идентификатора сессии и даты, —
    а подписывается и отправляется дословно: `canonical_uri` оставляет `/` и
    точки как есть, потому что в ключе они законны. Значит `..` доедет до
    хранилища `..`, и объект ляжет не туда, куда мы считаем. Обратная косая
    здесь тоже запрещена: в ключе S3 это обычный символ, а в имени, которое
    кто-то потом развернёт на диск, — разделитель.
    """
    if "\\" in value:
        raise StorageError(f"{what} содержит обратную косую черту", reason="bad_object_key")
    for segment in value.split("/"):
        if segment in _UNSAFE_SEGMENTS:
            raise StorageError(
                f"{what} содержит сегмент {segment!r} — ключ вышел бы за свой префикс",
                reason="bad_object_key",
            )
    return value


class SpeechAudioStorage:
    """Запись, чтение и удаление аудио в собственном бакете."""

    def __init__(self, config: StorageConfig, *, client: httpx.AsyncClient | None = None) -> None:
        """Настроить SpeechAudioStorage из переданных параметров и зависимостей."""
        if not config.base_url.strip() or not config.bucket.strip():
            raise StorageNotConfiguredError(
                "speech audio storage needs SPEECH_STORAGE_BASE_URL and SPEECH_STORAGE_BUCKET"
            )
        if not config.access_key_id.strip() or not config.secret_key.strip():
            raise StorageNotConfiguredError("speech audio storage needs credentials")
        self._config = config
        self._client = client

    @asynccontextmanager
    async def _http(self) -> AsyncIterator[httpx.AsyncClient]:
        """Вернуть HTTP-клиент адаптера, создавая его только при выполнении операции."""
        if self._client is not None:
            yield self._client
            return
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            yield client

    def _uri(self, key: str) -> str:
        # Путь-стиль (`/bucket/key`), а не виртуальный хост: без подстановочного
        # DNS `bucket.host` не резолвится, и это ровно тот случай.
        #
        # Префикс из адреса входит и в запрос, и в подпись. Без него хранилище
        # за префиксом отвечало бы `404` на объект, который лежит на месте.
        """Закодировать ключ объекта для канонического URI подписи S3."""
        return canonical_uri(f"{self._config.base_path}/{self._config.bucket}/{key}")

    def _headers(self, method: str, key: str, payload_sha256: str) -> dict[str, str]:
        """Собрать заголовки технического запроса к внешнему сервису."""
        return sign_request(
            method=method,
            host=self._config.host,
            uri=self._uri(key),
            query={},
            region=self._config.region,
            access_key_id=self._config.access_key_id,
            secret_key=self._config.secret_key,
            now=datetime.now(UTC),
            payload_sha256=payload_sha256,
        )

    def _url(self, key: str) -> str:
        """Построить адрес объекта в настроенной корзине."""
        return f"{self._config.scheme}://{self._config.host}{self._uri(key)}"

    async def put(self, key: str, chunks: AsyncIterator[bytes]) -> int:
        """Положить объект. Возвращает записанный размер в байтах.

        Содержимое собирается в память, а не передаётся потоком. Подпись SigV4
        включает хеш тела, а посчитать его, не прочитав тело целиком, нельзя —
        либо буфер, либо `UNSIGNED-PAYLOAD`, который снимает проверку
        целостности. При измеренных размерах записей (в среднем 730 КБ,
        максимум единицы мегабайт) буфер честнее.
        """
        body = bytearray()
        async for chunk in chunks:
            body.extend(chunk)
            if len(body) > MAX_OBJECT_BYTES:
                raise StorageError(
                    f"object exceeds {MAX_OBJECT_BYTES} bytes; a call recording of that "
                    "size means the source returned something other than a recording"
                )
        payload = bytes(body)
        digest = hashlib.sha256(payload).hexdigest()
        headers = self._headers("PUT", key, digest)
        headers["Content-Type"] = AUDIO_CONTENT_TYPE

        async with self._http() as client:
            try:
                response = await client.put(self._url(key), content=payload, headers=headers)
            except httpx.HTTPError as exc:
                raise StorageError(f"storage unreachable while writing {key}") from exc
        if response.status_code not in (200, 201):
            # Код, но не тело: ответ хранилища может содержать имя бакета,
            # ключи и внутренние адреса.
            raise StorageError(f"storage refused the write of {key}: HTTP {response.status_code}")
        return len(payload)

    @asynccontextmanager
    async def open(
        self, key: str, *, byte_range: str | None = None
    ) -> AsyncIterator[httpx.Response]:
        """Открыть объект на чтение, при необходимости диапазоном.

        Отдаётся как поток: этим же путём аудио уходит в браузер, и читать
        запись целиком в память ради того, чтобы тут же её отдать, незачем.
        Диапазон передаётся как есть — плеер просит его сам, когда пользователь
        перематывает.
        """
        headers = self._headers("GET", key, hashlib.sha256(b"").hexdigest())
        if byte_range:
            headers["Range"] = byte_range
        async with self._http() as client:
            try:
                async with client.stream("GET", self._url(key), headers=headers) as response:
                    status = response.status_code
                    if status == 404:
                        raise StorageError(
                            "stored object was not found", reason="not_found", status_code=status
                        )
                    if status not in (200, 206):
                        raise StorageError("storage refused the read", status_code=status)
                    yield response
            except httpx.TimeoutException as exc:
                raise StorageError("storage read timed out", reason="timeout") from exc
            except httpx.HTTPError as exc:
                raise StorageError("storage unreachable while reading") from exc

    async def delete(self, key: str) -> bool:
        """Удалить объект. `True`, если его больше нет.

        Отсутствующий объект — успех, а не ошибка: удаление по сроку хранения
        обязано быть повторяемым, иначе один прерванный прогон навсегда
        оставляет задание, которое падает. Успех этот выражается кодом **204**,
        и никаким другим: S3 отвечает так и когда объект был, и когда его не
        было.

        `404` успехом **не** является, и это проверено на живом MinIO, а не
        выведено из документации: удаление несуществующего ключа в существующей
        корзине даёт 204, а 404 приходит на `NoSuchBucket`. Пока 404 считался
        успехом, опечатка в имени корзины превращала удаление по сроку хранения
        в отчёт об удалении записей разговоров, которых оно не касалось: ни
        одной ошибки в журнале, ни одной удалённой записи.
        """
        headers = self._headers("DELETE", key, hashlib.sha256(b"").hexdigest())
        async with self._http() as client:
            try:
                response = await client.delete(self._url(key), headers=headers)
            except httpx.HTTPError as exc:
                raise StorageError(f"storage unreachable while deleting {key}") from exc
        if response.status_code in (204, 200):
            return True
        if response.status_code == 404:
            raise StorageError(
                f"storage has no bucket {self._config.bucket!r}: deleting {key} returned 404, "
                f"which S3 uses for a missing bucket and never for a missing object"
            )
        raise StorageError(f"storage refused the delete of {key}: HTTP {response.status_code}")

    async def exists(self, key: str) -> bool:
        """Есть ли объект. Отказ доступа — исключение, а не «нет».

        Всё, кроме 200 и 404, обязано быть ошибкой. Пока `exists` возвращала
        `False` на любой не-200, просроченный ключ или отозванный доступ
        выглядели как «объекта нет»: скачивание начиналось заново, а удаление
        по сроку хранения считало запись уже уничтоженной. Ответ «нет» здесь
        принимают за факт, поэтому давать его наугад нельзя.
        """
        headers = self._headers("HEAD", key, hashlib.sha256(b"").hexdigest())
        async with self._http() as client:
            try:
                response = await client.head(self._url(key), headers=headers)
            except httpx.HTTPError as exc:
                raise StorageError(f"storage unreachable while probing {key}") from exc
        if response.status_code == 200:
            return True
        if response.status_code == 404:
            return False
        raise StorageError(
            f"storage would not say whether {key} exists: HTTP {response.status_code}"
        )


def build_storage(settings: object) -> SpeechAudioStorage:
    """Собрать хранилище по настройкам."""
    config = StorageConfig(
        base_url=str(getattr(settings, "speech_storage_base_url", "") or ""),
        bucket=str(getattr(settings, "speech_storage_bucket", "") or ""),
        region=str(getattr(settings, "speech_storage_region", "") or "us-east-1"),
        access_key_id=str(getattr(settings, "speech_storage_access_key_id", "") or ""),
        secret_key=str(getattr(settings, "speech_storage_secret_key", "") or ""),
        timeout_seconds=float(getattr(settings, "speech_storage_timeout_seconds", 60.0)),
    )
    return SpeechAudioStorage(config)
