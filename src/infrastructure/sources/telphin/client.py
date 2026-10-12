"""REST API Телфина: токен и история вызовов (фича 14, §4).

Только то, что нужно приёму: токен доверенного приложения
(`client_credentials`), кто мы (`/user/`) и `call_history`. Остальное API —
настройка АТС, и приложению уровня Call API оно не нужно.

Факты — из руководства поставщика и разведки 2026-09-28 (журнал разработки,
«Телфин: разведка API»): токен живёт час и токена обновления не имеет; лимит —
на ресурс, у `call_history` — 400 запросов в час, общий на все приложения
клиента; `per_page=1000` принимается; времена — UTC.

Тексты ошибок — без тел ответов и без адресов: в теле ответа бывают номера
клиентов, а в адресе записи — доступ к разговору.
"""

from __future__ import annotations

import time as clock
from collections.abc import Sequence
from typing import Any
from urllib.parse import quote

import httpx

API_PREFIX = "/api/ver1.0"
TOKEN_PATH = "/oauth/token"

#: Запас до истечения токена: запрос, начатый за секунду до конца часа, не
#: должен уйти с токеном, который истечёт в пути.
TOKEN_MARGIN_S = 60


class TelphinError(RuntimeError):
    """API или хранилище Телфина ответили не так, как ожидалось."""


class TelphinRateLimited(TelphinError):
    """Лимит запросов часа исчерпан (`429`): следующий запрос — в следующем часе.

    Не ждём сброса: лимит общий на все приложения клиента, и прогон, час
    просидевший в ожидании, занял бы таймер. Следующий запуск повторит сутки —
    приём идемпотентен.
    """

    def __init__(self, reset_s: int | None) -> None:
        """Настроить TelphinRateLimited из переданных параметров и зависимостей."""
        tail = f"; resets in {reset_s} s" if reset_s is not None else ""
        super().__init__(f"telphin rate limit exhausted{tail}")
        self.reset_s = reset_s


def _int(raw: str | None) -> int | None:
    """Прочитать целое поле источника с обработкой пустого значения."""
    try:
        return int(raw) if raw is not None else None
    except ValueError:
        return None


def _query(params: Sequence[tuple[str, object]]) -> str:
    # Пробел — `%20`, как в примерах руководства: `start_datetime` содержит его.
    """Выполнить запрос к источнику с действующим токеном."""
    return "&".join(f"{key}={quote(str(value), safe='')}" for key, value in params)


class TelphinClient:
    """Клиент API от имени доверенного приложения."""

    def __init__(
        self,
        *,
        base_url: str,
        app_id: str,
        app_secret: str,
        timeout_s: float,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        """Настроить TelphinClient из переданных параметров и зависимостей."""
        self._base = base_url.rstrip("/")
        self._app_id = app_id
        self._app_secret = app_secret
        # `Accept-Encoding: gzip` руководство требует явно (с. 33). Прокси
        # (`HTTPS_PROXY` → ssrf_proxy) httpx берёт из окружения.
        self._http = http or httpx.AsyncClient(
            timeout=timeout_s, headers={"Accept-Encoding": "gzip"}
        )
        self._token: str | None = None
        self._token_until = 0.0
        self._client_id: int | None = None

    async def aclose(self) -> None:
        """Закрыть принадлежащий адаптеру HTTP-клиент."""
        await self._http.aclose()

    async def _new_token(self) -> str:
        """Запросить новый токен источника и сохранить срок его действия."""
        try:
            response = await self._http.post(
                self._base + TOKEN_PATH,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._app_id,
                    "client_secret": self._app_secret,
                },
            )
        except httpx.HTTPError as exc:
            raise TelphinError(f"telphin token request failed: {type(exc).__name__}") from None
        if response.status_code != 200:
            raise TelphinError(f"telphin token: HTTP {response.status_code}")
        body = response.json()
        token = body.get("access_token") if isinstance(body, dict) else None
        if not isinstance(token, str) or not token:
            raise TelphinError("telphin token: no access_token in the answer")
        expires = _int(str(body.get("expires_in") or "3600")) or 3600
        self._token = token
        self._token_until = clock.monotonic() + max(0, expires - TOKEN_MARGIN_S)
        return token

    async def _bearer(self) -> str:
        """Получить действующий токен доступа, обновив его при необходимости."""
        if self._token is None or clock.monotonic() >= self._token_until:
            return await self._new_token()
        return self._token

    async def get(self, path: str, params: Sequence[tuple[str, object]] = ()) -> Any:
        """`GET` ресурса API. `401` — один новый токен и один повтор."""
        url = self._base + API_PREFIX + path + (f"?{_query(params)}" if params else "")
        response: httpx.Response | None = None
        for attempt in (1, 2):
            bearer = await self._bearer()
            try:
                response = await self._http.get(url, headers={"Authorization": f"Bearer {bearer}"})
            except httpx.HTTPError as exc:
                raise TelphinError(f"telphin GET {path}: {type(exc).__name__}") from None
            if response.status_code == 401 and attempt == 1:
                # Токен отозван раньше срока: новый — и ещё раз, но только раз.
                self._token = None
                continue
            break
        assert response is not None
        if response.status_code == 429:
            raise TelphinRateLimited(_int(response.headers.get("X-RateLimit-Reset")))
        if response.status_code != 200:
            raise TelphinError(f"telphin GET {path}: HTTP {response.status_code}")
        return response.json()

    async def client_id(self) -> int:
        """Клиент ВАТС учётной записи приложения.

        Учётка обязана быть уровня клиента: `call_history` есть только у
        клиента, а учётка группы или добавочного видела бы часть АТС молча.
        """
        if self._client_id is None:
            user = await self.get("/user/")
            if not isinstance(user, dict) or user.get("client_id") is None:
                raise TelphinError("telphin /user/: no client_id in the answer")
            if user.get("extension_group_id") is not None or user.get("extension_id") is not None:
                raise TelphinError(
                    "telphin: the API account is not client-level; call_history needs the client"
                )
            self._client_id = int(user["client_id"])
        return self._client_id

    async def call_history(
        self, *, start: str, end: str, page: int, per_page: int
    ) -> list[dict[str, Any]]:
        """Одна страница исходящих вызовов интервала, с частями и записями, по времени."""
        client = await self.client_id()
        body = await self.get(
            f"/client/{client}/call_history/",
            [
                ("start_datetime", start),
                ("end_datetime", end),
                ("flow", "out"),
                ("order", "asc"),
                ("per_page", per_page),
                ("page", page),
            ],
        )
        items = body.get("call_history") if isinstance(body, dict) else None
        if not isinstance(items, list):
            raise TelphinError("telphin call_history: no call_history list in the answer")
        return [item for item in items if isinstance(item, dict)]
