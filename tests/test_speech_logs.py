"""Процессы речи не пишут адреса запросов в журнал.

Ссылка Телфина на запись бессрочная и без авторизации: попав в журнал
контейнера, она открывает разговор клиента любому, кто читает журнал (выпуск
2026-09-29, 20 строк в журналах воркеров). Контроль — первый тест: без
настройки `httpx` пишет адрес целиком, иначе второй тест ничего бы не доказывал.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator

import httpx
import pytest

from src.infrastructure.runtime.logs import URL_LOGGERS, configure_logging

SECRET = "FAKE-STORAGE-TOKEN-7f3c"
URL = f"https://storage.telphin.example/{SECRET}/record.mp3"


@pytest.fixture(autouse=True)
def restore_levels() -> Iterator[None]:
    saved = {name: logging.getLogger(name).level for name in URL_LOGGERS}
    yield
    for name, level in saved.items():
        logging.getLogger(name).setLevel(level)


def _logged_after_get(caplog: pytest.LogCaptureFixture) -> str:
    transport = httpx.MockTransport(lambda request: httpx.Response(200))
    with caplog.at_level(logging.INFO), httpx.Client(transport=transport) as client:
        client.get(URL)
    return caplog.text


def test_control_httpx_writes_the_full_url_at_info(caplog: pytest.LogCaptureFixture) -> None:
    logging.getLogger("httpx").setLevel(logging.NOTSET)
    assert SECRET in _logged_after_get(caplog)


def test_speech_logging_keeps_request_urls_out_of_the_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    configure_logging()
    assert SECRET not in _logged_after_get(caplog)
