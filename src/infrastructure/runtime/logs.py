"""Журнал процессов речи: воркера, приёма, догрузки истории.

Уровень — INFO: ход конвейера нужен в `docker logs`. Но `httpx` на INFO пишет
полный адрес каждого запроса, а у Телфина ссылка на запись в хранилище
бессрочная и без авторизации — сама по себе ключ к разговору клиента. В журнале
контейнера она переживает и запись, и звонок.

Так и случилось 2026-09-29 при первом приёме Телфина: 20 скачиваний — 20 строк
со ссылками в журналах четырёх воркеров (ранбук выпуска
`runbooks/operations/2026-09-29-release-telphin-chat-files.md`, «стоп перед 2i»
и дальше). Поэтому `httpx` и `httpcore` во всех процессах речи — не ниже
WARNING: сбой запроса по-прежнему виден, а адрес успешного — нет.
"""

from __future__ import annotations

import logging
from typing import Final

#: Логгеры, которые на INFO пишут адрес запроса целиком.
URL_LOGGERS: Final[tuple[str, ...]] = ("httpx", "httpcore")

LOG_FORMAT: Final[str] = "%(levelname)s %(name)s %(message)s"


def configure_logging() -> None:
    """Журнал процесса речи: INFO, но без адресов запросов."""
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    for name in URL_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)


__all__ = ["LOG_FORMAT", "URL_LOGGERS", "configure_logging"]
