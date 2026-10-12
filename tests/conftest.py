from __future__ import annotations

import os
from typing import Any

import pytest

from tests.speech_db_support import ENV_VAR as LIVE_DATABASE_ENV_VAR

#: Переменная, которой требуют строгости от прогона.
#:
#: Пропуск доказательства — законный ответ на машине без Postgres и ложь на
#: стенде, где база есть. Отличить одно от другого код не может: он видит
#: только отсутствие адреса. Поэтому решение объявляется снаружи, и по
#: умолчанию прогон не падает, а **говорит**.
STRICT_ENV_VAR = "SPEECH_REQUIRE_LIVE_PROOFS"


def _skip_reason(report: Any) -> str:
    """Причина пропуска из отчёта pytest.

    У пропуска `longrepr` — тройка `(файл, строка, "Skipped: причина")`. Форма
    недокументированная, поэтому читается осторожно: сводка не должна ронять
    прогон, если pytest однажды отдаст что-то другое.
    """
    longrepr = getattr(report, "longrepr", None)
    if isinstance(longrepr, tuple) and len(longrepr) == 3:
        return str(longrepr[2])
    return str(longrepr or "")


def _is_strict() -> bool:
    return (os.getenv(STRICT_ENV_VAR) or "").strip().lower() in {"1", "true", "yes", "on"}


def pytest_terminal_summary(terminalreporter: Any, exitstatus: int, config: Any) -> None:
    """Сказать вслух, чего прогон не проверил.

    Наборы доказательств речевого конвейера закрыты `skipif` по адресу живой
    базы: очередь держится на `FOR UPDATE SKIP LOCKED`, `ON CONFLICT` и
    блокировках, которых SQLite не выражает, — проверять их на нём значит
    проверять другую программу.

    Пропуск при этом невидим. В сводке он одно число рядом с числом успехов, и
    «199 skipped» читается как «всё хорошо, что-то неважное не пошло». Измерено
    2026-09-11: без адреса базы пропадает **199 проверок из 358** в тринадцати
    наборах — 56%, — и прогон остаётся зелёным.

    Поэтому здесь не строка, а блок, и в нём названо **что именно** осталось
    непроверенным. Число без имён не помогает: «пропущено 199» и «протокол
    очереди не проверялся» — разные сообщения для того, кто решает, выкатывать
    ли.
    """
    skipped = terminalreporter.stats.get("skipped", [])
    ours = [report for report in skipped if LIVE_DATABASE_ENV_VAR in _skip_reason(report)]
    if not ours:
        return

    by_suite: dict[str, int] = {}
    for report in ours:
        suite = str(getattr(report, "nodeid", "?")).split("::", 1)[0]
        by_suite[suite] = by_suite.get(suite, 0) + 1

    strict = _is_strict()
    terminalreporter.write_sep("=", "НЕ ПРОВЕРЕНО: доказательства на живой базе", red=strict)
    terminalreporter.write_line(
        f"Пропущено {len(ours)} проверок в {len(by_suite)} наборах: живой Postgres недоступен."
    )
    terminalreporter.write_line(
        "Это не «что-то неважное не пошло». Очередь, полнотекстовый поиск и "
        "политики удаления держатся на SQL, которого нет у SQLite; здесь они не "
        "проверялись вовсе."
    )
    terminalreporter.write_line("")
    for suite, count in sorted(by_suite.items(), key=lambda item: (-item[1], item[0])):
        terminalreporter.write_line(f"  {count:4d}  {suite}")
    terminalreporter.write_line("")
    terminalreporter.write_line(
        f"Чтобы проверить: заведи отдельную базу и выставь {LIVE_DATABASE_ENV_VAR} "
        "(см. README, раздел про живые тесты)."
    )
    if strict:
        terminalreporter.write_line(f"{STRICT_ENV_VAR} включена — прогон объявляется неуспешным.")
    else:
        terminalreporter.write_line(
            f"На стенде и в конвейере выставляй {STRICT_ENV_VAR}=1: там пропуск — отказ."
        )


def pytest_sessionfinish(session: Any, exitstatus: int) -> None:
    """В строгом режиме пропущенное доказательство — неуспех прогона.

    Меняется именно код возврата, а не результат отдельных тестов: пропуск не
    становится падением, потому что тест ничего не показал — ни успеха, ни
    отказа. Врать в другую сторону не лучше.
    """
    if not _is_strict():
        return
    reporter = session.config.pluginmanager.getplugin("terminalreporter")
    if reporter is None:
        return
    if any(
        LIVE_DATABASE_ENV_VAR in _skip_reason(report)
        for report in reporter.stats.get("skipped", [])
    ):
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


@pytest.fixture(autouse=True)
def blank_external_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Не дать тестам уйти в сеть с настоящими ключами разработчика.

    `Settings(...)` всегда читает локальный `.env` (pydantic-settings,
    `env_file`), даже когда тест задаёт другие поля явно. Живой ключ
    `OPENROUTER_*` или `TRANSCRIPTION_*` увёл бы тесты «распознавание не
    настроено» к провайдеру.

    `TRANSCRIPTION_ENABLED` намеренно не трогается: её умолчание `None` значит
    «унаследовать `OPENROUTER_ENABLED`», и закрепление `false` здесь перекрыло бы
    явные аргументы тестов, которые это наследование проверяют.
    """
    monkeypatch.setenv("OPENROUTER_ENABLED", "false")
    monkeypatch.setenv("OPENROUTER_API_KEY", "")
    monkeypatch.setenv("TRANSCRIPTION_PROVIDER", "")
    monkeypatch.setenv("TRANSCRIPTION_BASE_URL", "")
    monkeypatch.setenv("TRANSCRIPTION_API_KEY", "")
    monkeypatch.setenv("TRANSCRIPTION_MODEL", "")
