"""Охрана вокруг адреса тестовой базы.

Проверяется не «функция возвращает строку», а то, ради чего она написана: URL,
направленный на рабочую базу, обязан остановить сбор набора **до** первого
`DELETE FROM speech_calls`.
"""

from __future__ import annotations

import pytest

from tests.speech_db_support import (
    ENV_VAR,
    UnsafeTestDatabase,
    speech_test_database_url,
)

_DSN = "postgresql+asyncpg://postgres:secret@db:5432/"


def test_an_unset_variable_is_a_skip_not_a_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """На машине без Postgres доказательные наборы пропускаются — это норма."""
    monkeypatch.delenv(ENV_VAR, raising=False)
    assert speech_test_database_url() is None

    monkeypatch.setenv(ENV_VAR, "   ")
    assert speech_test_database_url() is None, "пробелы — это тоже «не задано»"


@pytest.mark.parametrize(
    "database",
    ["speech_test", "speech-test", "test_speech", "speech_ci", "TEST", "ci"],
)
def test_a_database_named_as_a_test_database_passes(
    monkeypatch: pytest.MonkeyPatch, database: str
) -> None:
    monkeypatch.setenv(ENV_VAR, _DSN + database)
    assert speech_test_database_url() == _DSN + database


@pytest.mark.parametrize(
    "database",
    [
        "speech",  # ровно тот случай, который стоил разобранного корпуса
        "speech_dev",
        "speech_local",
        "postgres",
        "contest",  # «test» внутри слова — не имя тестовой базы
        "latest",
    ],
)
def test_a_database_not_named_as_a_test_database_stops_the_run(
    monkeypatch: pytest.MonkeyPatch, database: str
) -> None:
    """Отказ, а не пропуск: пропуск заметили бы через день, а данные удаляются сразу."""
    monkeypatch.setenv(ENV_VAR, _DSN + database)
    with pytest.raises(UnsafeTestDatabase) as failure:
        speech_test_database_url()

    # Сообщение обязано говорить, что делать: тот, кто его увидит, запускал
    # тесты, а не читал этот модуль.
    assert "createdb speech_test" in str(failure.value)
    assert database in str(failure.value)


def test_query_parameters_do_not_hide_the_database_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`?ssl=require` в хвосте не должен превращать `speech` в «нераспознано»."""
    monkeypatch.setenv(ENV_VAR, _DSN + "speech?ssl=require")
    with pytest.raises(UnsafeTestDatabase):
        speech_test_database_url()
