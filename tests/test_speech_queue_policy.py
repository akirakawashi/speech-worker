"""Политика повторов очереди: что ретраить, что хоронить, через сколько.

Файл отдельный и **без флага базы** намеренно. Эти проверки не нуждаются ни в
каком Postgres — это чистые функции, — но лежали они в
`test_speech_queue_protocol.py`, у которого на весь модуль стоит
`skipif(not SPEECH_POSTGRES_TEST_URL)`. Один из тестов там прямо носил
пояснение «чистая функция — проверяется без базы» и при этом не выполнялся ни
разу: переменную окружения в репозитории не выставляет ничто.

Политика описана в
`aicistent-documentation/speech-analytics/03-adr-postgres-job-queue.md`, §5.
Расхождение кода с этим документом — дефект кода.
"""

from __future__ import annotations

import pytest

from src.domain.calls.contracts import JobErrorClass, JobKind, is_terminal_error
from src.infrastructure.queue.postgres import backoff_seconds


def test_backoff_grows_and_is_capped_and_jittered() -> None:
    early = backoff_seconds(JobKind.TRANSCRIBE, JobErrorClass.TIMEOUT, 1)
    later = backoff_seconds(JobKind.TRANSCRIBE, JobErrorClass.TIMEOUT, 4)
    assert 24 <= early <= 36
    assert later > early
    # Двусторонне. Один только потолок доказывает, что значение «не больше», и
    # проходит на функции, возвращающей ноль, — то есть на конвейере, который
    # повторяет отказавшее заданиe немедленно и без конца. Проверяется, что
    # потолок **достигнут**: при девяноста девяти попытках задержка обязана
    # упереться в него, а не остаться на полпути.
    capped = backoff_seconds(JobKind.TRANSCRIBE, JobErrorClass.TIMEOUT, 99)
    assert 900 * 0.8 <= capped <= 900 * 1.2

    # Джиттер: одинаковых значений подряд быть не должно, иначе полторы тысячи
    # заданий повторятся в одну секунду.
    values = {backoff_seconds(JobKind.ANALYZE, JobErrorClass.TIMEOUT, 2) for _ in range(20)}
    assert len(values) > 1

    # У скачивания своё окно, шире общего. Тоже с обеих сторон: нижняя граница
    # одна прошла бы и на задержке в сутки, а «запись появится позже» — это
    # минуты, а не день.
    ingest = backoff_seconds(JobKind.INGEST, JobErrorClass.NOT_FOUND, 1)
    assert 48 <= ingest <= 72


@pytest.mark.parametrize("kind", list(JobKind))
def test_invalid_output_is_terminal_on_every_stage(kind: JobKind) -> None:
    """Невалидный ответ модели считается детерминированным.

    Повтор такого отказа занимает воркера и заканчивается тем же самым.
    """
    assert is_terminal_error(kind, JobErrorClass.INVALID_OUTPUT) is True


@pytest.mark.parametrize("kind", [JobKind.TRANSCRIBE, JobKind.ANALYZE])
def test_not_found_is_terminal_where_the_input_is_gone(kind: JobKind) -> None:
    """Пропал вход этапа — повторять нечего.

    `not_found` на этих этапах означает, что исчез объект аудио или транскрипт.
    Никакая попытка их не вернёт: нужна переобработка с более раннего этапа, и
    сказать об этом надо сразу, а не после того, как задание молча израсходует
    весь бюджет попыток.
    """
    assert is_terminal_error(kind, JobErrorClass.NOT_FOUND) is True


def test_not_found_on_ingest_is_retried() -> None:
    """У сбора `not_found` — это «записи ещё нет у источника».

    Ровно тот случай, ради которого у `ingest` своя, более широкая задержка
    (`backoff_seconds` её и даёт). Терминальность и задержка обязаны говорить
    одно и то же: пока они расходились, `ingest` честно ждал минуту между
    попытками, которых ему не полагалось.
    """
    assert is_terminal_error(JobKind.INGEST, JobErrorClass.NOT_FOUND) is False
    assert backoff_seconds(JobKind.INGEST, JobErrorClass.NOT_FOUND, 1) > backoff_seconds(
        JobKind.TRANSCRIBE, JobErrorClass.NOT_FOUND, 1
    )


@pytest.mark.parametrize(
    "error_class",
    [
        JobErrorClass.TIMEOUT,
        JobErrorClass.PROVIDER_ERROR,
        JobErrorClass.LEASE_EXPIRED,
        JobErrorClass.UNEXPECTED_ERROR,
    ],
)
@pytest.mark.parametrize("kind", list(JobKind))
def test_transient_classes_are_retried_everywhere(
    kind: JobKind, error_class: JobErrorClass
) -> None:
    """Всё, что может пройти со второго раза, хоронить нельзя.

    Бюджет попыток при этом остаётся: `max_attempts` кончится и без объявления
    класса терминальным.
    """
    assert is_terminal_error(kind, error_class) is False
