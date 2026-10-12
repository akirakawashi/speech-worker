"""Маска и псевдоним номера телефона.

Сырой номер клиента в системе не хранится нигде. Эти функции — единственный
способ, которым номер вообще попадает в базу, поэтому проверяются подробно.
"""

from __future__ import annotations

import pytest

from src.domain.calls.phone import (
    MASK_CHAR,
    hash_phone_number,
    mask_phone_number,
    mask_phones_in_snapshot,
    normalize_phone,
)

PEPPER = "test-pepper-not-a-real-secret"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("+7 (999) 123-45-67", "79991234567"),
        ("89991234567", "79991234567"),
        ("79991234567", "79991234567"),
        ("+7-999-123-45-67", "79991234567"),
    ],
)
def test_the_same_number_written_differently_normalises_the_same(raw: str, expected: str) -> None:
    """Иначе один клиент дал бы несколько разных псевдонимов.

    `8` в начале — тот же номер, что и `7`; без приведения группировка звонков
    одного человека распалась бы на две.
    """
    assert normalize_phone(raw) == expected


@pytest.mark.parametrize("raw", ["", "   ", "abc", "1234", "-"])
def test_what_is_not_a_number_is_refused(raw: str) -> None:
    """Короче пяти цифр — внутренний добавочный, а не номер клиента."""
    assert normalize_phone(raw) is None
    assert mask_phone_number(raw) is None


def test_mask_keeps_only_the_last_four_digits() -> None:
    """По четырём последним человек узнаёт звонок; восстановить номер нельзя."""
    masked = mask_phone_number("+7 (999) 123-45-67")
    assert masked is not None
    assert masked.endswith("4567")
    assert "999" not in masked
    assert "123" not in masked
    assert masked.startswith("+7")


def test_hash_is_stable_across_spellings_and_differs_across_numbers() -> None:
    a = hash_phone_number("+7 (999) 123-45-67", PEPPER)
    b = hash_phone_number("89991234567", PEPPER)
    other = hash_phone_number("+7 (999) 123-45-68", PEPPER)

    assert a == b, "один номер — один псевдоним"
    assert a != other


def test_hash_depends_on_the_pepper() -> None:
    """Смена соли обесценивает старые псевдонимы — так и задумано."""
    assert hash_phone_number("79991234567", "one") != hash_phone_number("79991234567", "two")


def test_hashing_without_a_pepper_is_refused() -> None:
    """Несолёный хеш номера подбирается перебором за минуты.

    Отказ, а не хеш без соли: молча выдав его, мы получили бы поле, которое
    выглядит псевдонимом и им не является.
    """
    with pytest.raises(ValueError, match="pepper"):
        hash_phone_number("79991234567", "  ")


def _digits(value: object) -> str:
    """Только цифры из значения — как их увидел бы тот, кто ищет номер.

    Проверять снимок подстрокой `"9991234567"` бесполезно: в записи
    `+7 (999) 123-45-67` между цифрами стоят скобки и дефисы, и такая проверка
    проходит, даже когда номер лежит в базе целиком. Ровно это и скрывало
    дефект: маска не срабатывала, а тест был зелёный.
    """
    return "".join(ch for ch in str(value) if ch.isdigit())


def test_numbers_inside_a_source_snapshot_are_masked_on_write() -> None:
    """Снимок записи источника кладётся в базу уже без номеров.

    Маскировать при чтении нельзя: однажды кто-нибудь прочитает снимок мимо
    маскирующего кода — и это будет обычный `SELECT`, а не ошибка.
    """
    snapshot = {
        "comment": "клиент просил перезвонить на +7 (999) 123-45-67",
        "nested": {"phones": ["89991234567", "не номер"]},
        "duration": 120,
        "flag": True,
    }

    masked = mask_phones_in_snapshot(snapshot)

    assert "9991234567" not in _digits(masked)
    assert "4567" in str(masked), "хвост оставлен для узнавания"
    assert masked["duration"] == 120, "нестроковые значения не трогаются"
    assert masked["flag"] is True
    assert "не номер" in str(masked)


@pytest.mark.parametrize(
    "written",
    [
        "+7 (999) 123-45-67",
        "8 (999) 123-45-67",
        "8-999-123-45-67",
        "8 999 123 45 67",
        "+7 999 123 45 67",
        "(999) 123-45-67",
        "89991234567",
        "79991234567",
        "9991234567",
    ],
    ids=lambda w: w,
)
def test_every_written_form_of_a_number_is_masked(written: str) -> None:
    """Номер маскируется во всех записях, которыми его пишут люди.

    Скобки и пробелы в `+7 (999) 123-45-67` — это самая обычная запись
    российского номера, и именно её прежняя редакция пропускала: разделитель
    групп допускался ровно один символ, а здесь их два (` (` и `) `). Номер
    уходил в базу сырым.
    """
    masked = mask_phones_in_snapshot({"any_field": written})

    assert "9991234567" not in _digits(masked["any_field"])
    assert str(masked["any_field"]).endswith("4567")


@pytest.mark.parametrize(
    "value",
    [
        "ncc-tm/2026/01/14/node_1_domain_2_nauss_0_1766092829_1138986.mp3",
        "node_1_domain_2_nauss_0_1766092829_1138986",
        "2026-01-14",
        "2026-01-14T00:00:00+00:00",
        "20260114",
        "daily-batch",
        "second",
        "VolnaAPI",
        "заказ 1766092829 закрыт",
        "договор 123456789012345",
    ],
    ids=lambda v: v[:32],
)
def test_identifiers_and_dates_survive_masking_untouched(value: str) -> None:
    """Идентификаторы и даты не трогаются.

    Не косметика: `audio_locator` — единственный способ снова добраться до
    записи в источнике. Прежняя редакция видела «телефон» в отметке времени
    внутри `sessionId` (`1766092829`) и портила локатор **в каждом** снимке,
    а `day` вида `2026-01-14` — принимала за номер целиком.
    """
    assert mask_phones_in_snapshot({"any_field": value})["any_field"] == value


def test_a_number_arriving_as_a_json_number_is_masked_too() -> None:
    """`{"caller": 79991234567}` — тот же номер, просто без кавычек.

    Обход маски через тип значения ничем не лучше обхода через форму записи:
    в базу попадёт то же самое.
    """
    masked = mask_phones_in_snapshot({"caller": 79991234567, "duration": 120, "flag": True})

    assert "9991234567" not in _digits(masked["caller"])
    assert masked["duration"] == 120, "число, не похожее на номер, остаётся числом"
    assert masked["flag"] is True, "булево значение в Python тоже int — и не номер"


def test_a_short_number_is_not_disclosed_by_its_own_mask() -> None:
    """Маска, показывающая весь вход, — это не маска.

    При хвосте ровно в четыре цифры пятизначный номер печатался как `+1•2345`:
    первая цифра плюс четыре последних — весь номер целиком, поданный как
    скрытый.
    """
    masked = mask_phone_number("12345")

    assert masked is not None
    assert "12345" not in _digits(masked)
    assert MASK_CHAR in masked


def test_the_mask_of_a_full_number_is_unchanged() -> None:
    """Ограничение хвоста не должно менять то, что видит пользователь.

    У российского номера половина длиннее четырёх, поэтому хвост остаётся
    четырёхзначным — по нему человек и узнаёт свой звонок.
    """
    assert mask_phone_number("+7 (999) 123-45-67") == f"+7{MASK_CHAR * 6}4567"
    assert mask_phone_number("9991234567") == f"+9{MASK_CHAR * 5}4567"
