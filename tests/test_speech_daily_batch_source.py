"""Источник суточного пакета: разбор ключей и чтение локальной выгрузки.

Фикстуры синтетические. Настоящие записи — разговоры реальных клиентов, и
класть их в репозиторий ради теста нельзя; имена и раскладка каталогов взяты с
настоящей выгрузки, содержимое — нет.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.infrastructure.sources.ncc.source import (
    CallRef,
    DailyBatchSource,
    LocalExportSource,
    RecordingNotAvailable,
    build_daily_batch_source,
    parse_object_key,
)
from src.setting.config import Settings

# Имена — точь-в-точь как в переданной выгрузке: URL-кодированный ключ объекта.
KEY_14 = "ncc-tm%2F2026%2F01%2F14%2Fnode_1_domain_2_nauss_0_1766092829_1138986.mp3"
KEY_19 = "ncc-tm%2F2026%2F01%2F19%2Fnode_1_domain_3_nauss_0_1766092829_1385193.mp3"
KEY_14_B = "ncc-tm%2F2026%2F01%2F14%2Fnode_2_domain_1_nauss_1_1766092823_1603045.mp3"


def test_object_key_is_decoded_into_session_and_day() -> None:
    parsed = parse_object_key(KEY_14)
    assert parsed == ("node_1_domain_2_nauss_0_1766092829_1138986", date(2026, 1, 14))


@pytest.mark.parametrize(
    "name",
    [
        ".DS_Store",
        "readme.txt",
        # Правильная форма ключа, но идентификатор сессии не наумовский:
        # каталог с чужими файлами не должен заводить строки в базе.
        "ncc-tm%2F2026%2F01%2F14%2Fsomething-else.mp3",
        # Несуществующая дата.
        "ncc-tm%2F2026%2F13%2F45%2Fnode_1_domain_2_nauss_0_1_2.mp3",
    ],
)
def test_anything_that_is_not_a_call_is_refused(name: str) -> None:
    assert parse_object_key(name) is None


@pytest.fixture()
def export(tmp_path: Path) -> Path:
    """Выгрузка переменной глубины — как настоящая."""
    tree = {
        "В2В/Мобильная связь/Консультация/Финансы/Баланс_ расходы": [KEY_14],
        "В2В/ШПД/Недовольство_негатив/Не работает/Проводной интернет": [KEY_19],
        "Кошелек/Консультация/Не работает/ЛК_МП": [KEY_14_B],
    }
    for folder, files in tree.items():
        directory = tmp_path / folder
        directory.mkdir(parents=True, exist_ok=True)
        for name in files:
            (directory / name).write_bytes(b"ID3fake-audio-bytes")
    # Сор, который в настоящей выгрузке есть и который не должен стать звонком.
    (tmp_path / "В2В/ШПД/Консультация").mkdir(parents=True, exist_ok=True)
    (tmp_path / "В2В/ШПД/Консультация/.DS_Store").write_bytes(b"\x00")
    return tmp_path


async def test_lists_only_the_requested_day(export: Path) -> None:
    source = LocalExportSource(export)

    day14 = await source.list_calls(date(2026, 1, 14))
    day19 = await source.list_calls(date(2026, 1, 19))

    assert {ref.session_id for ref in day14} == {
        "node_1_domain_2_nauss_0_1766092829_1138986",
        "node_2_domain_1_nauss_1_1766092823_1603045",
    }
    assert [ref.session_id for ref in day19] == ["node_1_domain_3_nauss_0_1766092829_1385193"]


async def test_a_day_with_no_calls_is_an_empty_list_not_an_error(export: Path) -> None:
    assert await LocalExportSource(export).list_calls(date(2026, 1, 15)) == []


async def test_a_flat_directory_is_the_expected_production_shape(tmp_path: Path) -> None:
    """Настоящая суточная выгрузка — плоский каталог, без дерева тематик.

    Дерево есть только у подборки, на которой идёт разработка. Работает и то и
    другое, потому что всё нужное лежит в имени файла: оно и есть ключ объекта
    с датой и идентификатором сессии.
    """
    (tmp_path / KEY_14).write_bytes(b"ID3fake")
    (tmp_path / KEY_19).write_bytes(b"ID3fake")

    source = LocalExportSource(tmp_path)
    refs = await source.list_calls(date(2026, 1, 14))

    assert [r.session_id for r in refs] == ["node_1_domain_2_nauss_0_1766092829_1138986"]
    assert refs[0].disposition_path == (), "у плоского каталога классификации нет"
    assert await source.available_days() == [date(2026, 1, 14), date(2026, 1, 19)]


async def test_directories_are_recorded_when_present(export: Path) -> None:
    """Каталоги, если они есть, сохраняются как есть и ни на что не влияют.

    Переменная глубина здесь существенна: у направления «В2В» есть уровень
    продукта, у «Кошелька» его нет. Разбор по фиксированным уровням сломался бы
    на втором каталоге подборки — а в настоящей выгрузке каталогов не будет
    вовсе.
    """
    (ref,) = await LocalExportSource(export).list_calls(date(2026, 1, 19))
    assert ref.disposition_path == (
        "В2В",
        "ШПД",
        "Недовольство_негатив",
        "Не работает",
        "Проводной интернет",
    )

    wallet = [
        r
        for r in await LocalExportSource(export).list_calls(date(2026, 1, 14))
        if r.disposition_path and r.disposition_path[0] == "Кошелек"
    ]
    assert wallet[0].disposition_path == ("Кошелек", "Консультация", "Не работает", "ЛК_МП")


async def test_unknown_fields_are_none_rather_than_invented(export: Path) -> None:
    """Локальная выгрузка не знает оператора, очередь и время начала.

    Это записано в типе, а не в комментарии: подставить полночь и пустую
    строку значило бы показать пользователю выдуманные данные как настоящие.
    """
    (ref,) = await LocalExportSource(export).list_calls(date(2026, 1, 19))
    assert ref.started_at is None
    assert ref.ended_at is None
    assert ref.operator_login is None
    assert ref.project_hint is None
    assert ref.duration_s is None
    assert ref.day == date(2026, 1, 19)


async def test_the_same_call_in_two_folders_is_listed_once(tmp_path: Path) -> None:
    """Запись может лежать сразу в двух тематиках — звонок от этого не два."""
    for folder in ("Кошелек/Консультация/Тема А", "Кошелек/Консультация/Тема Б"):
        directory = tmp_path / folder
        directory.mkdir(parents=True, exist_ok=True)
        (directory / KEY_14).write_bytes(b"ID3fake")

    refs = await LocalExportSource(tmp_path).list_calls(date(2026, 1, 14))

    assert len(refs) == 1


async def test_audio_is_streamed_not_read_whole(export: Path) -> None:
    source = LocalExportSource(export, chunk_size=4)
    (ref,) = await source.list_calls(date(2026, 1, 19))

    chunks = [chunk async for chunk in await source.open_audio(ref)]

    assert len(chunks) > 1, "поток обязан отдаваться кусками, а не одним куском"
    assert b"".join(chunks) == b"ID3fake-audio-bytes"


async def test_available_days_are_discovered_from_the_export(export: Path) -> None:
    assert await LocalExportSource(export).available_days() == [
        date(2026, 1, 14),
        date(2026, 1, 19),
    ]


async def test_missing_export_directory_raises_rather_than_returning_nothing(
    tmp_path: Path,
) -> None:
    """Пустая выдача и отсутствующий каталог — разные события.

    Первое означает выходной день, второе — что источник настроен неверно.
    Вернув на оба пустой список, суточный прогон записал бы «за эту дату
    звонков нет» и пошёл дальше.
    """
    with pytest.raises(FileNotFoundError):
        await LocalExportSource(tmp_path / "нет-такого").list_calls(date(2026, 1, 14))


async def test_audio_that_disappeared_between_listing_and_download_raises(export: Path) -> None:
    """`RecordingNotAvailable`, а не `FileNotFoundError`.

    Отсутствие записи — часть контракта порта, а не деталь файлового адаптера:
    этап скачивания обязан отличать его от поломки источника одинаково для всех
    адаптеров, и `404` от REST означает ровно то же самое.
    """
    source = LocalExportSource(export)
    (ref,) = await source.list_calls(date(2026, 1, 19))
    Path(ref.audio_locator).unlink()

    with pytest.raises(RecordingNotAvailable):
        await source.open_audio(ref)


def test_local_export_satisfies_the_port(export: Path) -> None:
    assert isinstance(LocalExportSource(export), DailyBatchSource)


def test_call_ref_is_immutable() -> None:
    """Ссылку на звонок конвейер не правит: он на неё смотрит."""
    ref = CallRef(session_id="node_1_domain_1_nauss_1_1_1", audio_locator="x", day=date(2026, 1, 1))
    with pytest.raises(ValidationError, match="frozen_instance"):
        ref.session_id = "other"  # type: ignore[misc]


def test_factory_refuses_an_unknown_source_kind() -> None:
    settings = Settings(_env_file=None, speech_ncc_source_kind="magic")  # type: ignore[call-arg]
    with pytest.raises(RuntimeError, match="is unknown"):
        build_daily_batch_source(settings)


def test_factory_refuses_local_export_without_a_directory() -> None:
    """Ненастроенный источник обязан останавливать прогон, а не молчать.

    Пустой каталог и «звонков за дату нет» неотличимы по результату, и второе
    обнаруживается только через неделю пустого поиска.
    """
    settings = Settings(  # type: ignore[call-arg]
        _env_file=None, speech_ncc_source_kind="local_export", speech_ncc_export_dir=""
    )
    with pytest.raises(RuntimeError, match="SPEECH_NCC_EXPORT_DIR"):
        build_daily_batch_source(settings)


def test_factory_builds_the_local_export_source(export: Path) -> None:
    settings = Settings(  # type: ignore[call-arg]
        _env_file=None, speech_ncc_source_kind="local_export", speech_ncc_export_dir=str(export)
    )
    source = build_daily_batch_source(settings)
    assert isinstance(source, LocalExportSource)
    assert source.name == "local-export"


def test_factory_rejects_the_abandoned_case_listing_source() -> None:
    """Режим rest должен остановить запуск, а не вернуть пустой список звонков."""
    settings = Settings(_env_file=None, speech_ncc_source_kind="rest")
    with pytest.raises(RuntimeError, match="опровергнут"):
        build_daily_batch_source(settings)


def test_declared_but_unimplemented_kinds_say_so() -> None:
    """`storage` объявлен и не реализован — это разные состояния от опечатки."""
    settings = Settings(_env_file=None, speech_ncc_source_kind="storage")  # type: ignore[call-arg]
    with pytest.raises(RuntimeError, match="not implemented yet"):
        build_daily_batch_source(settings)
