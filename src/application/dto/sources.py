"""Метаданные звонка, которые возвращает источник записей."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field


class CallRef(BaseModel):
    """Один звонок, каким его знает источник — до всякой обработки.

    Обязательны только два поля: без идентификатора сессии звонок не опознать,
    без локатора не скачать. Всё остальное необязательно, и это не осторожность,
    а факт: локальная выгрузка не знает ни оператора, ни очереди, ни времени
    начала — только дату. Приложение обязано работать и без них, теряя фильтры,
    а не работу.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    audio_locator: str
    day: date
    #: `None`, когда источник знает только дату. Тогда карточка звонка
    #: показывает дату без времени, а не полночь: полночь у полутора тысяч
    #: звонков подряд выглядит как поломка, а не как отсутствие данных.
    started_at: datetime | None = None
    ended_at: datetime | None = None
    duration_s: int | None = None
    operator_login: str | None = None
    project_hint: str | None = None
    #: Устойчивый ключ проекта у источника; по нему приём заводит проект и
    #: связывает с ним звонок. `project_hint` — название для человека, и оно
    #: меняется (у «Волны» уточнится по ходу), ключ — нет. `None`, когда
    #: источник проекта не знает: тогда звонок остаётся без проекта.
    project_key: str | None = None
    #: Классификация обращения, которую выбрал оператор, — уровни по местам: у
    #: локальной выгрузки — путь каталогов файла, у `reporting_view` —
    #: `uroven_1…5`. Пустой уровень в середине — пустая строка, пустой хвост
    #: снят (`src.infrastructure.persistence.classifier.classifier_levels`). Приём связывает путь с
    #: узлом классификатора (SP-310) и пишет его непустые уровни в
    #: `speech_calls.disposition` через `DISPOSITION_SEPARATOR` — «проблема»
    #: звонка в отчёте (SP-604). Пусто, если источник классификации не знает.
    disposition_path: tuple[str, ...] = Field(default_factory=tuple)
    #: Источник звонка (`CallSource`): по нему воркер выбирает, откуда качать
    #: запись, а отчёты — какие звонки брать (S-35).
    source: str = "ncc"
    #: `inbound` / `outbound`; `None` — источник не говорит (NCC).
    direction: str | None = None
    #: Имя оператора, если источник его знает (Телфин — название добавочного).
    operator_name: str | None = None
    #: Номер клиента как его отдал источник. В базу не пишется: приём сохраняет
    #: только маску и хеш (ING-02).
    client_number: str | None = Field(default=None, repr=False)
    #: Ссылка на запись, если она — секрет сама по себе (Телфин: постоянная
    #: ссылка без аутентификации). `repr=False` — ссылка не должна попасть в
    #: журнал вместе с объектом.
    audio_url: str | None = Field(default=None, repr=False)
