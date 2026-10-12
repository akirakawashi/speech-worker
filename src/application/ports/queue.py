"""Граница очереди: что воркер о ней знает.

Одно решение стоит объяснить, потому что оно объясняет форму всего порта:
**часть операций принимает сессию, а часть открывает свою.**

`enqueue` и `complete` принимают сессию вызывающего, потому что обязаны
выполниться в **той же транзакции**, что и запись результата этапа. Иначе
появляется состояние «результат записан, задание не завершено» — и повторный
прогон переделает уже сделанную работу, — либо «задание завершено, результата
нет», и звонок молча застревает без единой ошибки в журнале.

`claim`, `heartbeat`, `fail`, `requeue_expired` и `snapshot` открывают свои:
они по смыслу вне транзакции обработки. `fail`, выполненный в транзакции,
которая тут же откатывается, не записал бы ни попытки, ни задержки.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Final, Protocol

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from src.domain.calls.contracts import JobErrorClass, JobKind, JobLane, JobStatus

#: Ключ в `payload` задания: раньше этого момента (ISO 8601) задание не начинали
#: намеренно — приём отложил звонок до ночи. Сам запрет держит `run_after`, но
#: его переписывают повторы и ожидание свободного слота; этот ключ не трогает
#: никто, и по нему видно, с какого момента задание действительно ждёт.
NOT_BEFORE_KEY: Final[str] = "not_before"

#: С какого момента задание ждёт воркера — выражением SQL над `speech_jobs`.
#: У обычного задания это момент создания, у отложенного — конец отсрочки.
#: По нему считается возраст очереди: считая от создания, отложенные до ночи
#: звонки каждый день показывали бы очередь зависшей на полсуток.


class NewJob(BaseModel):
    """Задание, которое ставят в очередь."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    call_id: uuid.UUID
    kind: JobKind
    lane: JobLane
    max_attempts: int
    payload: dict[str, JsonValue] = Field(default_factory=dict)
    #: Отложенный старт. `None` — можно брать сразу.
    run_after: datetime | None = None
    #: Аренда: сколько задание считается выполняющимся без подтверждения.
    #: У расшифровки она длиннее — часовая запись на загруженном Whisper
    #: обрабатывается дольше пяти минут, и короткая аренда вернула бы в очередь
    #: живую работу.
    lease_seconds: int = 300
    #: Приоритет: меньше — раньше. `None` — по полосе (`daily` — 0, `backfill` —
    #: 100). Своё значение ставит приём, когда у звонков одной полосы разная
    #: срочность: отложенные на ночь и звонки менее важного источника идут
    #: после свежих. Следующий этап наследует приоритет своего звонка.
    priority: int | None = None


class ClaimedJob(BaseModel):
    """Задание, захваченное воркером."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    id: uuid.UUID
    call_id: uuid.UUID
    kind: JobKind
    lane: JobLane
    attempts: int
    max_attempts: int
    payload: dict[str, JsonValue]
    lease_seconds: int
    #: Приоритет задания; с ним же ставится следующий этап. `None` — по полосе.
    priority: int | None = None

    @property
    def is_last_attempt(self) -> bool:
        """Последняя попытка: следующий отказ сделает задание `dead`.

        Обработчику это нужно, чтобы решить, писать ли частичный результат.
        """
        return self.attempts >= self.max_attempts


class ExpiredJob(BaseModel):
    """Задание, у которого истекла аренда, и что с ним стало.

    Не просто счётчик: если попытки исчерпаны, задание становится `dead`, и
    тогда звонок надо перевести в отказ. Вернув одно число, вызывающий не смог
    бы отличить «вернули в очередь» от «похоронили» и оставил бы звонок висеть
    в статусе «в очереди» навсегда.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    id: uuid.UUID
    call_id: uuid.UUID
    kind: JobKind
    status: JobStatus


class LaneDepth(BaseModel):
    """Число заданий и возраст ожидания в одной полосе очереди."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    lane: JobLane
    status: JobStatus
    count: int
    #: Возраст самого старого ожидающего задания в полосе. Главный показатель
    #: того, успевает ли ночная обработка к девяти утра.
    oldest_queued_age: timedelta | None = None


class QueueSnapshot(BaseModel):
    """Состояние очереди для панели администратора и метрик."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    lanes: tuple[LaneDepth, ...]
    dead_total: int
    failed_last_24h: int
    errors_by_class: dict[JobErrorClass, int]

    def queued(self, lane: JobLane) -> int:
        """Вернуть суммарное число ожидающих заданий всех полос."""
        return sum(d.count for d in self.lanes if d.lane == lane and d.status == JobStatus.QUEUED)

    def oldest_queued_age(self, lane: JobLane) -> timedelta | None:
        """Вернуть возраст старейшего ожидающего задания в секундах."""
        ages = [
            d.oldest_queued_age
            for d in self.lanes
            if d.lane == lane and d.status == JobStatus.QUEUED and d.oldest_queued_age is not None
        ]
        return max(ages) if ages else None


class ReprocessingCallNotFound(LookupError):
    """The call disappeared before its pipeline could be locked."""


class ReprocessingConflict(RuntimeError):
    """An active pipeline or an ineligible retry must not be overwritten."""


class JobQueue(Protocol):
    """Очередь заданий конвейера."""

    async def claim(
        self,
        lanes: Sequence[JobLane],
        worker_id: str,
        *,
        kinds: Sequence[JobKind] | None = None,
    ) -> ClaimedJob | None:
        """Взять одно задание. `None`, если брать нечего.

        `kinds` ограничивает этапы теми, которые воркер умеет обрабатывать.
        Умолчание — все: воркер, взявший чужой этап, провалит его и будет
        повторять, и снаружи это неотличимо от отказа сервиса.
        """
        ...

    async def heartbeat(self, job_id: uuid.UUID, worker_id: str) -> bool:
        """Продлить аренду. `False`, если задание уже отобрано другим."""
        ...

    async def fail(
        self,
        job_id: uuid.UUID,
        worker_id: str,
        error_class: JobErrorClass,
    ) -> JobStatus | None:
        """Записать отказ. Возвращает новое состояние задания."""
        ...

    async def defer(self, job_id: uuid.UUID, worker_id: str, *, delay_seconds: float) -> bool:
        """Вернуть задание в очередь, не считая это отказом.

        Нужна там, где работа не сделана и не провалилась: внешний ресурс занят.
        Отличается от `fail` в двух местах, и оба существенны.

        Класс ошибки не записывается: его отсутствие и есть признак того, что
        отказа не было. Иначе занятость GPU выглядела бы в панели
        администратора как сбой Whisper.

        Потраченная попытка **возвращается**. Попытка засчитывается при захвате,
        и без вычитания задание, которому просто не повезло с очередью, за
        `max_attempts` заходов стало бы `dead`, ни разу не дойдя до модели.
        """
        ...

    async def requeue_expired(self) -> Sequence[ExpiredJob]:
        """Разобрать задания с истёкшей арендой.

        Задание с непотраченными попытками возвращается в очередь; задание,
        исчерпавшее их, становится `dead`. Второе обязательно: воркер, который
        умер, а не отказал, через `fail` не проходит вовсе, и без проверки
        бюджета здесь такое задание повторялось бы вечно.
        """
        ...

    async def snapshot(self) -> QueueSnapshot:
        """Состояние очереди целиком."""
        ...
