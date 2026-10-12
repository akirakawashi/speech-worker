"""Этапы конвейера: как они устроены и почему именно так.

Ключевое решение — **этап выполняется в две фазы**, и это не усложнение ради
симметрии.

Долгая часть (`work`) идёт **без открытой транзакции**: скачать запись, отдать
её Whisper, дождаться разметки. Это минуты. Транзакция, открытая на всё это
время, держит соединение в состоянии `idle in transaction`, удерживает снимок и
мешает автоочистке — на кластере, который делится с LLM и у которого запас
соединений измерялся единицами, это не мелочь.

Короткая часть (`store`) — одна транзакция, в которой происходит сразу три
вещи: пишется результат этапа, завершается задание и ставится следующее. Все
три обязаны быть неразделимы. Разорви их — и появится либо «результат записан,
задание не завершено» (повторный прогон переделает готовую работу), либо
«задание завершено, результата нет» (звонок молча застревает без единой ошибки
в журнале).

Обработчик не знает ни про очередь, ни про её протокол: он получает задание и
зависимости, возвращает результат. Заменить Postgres-очередь на брокер можно,
не трогая ни один обработчик.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field

from src.application.dto.pipeline import (
    StageResult as StageResult,
)
from src.application.dto.pipeline_settings import PipelineSettings
from src.application.ports.media import MediaProcessor
from src.application.ports.pipeline import StagePersistence, StageRepository
from src.application.ports.quality_execution import QualityExecutor
from src.application.ports.queue import ClaimedJob
from src.application.ports.recordings import (
    AnalysisBackend,
    RecordingFetcher,
    RecordingStorage,
    RecordingTranscriber,
)
from src.application.ports.workspace import RecordingWorkspace
from src.domain.calls.contracts import JobErrorClass, JobKind


class StageError(Exception):
    """Отказ этапа с известным классом.

    Класс, а не текст: в базу и в интерфейс администратора попадает только он.
    Тела ответов провайдеров, адреса и ключи туда не едут — это требование
    BR-13, и соблюсти его проще всего, если наружу просто нечего вынести.

    В журнале остаются только класс и идентификаторы: даже цепочка исключений
    может содержать исходную реплику или параметры SQL.
    """

    def __init__(self, error_class: JobErrorClass, message: str) -> None:
        """Настроить StageError из переданных параметров и зависимостей."""
        super().__init__(message)
        self.error_class = error_class


class StageHandler[W](ABC):
    """Один этап конвейера.

    Параметр типа — то, что долгая фаза передаёт короткой. Он объявлен явно,
    чтобы `store` не принимал `Any`: тип результата расшифровки и тип
    результата разбора не должны быть взаимозаменяемы по недосмотру.
    """

    kind: ClassVar[JobKind]

    @abstractmethod
    async def work(self, job: ClaimedJob, deps: StageDeps) -> W:
        """Долгая часть: сеть, ffmpeg, модели. Транзакции нет.

        Отказ выражается исключением `StageError` с классом. Всё остальное,
        что может здесь произойти, воркер считает `unexpected_error` — и это
        честнее, чем пытаться угадать класс по типу исключения.
        """

    def slot_limit(self, deps: StageDeps) -> int | None:
        """Сколько таких этапов может идти одновременно во всём стенде.

        `None` — без ограничения, и это верно для большинства этапов: они
        упираются в собственный процесс, которых столько, сколько воркеров.

        Число возвращает этап, который упирается во **внешний** ресурс, общий
        для всех воркеров. Спрашивает воркер, а знание живёт здесь: иначе воркер
        пришлось бы учить словам «расшифровка» и «Whisper», и общий цикл
        обработки перестал бы быть общим.

        Протокол ограничения — §8 ADR очереди.
        """
        return None

    @abstractmethod
    async def store(self, persistence: StagePersistence, job: ClaimedJob, work: W) -> StageResult:
        """Короткая часть: запись результата в транзакции вызывающего.

        Результат пишется **целиком, с удалением предыдущей версии**: этап
        может выполняться повторно, и дописывание к прошлому результату дало бы
        удвоенный транскрипт вместо нового.
        """


class StageDeps(BaseModel):
    """Всё, что этапам нужно снаружи.

    Собирается один раз при старте воркера и передаётся в обработчики. Так их
    можно проверять на подставных зависимостях, не поднимая ни Whisper, ни
    хранилища.
    """

    model_config = ConfigDict(frozen=False, arbitrary_types_allowed=True)

    repository: StageRepository | None = None
    workspace: RecordingWorkspace
    media: MediaProcessor | None = None
    fetcher: RecordingFetcher | None = None
    storage: RecordingStorage | None = None
    transcription: RecordingTranscriber | None = None
    analysis: AnalysisBackend | None = None
    evaluation: QualityExecutor | None = None
    #: Validated DB snapshot for this job; deployment secrets stay in settings.
    runtime_settings: PipelineSettings = Field(default_factory=PipelineSettings)


class StageRegistry:
    """Соответствие этапа обработчику.

    Реестр, а не `if/elif`: воркер обязан отказаться стартовать, если для
    какого-то этапа обработчика нет. Иначе задание такого этапа будет
    захватываться, падать и возвращаться в очередь до исчерпания попыток —
    это выглядит как сбой внешней системы, хотя код просто не написан.
    """

    def __init__(self) -> None:
        """Настроить StageRegistry из переданных параметров и зависимостей."""
        self._handlers: dict[JobKind, StageHandler[Any]] = {}

    def register(self, handler: StageHandler[Any]) -> None:
        """Зарегистрировать обработчик этапа; повторную регистрацию отклонить."""
        if handler.kind in self._handlers:
            raise ValueError(f"handler for stage {handler.kind.value!r} is already registered")
        self._handlers[handler.kind] = handler

    def get(self, kind: JobKind) -> StageHandler[Any]:
        """Вернуть зарегистрированный объект по ключу либо сообщить об отсутствии."""
        try:
            return self._handlers[kind]
        except KeyError:
            raise LookupError(f"no handler registered for stage {kind.value!r}") from None

    def assert_complete(self) -> None:
        """Все ли этапы закрыты обработчиками."""
        missing = sorted(kind.value for kind in JobKind if kind not in self._handlers)
        if missing:
            raise RuntimeError(
                "no handler for pipeline stages: "
                + ", ".join(missing)
                + ". A worker without them would claim such jobs, fail, and retry "
                "until the attempts run out -- which looks like an outage rather "
                "than unwritten code."
            )

    @property
    def kinds(self) -> frozenset[JobKind]:
        """Этапы, которые реестр умеет. Порядок цепочки — у `ordered_kinds`."""
        return frozenset(self._handlers)

    @property
    def ordered_kinds(self) -> tuple[JobKind, ...]:
        """То же, но в порядке цепочки: список, читающийся не по ней, путает."""
        return tuple(kind for kind in JobKind if kind in self._handlers)
