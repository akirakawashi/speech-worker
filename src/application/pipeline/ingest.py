"""Этап `ingest`: запись переезжает от источника к нам.

Три исхода, и все три штатные:

* запись скачана и лежит в нашем хранилище — дальше расшифровка;
* звонок короче порога — записи не берём вовсе, цепочка кончается (ING-05);
* записи у источника нет — после исчерпания окна повторов цепочка кончается
  тоже, и звонок помечается `unavailable_at_source` (ING-04).

Третий исход — не отказ, и это главное решение этого модуля. Сбор идёт вечером,
когда записи за сутки давно готовы; отсутствие записи в это время означает, что
её не будет, а не что источник не успел. Оформив это отказом, мы получили бы
задание, которое умирает, звонок в статусе «сломан» и ежедневный поток ложных
тревог ровно там, где ничего не сломано.

Почему файл едет через диск, а не прямо из источника в хранилище: `ffprobe`
работает с файлом, а число каналов знать обязательно — на нём построено
определение ролей. Оценка объёма делает это безболезненным: средняя запись
730 КБ, а `/tmp` у воркера — tmpfs на 4 ГБ.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from src.application.dto.media import AudioProbe
from src.application.dto.pipeline import IngestWork as IngestWork
from src.application.dto.pipeline_input import CallForIngest
from src.application.dto.pipeline_settings import PipelineSettings
from src.application.dto.sources import CallRef
from src.application.errors.external import MediaError, RecordingNotAvailable, StorageError
from src.application.pipeline.stages import StageDeps, StageError, StageHandler, StageResult
from src.application.ports.pipeline import StagePersistence
from src.application.ports.queue import ClaimedJob
from src.application.recordings import object_key
from src.domain.calls.contracts import (
    AudioStatus,
    JobErrorClass,
    JobKind,
    ProcessingStatus,
)

logger = logging.getLogger(__name__)

#: Насколько измеренная длительность может расходиться с заявленной источником,
#: прежде чем это попадёт в журнал. Не отказ: расхождение означает неточность
#: данных источника, а не испорченную запись, и терять из-за него разговор
#: нельзя. Но молчать о нём тоже нельзя — систематическое расхождение означает,
#: что мы скачиваем не те файлы.
DURATION_MISMATCH_RATIO = 0.1

#: Сегмент и продукт берутся у проекта тем же запросом. Отдельным он стоил бы
#: второго обращения к базе на каждый звонок ради двух строковых полей.

#: `COALESCE` на измерениях — не осторожность, а необходимость. Повторный
#: прогон над уже сохранённым объектом ничего не измеряет: он лишь убеждается,
#: что объект на месте. Без `COALESCE` такая проверка обнулила бы число каналов
#: и байт, а `duration_s` объявлен `NOT NULL` — то есть просто отказала бы.

#: Ссылка на запись, которая сама по себе секрет (Телфин), нужна только до
#: скачивания. Стирается при **любом** исходе этапа: забранная запись в ней не
#: нуждается, короткий звонок и звонок без записи — тем более.


#: Сегмент и продукт из сопоставленного проекта. Пишутся отдельным запросом,
#: потому что нужны при **любом** исходе этапа: короткий звонок и звонок без
#: записи тоже попадают в фильтры, и оставить их без измерений значило бы
#: спрятать их от половины запросов.


class IngestHandler(StageHandler[IngestWork]):
    """Скачать запись звонка в наше хранилище."""

    kind = JobKind.INGEST

    async def work(self, job: ClaimedJob, deps: StageDeps) -> IngestWork:
        """Загрузить снимок и выполнить долгую фазу этапа через порты; вернуть DTO для записи."""
        call = await self._load_call(job, deps)
        log = logger.getChild("work")
        # Сегмент и продукт известны из проекта независимо от того, чем
        # кончится скачивание, и приписываются к любому исходу: короткий звонок
        # и звонок без записи тоже ищутся по фильтрам.
        dims = {"segment": call.project_segment, "product": call.project_product}

        threshold = _short_call_threshold(deps.runtime_settings)
        stated = int(call.duration_s or 0)
        # Ноль — это «источник не сказал», а не «звонок нулевой длины»: локальная
        # выгрузка длительности не знает вовсе. Пропускать по нулю значило бы
        # пропустить весь пакет.
        if 0 < stated < threshold:
            log.info(
                "call=%s is %ss long, below the %ss threshold: not fetching its audio",
                job.call_id,
                stated,
                threshold,
            )
            return IngestWork(outcome=IngestWork.SKIPPED_SHORT, **dims)

        key = object_key(str(call.session_id), call.started_at)
        if call.audio_status == AudioStatus.STORED.value and call.audio_object_key == key:
            stored = await self._already_stored(deps, key)
            if stored is not None:
                # Повторный прогон за ту же дату — обычное дело (ING-01), и
                # перекачивать из-за него сохранённое незачем.
                log.info("call=%s already has its audio at %s", job.call_id, key)
                return stored.model_copy(update=dims)

        ref = _call_ref(call)
        try:
            return await self._fetch(job, deps, ref, key, dims)
        except RecordingNotAvailable as exc:
            return self._give_up_or_retry(job, exc, log, dims)

    def _give_up_or_retry(
        self,
        job: ClaimedJob,
        exc: RecordingNotAvailable,
        log: logging.Logger,
        dims: dict[str, Any],
    ) -> IngestWork:
        """Ещё подождать или признать, что записи не будет.

        Решается здесь, а не смертью задания, потому что исход разный. Мёртвое
        задание переводит звонок в `failed` — «сломалось»; исчерпанное окно
        ожидания означает «записи нет», и ING-04 требует именно этого.
        """
        if not job.is_last_attempt:
            raise StageError(
                JobErrorClass.NOT_FOUND, f"source has no recording yet for call {job.call_id}"
            ) from exc
        log.warning(
            "call=%s: no recording after %s attempt(s); marking it unavailable at source",
            job.call_id,
            job.attempts,
        )
        return IngestWork(outcome=IngestWork.NO_AUDIO, **dims)

    async def _fetch(
        self, job: ClaimedJob, deps: StageDeps, ref: CallRef, key: str, dims: dict[str, Any]
    ) -> IngestWork:
        """Скачать, измерить и сохранить запись через внедрённые порты; вернуть результат."""
        with deps.workspace.directory(prefix="speech-ingest-") as tmp:
            path = Path(tmp) / "recording.mp3"
            written = await self._download(deps, ref, path)
            probe = await self._probe(deps, path)
            await self._upload(deps, key, path)

        _warn_on_duration_mismatch(job, ref, probe.duration_s)
        return IngestWork(
            outcome=IngestWork.STORED,
            object_key=key,
            audio_bytes=written,
            channels=probe.channels,
            duration_s=round(probe.duration_s),
            **dims,
        )

    async def _download(self, deps: StageDeps, ref: CallRef, path: Path) -> int:
        """Скачать запись внедрённым адаптером и вернуть число байтов."""
        if deps.fetcher is None:
            raise RuntimeError("recording fetcher is not configured")
        return await deps.fetcher.download(ref, path)

    async def _probe(self, deps: StageDeps, path: Path) -> AudioProbe:
        """Измерить запись через медиапорт либо вернуть нормализованный отказ этапа."""
        try:
            if deps.media is None:
                raise RuntimeError("media processor is not configured")
            return await deps.media.probe(path)
        except MediaError as exc:
            raise StageError(
                JobErrorClass.INVALID_OUTPUT, f"the fetched file is not usable audio: {exc}"
            ) from exc

    async def _upload(self, deps: StageDeps, key: str, path: Path) -> None:
        """Передать файл порту хранилища либо вернуть нормализованный отказ этапа."""
        try:
            if deps.storage is None:
                raise RuntimeError("recording storage is not configured")
            await deps.storage.put_file(key, path)
        except StorageError as exc:
            raise StageError(
                JobErrorClass.PROVIDER_ERROR, f"could not store the recording under {key}"
            ) from exc

    async def _already_stored(self, deps: StageDeps, key: str) -> IngestWork | None:
        """Проверить, что объект и правда на месте.

        Проверяется хранилище, а не только колонка: `audio_status='stored'` при
        отсутствующем объекте — ровно тот случай, ради которого этап
        перезапускают, и поверить колонке на слово значит отправить в
        расшифровку звонок без записи.
        """
        if deps.storage is None:
            raise RuntimeError("recording storage is not configured")
        try:
            if not await deps.storage.exists(key):
                return None
        except StorageError as exc:
            raise StageError(
                JobErrorClass.PROVIDER_ERROR, f"could not check whether {key} is stored"
            ) from exc
        return IngestWork(outcome=IngestWork.STORED, object_key=key)

    async def _load_call(self, job: ClaimedJob, deps: StageDeps) -> CallForIngest:
        """Получить DTO записи и проекта через внедрённый репозиторий."""
        if deps.repository is None:
            raise RuntimeError("pipeline repository is not configured")
        call = await deps.repository.load_ingest(job.call_id)
        if call is None:
            raise StageError(JobErrorClass.NOT_FOUND, f"call {job.call_id} is gone")
        return call

    async def store(
        self, persistence: StagePersistence, job: ClaimedJob, work: IngestWork
    ) -> StageResult:
        """Сохранить результат приёма и выбрать следующий этап конвейера."""
        await persistence.save_ingest(job, work)
        if work.outcome == IngestWork.SKIPPED_SHORT:
            return StageResult(processing_status=ProcessingStatus.SKIPPED_SHORT, next_kind=None)
        if work.outcome == IngestWork.NO_AUDIO:
            return StageResult(processing_status=ProcessingStatus.SKIPPED_NO_AUDIO, next_kind=None)
        return StageResult(
            processing_status=ProcessingStatus.TRANSCRIBING, next_kind=JobKind.TRANSCRIBE
        )


def _call_ref(call: CallForIngest) -> CallRef:
    """Восстановить ссылку источника из строки звонка.

    Локатор лежит в снимке: колонки под него нет намеренно — он свойство
    источника, а не звонка, и при смене источника меняется целиком.
    """
    snapshot = call.source_snapshot or {}
    locator = snapshot.get("audio_locator")
    if not locator:
        raise StageError(
            JobErrorClass.NOT_FOUND,
            f"call {call.session_id} has no audio locator in its source snapshot",
        )
    started = call.started_at
    stated = int(call.duration_s or 0)
    return CallRef(
        session_id=str(call.session_id),
        audio_locator=str(locator),
        day=started.date(),
        started_at=started,
        # Ноль означает «источник не знал»; ссылка обязана нести это как `None`,
        # иначе сверка длительности сравнивала бы измерение с выдумкой.
        duration_s=stated or None,
        # По источнику воркер выбирает скачиватель (фича 14); ссылка хранилища
        # Телфина — из своей колонки, в снимке её нет.
        source=str(call.source or "ncc"),
        audio_url=call.source_audio_url,
    )


def _short_call_threshold(settings: PipelineSettings) -> int:
    """Порог короткого звонка.

    Из проверенного снимка `speech_settings` для текущего задания (ING-05).
    """
    return settings.short_call_threshold_s


def _warn_on_duration_mismatch(job: ClaimedJob, ref: CallRef, measured: float) -> None:
    """Зафиксировать существенное расхождение измеренной и заявленной длительности."""
    stated = ref.duration_s
    if not stated or measured <= 0:
        return
    if abs(measured - stated) / stated > DURATION_MISMATCH_RATIO:
        logger.warning(
            "call=%s: source says %ss, the file measures %.1fs — more than %.0f%% apart",
            job.call_id,
            stated,
            measured,
            DURATION_MISMATCH_RATIO * 100,
        )
