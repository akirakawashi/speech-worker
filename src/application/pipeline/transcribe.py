"""Этап `transcribe`: запись превращается в реплики с ролями.

Один проход Whisper, а не два. Роли берутся не из модели и не из диаризации, а
из **энергии каналов**: записи NCC — стерео с двумя ногами звонка, и разделение
между каналами измерено на всех 61 записи выгрузки (медиана 44 дБ, уверенно
относится 98,7% окон речи). Второй проход по каналам дал бы ту же атрибуцию за
двойное время GPU.

Оператор — канал 0. Не догадка: первые 15 секунд каждого канала распознаны
отдельно на десяти записях, и в десяти из десяти канал 0 произносит
операторское приветствие. Значение живёт в настройке, потому что запись с
другой микшировкой обязана чиниться настройкой, а не правкой кода.

Атрибуция может не получиться — говорят одновременно, запись оказалась моно, —
и тогда реплика получает `unknown`. Это честное состояние, а не заглушка:
приписать её оператору значило бы испортить статистику разговоров тем сильнее,
чем хуже запись.
"""

from __future__ import annotations

import logging
from pathlib import Path

from src.application.dto.media import ChannelEnvelope
from src.application.dto.pipeline import (
    Reply as Reply,
    TranscribeWork as TranscribeWork,
)
from src.application.dto.pipeline_input import CallForTranscription
from src.application.dto.transcription import (
    TranscriptionError,
    TranscriptionResult,
    TranscriptionSegment,
)
from src.application.errors.external import MediaError, StorageError, WhisperNotConfigured
from src.application.pipeline.stages import StageDeps, StageError, StageHandler, StageResult
from src.application.ports.pipeline import StagePersistence
from src.application.ports.queue import ClaimedJob
from src.domain.calls.contracts import (
    AudioStatus,
    JobErrorClass,
    JobKind,
    ProcessingStatus,
    RoleMethod,
    Speaker,
)

logger = logging.getLogger(__name__)

#: Реплика короче этого в атрибуции по энергии не участвует — окно огибающей
#: 100 мс, и на двух окнах разница каналов ещё не измерение, а шум.
MIN_ATTRIBUTABLE_S = 0.3


#: Транскрипт переписывается целиком. Реплики уезжают каскадом вместе со старым
#: транскриптом: дописывание к прошлому результату дало бы удвоенный разговор,
#: а `ON CONFLICT DO UPDATE` оставило бы реплики от предыдущего прогона.


class TranscribeHandler(StageHandler[TranscribeWork]):
    """Распознать запись и разложить реплики по собеседникам."""

    kind = JobKind.TRANSCRIBE

    def slot_limit(self, deps: StageDeps) -> int | None:
        """Whisper на стенде один, и воркеров у него больше, чем он держит.

        Замер SP-003: один поток даёт 23,8× реального времени, два — 24,5×,
        три — 19,8×. То есть параллелизм упирается в тот же GPU, и третий поток
        уже вредит. Ограничение внешнее, поэтому и считается снаружи процесса —
        advisory-семафором в базе (§8 ADR очереди).
        """
        return deps.runtime_settings.transcribe_concurrency

    async def work(self, job: ClaimedJob, deps: StageDeps) -> TranscribeWork:
        """Загрузить снимок и выполнить долгую фазу этапа через порты; вернуть DTO для записи."""
        call = await self._load_call(job, deps)
        key = call.audio_object_key
        if call.audio_status != AudioStatus.STORED.value or not key:
            # Записи нет — распознавать нечего, и повтор её не вернёт.
            raise StageError(
                JobErrorClass.NOT_FOUND,
                f"call {job.call_id} has no stored audio ({call.audio_status})",
            )

        with deps.workspace.directory(prefix="speech-transcribe-") as tmp:
            source = await self._fetch(deps, str(key), Path(tmp) / "call.mp3")
            mono = await self._downmix(deps, source, Path(tmp) / "mono.wav")
            result = await self._recognise(deps, mono)
            envelope = await self._envelope(deps, source, call)

        segments = self._drop_past_the_end(result.segments, envelope, call, job)
        replies, method = self._attribute(segments, envelope, deps)
        logger.getChild("work").info(
            "call=%s transcribed: %s reply(ies), roles by %s",
            job.call_id,
            len(replies),
            method.value,
        )
        return TranscribeWork(
            engine=result.provider_kind,
            model_name=result.model,
            language=result.language or "ru",
            role_method=method,
            duration_s=result.duration_seconds,
            replies=replies,
        )

    # ------------------------------------------------------------------ #
    # Долгая фаза по шагам
    # ------------------------------------------------------------------ #

    async def _fetch(self, deps: StageDeps, key: str, path: Path) -> Path:
        """Скачать запись из **нашего** хранилища, а не из источника.

        К этому моменту она там лежит (этап `ingest`), и повторное обращение к
        NCC было бы лишней зависимостью от чужой доступности в самом длинном
        этапе конвейера.
        """
        try:
            if deps.storage is None:
                raise RuntimeError("recording storage is not configured")
            await deps.storage.fetch(key, path)
        except StorageError as exc:
            error_class = {
                "not_found": JobErrorClass.NOT_FOUND,
                "timeout": JobErrorClass.TIMEOUT,
            }.get(exc.reason, JobErrorClass.PROVIDER_ERROR)
            raise StageError(error_class, "stored audio is not readable") from exc
        except OSError as exc:
            raise StageError(JobErrorClass.UNEXPECTED_ERROR, f"could not write {path}") from exc
        if path.stat().st_size == 0:
            raise StageError(JobErrorClass.NOT_FOUND, f"stored audio {key} is empty")
        return path

    async def _downmix(self, deps: StageDeps, source: Path, dst: Path) -> Path:
        """Подготовить моно-файл через внедрённый медиапорт; вернуть путь результата."""
        try:
            if deps.media is None:
                raise RuntimeError("media processor is not configured")
            return await deps.media.downmix(source, dst)
        except MediaError as exc:
            raise StageError(
                JobErrorClass.INVALID_OUTPUT, f"stored audio cannot be prepared: {exc}"
            ) from exc

    async def _recognise(self, deps: StageDeps, mono: Path) -> TranscriptionResult:
        """Распознать подготовленный файл через порт; вернуть DTO либо отказ этапа."""
        if deps.transcription is None:
            raise StageError(JobErrorClass.PROVIDER_ERROR, "the worker has no transcription client")
        try:
            result = await deps.transcription.transcribe(mono)
        except WhisperNotConfigured as exc:
            # Ошибка развёртывания, а не провайдера: повторять нечего, пока не
            # починят настройку.
            raise StageError(JobErrorClass.INVALID_OUTPUT, str(exc)) from exc
        except TranscriptionError as exc:
            # `reason` — нормализованный класс модуля распознавания, тела ответа
            # в нём нет (BR-13).
            error_class = (
                JobErrorClass.TIMEOUT if exc.reason == "timeout" else JobErrorClass.PROVIDER_ERROR
            )
            raise StageError(error_class, f"whisper refused the call: {exc.reason}") from exc
        if not result.segments:
            # Пустая расшифровка — не «тихий разговор»: сегментов не бывает
            # ноль у записи, которая прошла проверку на пустоту при скачивании.
            raise StageError(
                JobErrorClass.INVALID_OUTPUT, "whisper returned no segments for a non-empty file"
            )
        return result

    async def _envelope(
        self, deps: StageDeps, source: Path, call: CallForTranscription
    ) -> ChannelEnvelope | None:
        """Огибающая по каналам исходной записи. `None`, если запись моно.

        Считается по **исходнику**, а не по даунмиксу: в даунмиксе каналы уже
        сведены, и разделять там нечего.
        """
        if int(call.audio_channels or 0) < 2:
            return None
        try:
            if deps.media is None:
                raise RuntimeError("media processor is not configured")
            return await deps.media.envelope(source)
        except MediaError:
            # Не отказ этапа: без огибающей расшифровка всё равно верна, теряются
            # только роли. Терять из-за них разговор нельзя.
            logger.getChild("work").warning(
                "channel envelope failed for call %s; replies will be unattributed",
                call.session_id,
            )
            return None

    # ------------------------------------------------------------------ #
    # Роли
    # ------------------------------------------------------------------ #

    def _drop_past_the_end(
        self,
        segments: tuple[TranscriptionSegment, ...],
        envelope: ChannelEnvelope | None,
        call: CallForTranscription,
        job: ClaimedJob,
    ) -> tuple[TranscriptionSegment, ...]:
        """Выбросить реплики, начинающиеся позже, чем кончается запись.

        Whisper разбирает звук окнами по тридцать секунд и дополняет последнее
        окно тишиной. На этой тишине он выдумывает текст и выдаёт его
        сегментами с метками за концом файла.

        Замер на корпусе из шестидесяти одного разговора: 119 реплик из 2919
        (4%) начинались позже конца записи. Тексты узнаваемы — «Спасибо.»,
        «Продолжение следует.», «я не знаю,», «.». Ни одной из этих фраз никто
        не произносил.

        Выбрасывать, а не помечать. Помеченная реплика остаётся в поиске:
        человек ищет «продолжение следует» и находит разговоры, где такого не
        говорили. А роли у неё не будет никогда — огибающая на несуществующем
        отрезке показывает тишину в обоих каналах, и 116 из 241 реплики без
        роли оказались именно этими фантомами.

        Длительность берётся **измеренная**, а не та, что вернул Whisper: он
        сообщает длину дополненного окна, то есть ровно ту границу, которую мы
        и не хотим признавать.
        """
        measured = self._measured_duration(envelope, call)
        if measured is None:
            # Длительности нет ниоткуда — отсекать не по чему. Молча пропустить
            # проверку хуже, чем сказать: без неё в расшифровку попадёт
            # выдуманный хвост, и обнаружится это в поиске.
            logger.getChild("work").warning(
                "call=%s: no measured duration, phantom replies cannot be cut off",
                job.call_id,
            )
            return segments

        kept = tuple(segment for segment in segments if segment.start_s < measured)
        dropped = len(segments) - len(kept)
        if dropped:
            logger.getChild("work").info(
                "call=%s: %s reply(ies) start after the recording ends (%.1f s) and were dropped",
                job.call_id,
                dropped,
                measured,
            )
        return kept

    @staticmethod
    def _measured_duration(
        envelope: ChannelEnvelope | None, call: CallForTranscription
    ) -> float | None:
        """Сколько длится запись по измерению.

        Огибающая считается по настоящему файлу и потому точнее всего. Если её
        нет — берётся длительность, измеренная `ffprobe` при приёме. Оба
        источника наши; ответ Whisper здесь не годится по построению.
        """
        if envelope is not None and envelope.duration_s > 0:
            return envelope.duration_s
        stored = call.duration_s
        if isinstance(stored, (int, float)) and stored > 0:
            return float(stored)
        return None

    def _attribute(
        self,
        segments: tuple[TranscriptionSegment, ...],
        envelope: ChannelEnvelope | None,
        deps: StageDeps,
    ) -> tuple[tuple[Reply, ...], RoleMethod]:
        """Определить роли реплик по энергии каналов и вернуть метод атрибуции."""
        if envelope is None or envelope.channels < 2:
            replies = tuple(
                Reply(
                    seq=index,
                    speaker=Speaker.UNKNOWN,
                    start_ms=round(segment.start_s * 1000),
                    end_ms=round(max(segment.start_s, segment.end_s) * 1000),
                    text=segment.text,
                )
                for index, segment in enumerate(segments)
            )
            # `llm_attribution` — то, чем роли получат **позже**, на этапе
            # анализа. Записать его здесь значило бы утверждать сделанное.
            return replies, RoleMethod.NONE

        operator_channel = _operator_channel(deps)
        replies = tuple(
            Reply(
                seq=index,
                speaker=self._speaker(segment, envelope, operator_channel),
                start_ms=round(segment.start_s * 1000),
                end_ms=round(max(segment.start_s, segment.end_s) * 1000),
                text=segment.text,
            )
            for index, segment in enumerate(segments)
        )
        return replies, RoleMethod.STEREO_ENERGY

    @staticmethod
    def _speaker(
        segment: TranscriptionSegment, envelope: ChannelEnvelope, operator_channel: int
    ) -> Speaker:
        """Преобразовать роль реплики в подпись участника разговора."""
        end = max(segment.end_s, segment.start_s + MIN_ATTRIBUTABLE_S)
        channel = envelope.dominant_channel(segment.start_s, end)
        if channel is None:
            return Speaker.UNKNOWN
        return Speaker.OPERATOR if channel == operator_channel else Speaker.CLIENT

    # ------------------------------------------------------------------ #
    # Короткая фаза
    # ------------------------------------------------------------------ #

    async def _load_call(self, job: ClaimedJob, deps: StageDeps) -> CallForTranscription:
        """Получить типизированные метаданные записи через внедрённый репозиторий."""
        if deps.repository is None:
            raise RuntimeError("pipeline repository is not configured")
        call = await deps.repository.load_transcription(job.call_id)
        if call is None:
            raise StageError(JobErrorClass.NOT_FOUND, f"call {job.call_id} is gone")
        return call

    async def store(
        self, persistence: StagePersistence, job: ClaimedJob, work: TranscribeWork
    ) -> StageResult:
        """Записать реплики через порт и передать звонок на разбор без обезличивания."""
        await persistence.save_transcription(job, work)
        return StageResult(processing_status=ProcessingStatus.ANALYZING, next_kind=JobKind.ANALYZE)


def _operator_channel(deps: StageDeps) -> int:
    """Вернуть номер операторского канала из проверенных настроек этапа."""
    return deps.runtime_settings.stereo_operator_channel
