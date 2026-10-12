"""Последовательное выполнение готовых модельных шагов одного задания."""

from uuid import UUID

from src.application.dto.calls import JobErrorClass
from src.application.dto.quality_execution import (
    MAX_QUALITY_COMPLETIONS,
    QualityStepRequest,
    QualityWork,
)
from src.application.errors.quality import QualityModelError
from src.application.pipeline.stages import StageError
from src.application.ports.quality_execution import QualityCompletion, QualitySteps


class QualityExecutionService:
    """Спросить бэкенд о следующем шаге, вызвать модель и вернуть её ответ бэкенду."""

    def __init__(self, steps: QualitySteps, model: QualityCompletion) -> None:
        """Принять подготовку шагов и HTTP-выполнение; бизнес-правил оценки здесь нет."""
        self._steps = steps
        self._model = model

    async def evaluate(self, call_id: UUID, *, attempts: int, max_attempts: int) -> QualityWork:
        """Выполнить шаги без транзакции, проверяя неизменность снимка и параметров.

        Сетевой отказ отдаётся очереди для повтора. На последней попытке
        бэкенд готовит итоговый отказ, который сохраняется атомарно с заданием.
        """
        command = QualityStepRequest(call_id=call_id)
        metadata: tuple[str, str] | None = None
        for _ in range(MAX_QUALITY_COMPLETIONS + 1):
            step = await self._steps.prepare(command)
            if command.snapshot is not None and step.snapshot != command.snapshot:
                raise StageError(JobErrorClass.INVALID_OUTPUT, "quality snapshot mismatch")
            if step.result is not None:
                actual = (step.result.model_name, step.result.script_version)
                if metadata is not None and actual != metadata:
                    raise StageError(JobErrorClass.PROVIDER_ERROR, "quality configuration changed")
                return step.result
            request = step.request
            assert request is not None
            actual = (request.model, request.prompt_version)
            if metadata is not None and actual != metadata:
                raise StageError(JobErrorClass.PROVIDER_ERROR, "quality configuration changed")
            metadata = actual
            command = command.model_copy(update={"snapshot": step.snapshot})
            try:
                response = await self._model.complete(request)
            except QualityModelError as exc:
                if exc.retryable and attempts < max_attempts:
                    error = (
                        JobErrorClass.TIMEOUT
                        if exc.reason == "timeout"
                        else JobErrorClass.PROVIDER_ERROR
                    )
                    raise StageError(error, "quality model request failed") from exc
                command = command.model_copy(update={"model_error": True})
            else:
                command = command.model_copy(update={"responses": (*command.responses, response)})
        raise StageError(JobErrorClass.INVALID_OUTPUT, "quality completion limit exceeded")
