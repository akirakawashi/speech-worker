"""Контракты данных оценки, общие для сценариев и адаптеров."""

from src.domain.quality.evaluation import (
    Evaluation as Evaluation,
    ObjectionVerdict as ObjectionVerdict,
    StageVerdict as StageVerdict,
    TranscriptLine as TranscriptLine,
)
from src.domain.quality.script import (
    CURRENT_SCRIPT as CURRENT_SCRIPT,
    OTHER_OBJECTION as OTHER_OBJECTION,
    UNKNOWN_OUTCOME as UNKNOWN_OUTCOME,
    CallScript as CallScript,
    ScriptObjection as ScriptObjection,
    ScriptStage as ScriptStage,
)
