"""Сроки хранения завершённых заданий и журнала доступа к записям речи.

Возраст считаем от колонки завершения; строки с пустой отметкой не удаляем.
Срок хранения оригинального аудио задаётся отдельно настройками конвейера.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class RetentionPolicy(BaseModel):
    """Проверить срок хранения и колонку завершения; вернуть неизменяемое правило."""

    model_config = ConfigDict(frozen=True)

    table: str
    completion_column: str
    days: int = Field(ge=1)
    reason: str
    basis: Literal["storage", "correctness", "compliance"] = "storage"


RETENTION_POLICIES: tuple[RetentionPolicy, ...] = (
    RetentionPolicy(
        table="speech_audit_events",
        completion_column="occurred_at",
        days=365,
        basis="compliance",
        reason=(
            "who listened to a recording, who downloaded it, who exported a "
            "list. Same year as the main audit journal and for the "
            "same reason: shortening it destroys the record of access to "
            "personal data, which is the thing the journal exists to prove"
        ),
    ),
    RetentionPolicy(
        table="speech_jobs",
        completion_column="finished_at",
        days=30,
        reason=(
            "finished pipeline jobs. A job without a finish time is still "
            "queued, running or dead awaiting a human -- pruning by age would "
            "delete work that was never done"
        ),
    ),
)

POLICY_BY_TABLE: dict[str, RetentionPolicy] = {
    policy.table: policy for policy in RETENTION_POLICIES
}

# Порядок удаления только из общей с API базы речи.
SPEECH_DELETE_ORDER: tuple[str, ...] = (
    "speech_jobs",
    "speech_audit_events",
)
