"""Очередь заданий конвейера.

Реализация одна — на Postgres. Порт существует не ради симметрии: в ADR прямо
записано, что Celery с Redis могут быть внедрены позже, и тогда меняется
адаптер, а не обработчики этапов.

Протокол целиком описан в
`aicistent-documentation/speech-analytics/03-adr-postgres-job-queue.md`.
Расхождение кода с этим документом — дефект кода, а не документа.
"""

from src.application.ports.queue import (
    ClaimedJob,
    ExpiredJob,
    JobQueue,
    LaneDepth,
    NewJob,
    QueueSnapshot,
)
from src.infrastructure.queue.postgres import PostgresJobQueue

__all__ = [
    "ClaimedJob",
    "ExpiredJob",
    "JobQueue",
    "LaneDepth",
    "NewJob",
    "PostgresJobQueue",
    "QueueSnapshot",
]
