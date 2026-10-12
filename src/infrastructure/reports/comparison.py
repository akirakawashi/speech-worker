"""Вывод CSV сравнения моделей с BOM без текста разговора."""

import csv
import sys
from collections.abc import Callable, Sequence
from typing import Any, TextIO

from src.application.dto.classifier import NodePaths, level_match, path_caption
from src.application.dto.comparison import Outcome, ReasonOutcome, ReasonRow, Row


def _emit_csv(path: str, write: Callable[[TextIO], None]) -> None:
    """CSV в файл или, при `-`, в стандартный вывод — с меткой порядка байтов."""
    if path == "-":
        # Метка порядка байтов первой строкой — та же, что даёт `utf-8-sig` файлу.
        sys.stdout.write(chr(0xFEFF))
        write(sys.stdout)
        sys.stdout.flush()
        return
    # `utf-8-sig`: без метки порядка байтов Excel открывает кириллицу кракозябрами.
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        write(handle)


def _fresh_cells(outcome: Outcome | None, operator: Sequence[str]) -> list[Any]:
    """Подготовить ячейки результата модели без текста разговора."""
    if outcome is None:
        return ["", "", "", "", "", ""]
    work = outcome.work
    if work is None:
        return ["", "", "", "", f"{outcome.seconds:.1f}", outcome.error or ""]
    return [
        path_caption(work.path),
        sum(level_match(operator, work.path)),
        work.sentiment.value if work.sentiment else "",
        work.call_reason_code or "",
        f"{outcome.seconds:.1f}",
        "",
    ]


def _write_rows(handle: TextIO, rows: Sequence[Row], paths: NodePaths) -> None:
    """Записать строки сравнения в CSV без текста разговора."""
    writer = csv.writer(handle)
    variants = ("кандидат", "контроль")
    writer.writerow(
        [
            "call_id",
            "путь оператора",
            "сохранённый путь",
            "сохранённый: уровней совпало",
            "сохранённая тональность",
            "сохранённая причина",
            *(
                f"{name}: {column}"
                for name in variants
                for column in (
                    "путь",
                    "уровней совпало",
                    "тональность",
                    "причина",
                    "секунд",
                    "ошибка",
                )
            ),
        ]
    )
    for row in rows:
        operator = paths.labels(row.call.operator_node_id)
        stored = paths.labels(row.call.ai_node_id)
        writer.writerow(
            [
                str(row.call.call_id),
                path_caption(operator),
                path_caption(stored),
                sum(level_match(operator, stored)),
                row.call.sentiment or "",
                row.call.reason_code or "",
                *_fresh_cells(row.candidate, operator),
                *_fresh_cells(row.control, operator),
            ]
        )


def write_csv(path: str, rows: Sequence[Row], paths: NodePaths) -> None:
    """По звонку — пути, тональность, коды, время. Текста разговора и резюме нет.

    `-` — в стандартный вывод. Так CSV забирается из контейнера перенаправлением
    `> файл` на узле: `docker cp` файлов на tmpfs не видит, а `/tmp` у воркеров
    речи — tmpfs (проверено на узле 2026-09-29: «Could not find the file»).
    """
    _emit_csv(path, lambda handle: _write_rows(handle, rows, paths))


def write_reasons_csv(path: str, rows: Sequence[ReasonRow]) -> None:
    """По звонку — коды, время и итог. Текста причины и разговора нет."""

    def cells(outcome: ReasonOutcome | None) -> list[str]:
        """Подготовить ячейки кода причины, времени и результата модели."""
        if outcome is None:
            return ["", "", ""]
        return [outcome.code or "", f"{outcome.seconds:.1f}", outcome.status]

    def write(handle: TextIO) -> None:
        """Записать подготовленные строки отчёта в переданный поток."""
        writer = csv.writer(handle)
        writer.writerow(
            [
                "call_id",
                "сохранённый код",
                "кандидат: код",
                "кандидат: секунд",
                "кандидат: итог",
                "контроль: код",
                "контроль: секунд",
                "контроль: итог",
            ]
        )
        for row in rows:
            writer.writerow(
                [str(row.call_id), row.stored, *cells(row.candidate), *cells(row.control)]
            )

    _emit_csv(path, write)
