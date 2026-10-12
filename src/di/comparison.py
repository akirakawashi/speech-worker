"""Сборка клиентов, выборок и сценариев сравнения моделей."""

import argparse
import asyncio
import sys
from datetime import date
from typing import Final

from src.application.comparison.results import summarize, summarize_reasons
from src.application.comparison.service import compare, compare_reasons
from src.application.dto.calls import CallSource
from src.application.errors.reasons import ReasonsNotConfigured
from src.di.pipeline import build_stage_deps
from src.di.reasons import build_reason_client
from src.domain.calls.period import period_bounds
from src.infrastructure.clients.analysis import SpeechAnalysisClient, build_analysis_client
from src.infrastructure.persistence.comparison import PostgresComparisonRepository
from src.infrastructure.reports.comparison import write_csv, write_reasons_csv
from src.setting.config import Settings

CANDIDATE_KEY_ENV: Final[str] = "SPEECH_DIFY_CANDIDATE_APP_KEY"
MAX_LIMIT: Final[int] = 1000


def build_candidate_client(settings: Settings) -> SpeechAnalysisClient:
    """Собрать клиент отдельного приложения-кандидата, отклонив боевой ключ."""
    base_url = str(getattr(settings, "speech_dify_base_url", "") or "").strip().rstrip("/")
    app_key = settings.speech_dify_candidate_app_key.strip()
    if not base_url or not app_key:
        raise SystemExit(f"the candidate needs SPEECH_DIFY_BASE_URL and {CANDIDATE_KEY_ENV}")
    production = {
        str(getattr(settings, name, "") or "").strip()
        for name in ("speech_dify_app_key", "speech_dify_reason_app_key")
    }
    if app_key in production:
        # Кандидат с ключом боевого приложения сравнил бы модель саму с собой и
        # показал бы полное согласие — без единого слова о том, что сверки не было.
        raise SystemExit(f"{CANDIDATE_KEY_ENV} is a production app key, not a candidate")
    return SpeechAnalysisClient(
        base_url=base_url,
        app_key=app_key,
        timeout_seconds=float(getattr(settings, "speech_dify_timeout_seconds", 120.0)),
    )


def run_comparison_command(args: argparse.Namespace) -> int:
    """Собрать модели и выборку, выполнить сравнение и вывести результат."""
    first, last = date.fromisoformat(args.first), date.fromisoformat(args.last)
    if last < first:
        raise SystemExit("--to is earlier than --from")
    if not 1 <= args.limit <= MAX_LIMIT:
        raise SystemExit(f"--limit must be between 1 and {MAX_LIMIT}")

    from src.infrastructure.persistence.session import get_speech_ms_sessionmaker
    from src.setting.config import get_settings

    settings = get_settings()
    sessionmaker = get_speech_ms_sessionmaker(settings)
    repository = PostgresComparisonRepository(sessionmaker)
    start, end = period_bounds(first, last, settings.speech_display_timezone)
    candidate = build_candidate_client(settings) if args.apply else None
    control: SpeechAnalysisClient | None = None
    if args.apply and args.control:
        if args.app == "reasons":
            try:
                control = build_reason_client(settings)
            except ReasonsNotConfigured as exc:
                raise SystemExit(f"control: {exc}") from None
        else:
            control = build_analysis_client(settings)

    # CSV в стандартный вывод — значит всё остальное в поток ошибок, иначе сводка
    # и ход прогона оказались бы внутри файла.
    stream = sys.stderr if args.output == "-" else sys.stdout

    def echo(line: str) -> None:
        """Вывести строку отчёта в выбранный поток консоли."""
        print(line, file=stream, flush=True)

    async def run_reasons() -> int:
        """Выполнить собранное сравнение причин и вывести его результат."""
        source = CallSource(args.source)
        calls = await repository.sample_reason_calls(
            date_from=start, date_to=end, source=source, limit=args.limit
        )
        echo(
            f"звонков с кодом причины в выборке: {len(calls)}"
            f" (период {first}…{last}, источник {source.value}, предел {args.limit})"
        )
        if candidate is None:
            configured = bool(str(getattr(settings, "speech_dify_reason_app_key", "") or ""))
            echo(f"ключ боевого приложения причин в контейнере: {'есть' if configured else 'нет'}")
            echo("это была проверка; чтобы спросить модели, добавьте --apply")
            return 0
        rows = await compare_reasons(
            repository,
            calls,
            candidate=candidate,
            control=control,
            concurrency=args.concurrency,
            echo=echo,
        )
        for line in summarize_reasons(rows):
            echo(line)
        if args.output:
            write_reasons_csv(args.output, rows)
            echo(f"по звонкам: {'стандартный вывод' if args.output == '-' else args.output}")
        return 0

    async def run() -> int:
        """Выполнить собранный сценарий и вернуть его результат."""
        calls = await repository.sample_calls(date_from=start, date_to=end, limit=args.limit)
        echo(f"звонков в выборке: {len(calls)} (период {first}…{last}, предел {args.limit})")
        if candidate is None:
            echo("это была проверка; чтобы спросить модели, добавьте --apply")
            return 0
        rows = await compare(
            calls,
            candidate_deps=build_stage_deps(sessionmaker=sessionmaker, analysis=candidate),
            control_deps=build_stage_deps(sessionmaker=sessionmaker, analysis=control)
            if control is not None
            else None,
            concurrency=args.concurrency,
            echo=echo,
        )
        paths = await repository.paths()
        for line in summarize(rows, paths):
            echo(line)
        if args.output:
            write_csv(args.output, rows, paths)
            where = "стандартный вывод" if args.output == "-" else args.output
            echo(f"по звонкам: {where}")
        return 0

    return asyncio.run(run_reasons() if args.app == "reasons" else run())
