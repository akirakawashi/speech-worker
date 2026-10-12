"""Offline regressions for safe failures, storage classification and Whisper I/O.

All text and credentials below are synthetic. Real HTTP transports and database
connections are forbidden even if the developer has live services configured.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import traceback
from collections.abc import AsyncIterator
from email import policy
from email.parser import BytesParser
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, call
from uuid import UUID

import httpx
import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.exc import StatementError
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.dto.transcription import TranscriptionRequest
from src.application.pipeline.stages import (
    StageError,
    StageHandler,
    StageRegistry,
    StageResult,
)
from src.application.pipeline.transcribe import TranscribeHandler
from src.application.ports.queue import ClaimedJob, ExpiredJob
from src.di.pipeline import build_stage_deps
from src.domain.calls.contracts import (
    JobErrorClass,
    JobKind,
    JobLane,
    JobStatus,
    ProcessingStatus,
    is_terminal_error,
)
from src.infrastructure.clients import whisper as whisper_module
from src.infrastructure.persistence import session as db_session
from src.infrastructure.queue import worker as worker_module
from src.infrastructure.queue.postgres import PostgresJobQueue
from src.infrastructure.storage.recordings import SpeechAudioStorage, StorageConfig, StorageError
from src.infrastructure.transcription.providers import OpenAICompatibleAudioProvider

RAW_TEXT = "SYNTHETIC_RAW_TRANSCRIPT_DO_NOT_LOG_68419"
PROVIDER_BODY = "SYNTHETIC_PRIVATE_PROVIDER_BODY_73912"
JOB = ClaimedJob(
    id=UUID("11111111-2222-3333-4444-555555555555"),
    call_id=UUID("66666666-7777-8888-9999-aaaaaaaaaaaa"),
    kind=JobKind.TRANSCRIBE,
    lane=JobLane.DAILY,
    attempts=1,
    max_attempts=3,
    payload={},
    lease_seconds=900,
)
STORAGE_CONFIG = StorageConfig(
    base_url="https://storage.invalid",
    bucket="offline-recordings",
    region="us-east-1",
    access_key_id="synthetic-access-key",
    secret_key="synthetic-secret-key",
)
OBJECT_KEY = "calls/2026/09/10/synthetic-call.mp3"
LOG = logging.getLogger("src.infrastructure.queue.worker.transcribe")


@pytest.fixture(autouse=True)
def forbid_live_io(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_args, **_kwargs):
        pytest.fail("offline regression attempted a real HTTP or database connection")

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", forbidden)
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", forbidden)
    monkeypatch.setattr(Engine, "raw_connection", forbidden)


@pytest.fixture()
def worker_env() -> SimpleNamespace:
    session = MagicMock(spec=AsyncSession)
    session.__aenter__.return_value = session
    transaction = MagicMock()
    transaction.__aexit__.return_value = False
    session.begin.return_value = transaction
    queue = MagicMock(spec=PostgresJobQueue)
    queue.fail.return_value = JobStatus.QUEUED
    queue.complete.return_value = True
    registry = StageRegistry()
    for kind in JobKind:
        handler = MagicMock(spec=StageHandler)
        handler.kind = kind
        registry.register(handler)
    deps = build_stage_deps(settings=SimpleNamespace(), sessionmaker=Mock(return_value=session))
    worker = worker_module.Worker(
        queue, registry, deps, lanes=[JobLane.DAILY], worker_id="offline-regression-worker"
    )
    return SimpleNamespace(
        worker=worker,
        queue=queue,
        session=session,
        transaction=transaction,
        handler=registry.get(JOB.kind),
    )


def _private_error(kind: str, *, explicit_chain: bool = True) -> Exception:
    """Include actual traceback frames, SQL parameters and a private cause."""
    try:
        try:
            raise RuntimeError(PROVIDER_BODY)
        except RuntimeError as cause:
            raise StatementError(
                "synthetic database write failed",
                "INSERT INTO speech_transcript_segments (text_raw) VALUES (:text_raw)",
                {"text_raw": RAW_TEXT},
                cause,
                hide_parameters=False,
            ) from cause
    except StatementError as statement:
        if kind == "statement":
            return statement
        error = (
            StageError(JobErrorClass.PROVIDER_ERROR, RAW_TEXT)
            if kind == "stage"
            else RuntimeError(RAW_TEXT)
        )
        try:
            if explicit_chain:
                raise error from statement
            raise error
        except (StageError, RuntimeError) as chained:
            return chained


def _assert_private_error_is_a_real_leak_probe(error: Exception) -> None:
    rendered = "".join(traceback.format_exception(error))
    assert RAW_TEXT in rendered
    assert PROVIDER_BODY in rendered
    assert "[parameters:" in rendered


def _safe_worker_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    records = [r for r in caplog.records if r.name.startswith("src.infrastructure.queue.worker")]
    assert records, "silencing every diagnostic is not a logging fix"
    for record in records:
        assert record.exc_info is None, "structured loggers must not receive a traceback"
        assert record.exc_text is None
        assert record.stack_info is None
        for marker in (RAW_TEXT, PROVIDER_BODY):
            assert marker not in record.getMessage()
            assert marker not in repr(record.__dict__), "log arguments/extras must also be safe"
    assert RAW_TEXT not in caplog.text
    assert PROVIDER_BODY not in caplog.text
    assert "Traceback (most recent call last)" not in caplog.text
    return records


def _bounded_worker_sleeps(monkeypatch: pytest.MonkeyPatch, iterations: int) -> AsyncMock:
    """No wall-clock waits; a broken loop must fail rather than hang the suite."""
    sleep = AsyncMock(side_effect=[None] * iterations + [asyncio.CancelledError()])
    # Replace the module binding, not asyncio.sleep shared by the test runner.
    monkeypatch.setattr(
        worker_module,
        "asyncio",
        SimpleNamespace(sleep=sleep, CancelledError=asyncio.CancelledError),
    )
    return sleep


@pytest.mark.parametrize("phase", ["work", "store"])
@pytest.mark.parametrize("kind", ["stage", "statement", "generic"])
@pytest.mark.parametrize("explicit_chain", [True, False], ids=["cause", "context"])
async def test_worker_failure_keeps_class_and_ids_without_private_logs(
    worker_env, caplog, phase: str, kind: str, explicit_chain: bool
) -> None:
    error = _private_error(kind, explicit_chain=explicit_chain)
    _assert_private_error_is_a_real_leak_probe(error)
    caplog.set_level(logging.INFO, logger="src.infrastructure.queue.worker")
    expected = JobErrorClass.PROVIDER_ERROR if kind == "stage" else JobErrorClass.UNEXPECTED_ERROR

    async def fail_after_transaction(*_args):
        if phase == "store":
            assert worker_env.transaction.__aexit__.await_count == 1
            assert worker_env.session.__aexit__.await_count == 1
        return JobStatus.QUEUED

    worker_env.queue.fail.side_effect = fail_after_transaction
    if phase == "work":
        worker_env.handler.work.side_effect = error
        result = await worker_env.worker._work(JOB, worker_env.handler, LOG)
        assert result is worker_module._FAILED
        worker_env.handler.work.assert_awaited_once_with(JOB, worker_env.worker._deps)
        worker_env.handler.store.assert_not_awaited()
        worker_env.worker._deps.sessionmaker.assert_not_called()
    else:
        worker_env.handler.store.side_effect = error
        await worker_env.worker._store(JOB, worker_env.handler, "synthetic-work", LOG)
        persistence = worker_env.handler.store.await_args.args[0]
        assert persistence._session is worker_env.session
        worker_env.handler.store.assert_awaited_once_with(persistence, JOB, "synthetic-work")
        assert worker_env.transaction.__aexit__.await_args.args[1] is error
        worker_env.handler.work.assert_not_awaited()

    worker_env.queue.fail.assert_awaited_once_with(JOB.id, worker_env.worker.worker_id, expected)
    worker_env.queue.complete.assert_not_awaited()
    worker_env.queue.enqueue.assert_not_awaited()
    records = _safe_worker_records(caplog)
    assert len(records) == 1
    record = records[0]
    assert record.levelno == (logging.WARNING if kind == "stage" else logging.ERROR)
    assert f"job={JOB.id}" in record.getMessage()
    assert f"call={JOB.call_id}" in record.getMessage()
    if kind == "stage":
        assert f"class={expected.value}" in record.getMessage()
        assert f"attempt={JOB.attempts}" in record.getMessage()
    else:
        assert f"type={type(error).__name__}" in record.getMessage()


@pytest.mark.parametrize("kind", ["stage", "statement", "generic"])
async def test_store_commit_failure_is_caught_and_never_logged_as_success(
    worker_env, caplog, kind: str
) -> None:
    error = _private_error(kind)
    caplog.set_level(logging.INFO, logger="src.infrastructure.queue.worker")
    worker_env.handler.store.return_value = StageResult(processing_status=ProcessingStatus.DONE)
    worker_env.transaction.__aexit__.side_effect = error

    await worker_env.worker._store(JOB, worker_env.handler, "synthetic-work", LOG)

    worker_env.queue.complete.assert_awaited_once_with(
        worker_env.session, JOB.id, worker_env.worker.worker_id
    )
    worker_env.session.execute.assert_awaited_once()
    worker_env.transaction.__aexit__.assert_awaited_once_with(None, None, None)
    expected = JobErrorClass.PROVIDER_ERROR if kind == "stage" else JobErrorClass.UNEXPECTED_ERROR
    worker_env.queue.fail.assert_awaited_once_with(JOB.id, worker_env.worker.worker_id, expected)
    records = _safe_worker_records(caplog)
    assert len(records) == 1
    assert f"job={JOB.id}" in records[0].getMessage()
    assert f"call={JOB.call_id}" in records[0].getMessage()
    assert "stage done" not in caplog.text


async def test_heartbeat_errors_are_private_and_success_resets_failure_budget(
    worker_env, monkeypatch, caplog
) -> None:
    error = _private_error("statement")
    failures = worker_module.HEARTBEAT_MAX_FAILURES - 1
    outcomes = [error] * failures + [True] + [error] * failures + [False]
    worker_env.queue.heartbeat.side_effect = outcomes
    sleep = _bounded_worker_sleeps(monkeypatch, len(outcomes))
    caplog.set_level(logging.INFO, logger="src.infrastructure.queue.worker")

    await worker_env.worker._heartbeat_loop(JOB)

    assert worker_env.queue.heartbeat.await_args_list == [
        call(JOB.id, worker_env.worker.worker_id)
    ] * len(outcomes)
    assert sleep.await_count == len(outcomes)
    records = _safe_worker_records(caplog)
    failed = [r for r in records if "heartbeat failed" in r.getMessage()]
    assert len(failed) == 2 * failures
    for record, count in zip(failed, list(range(1, failures + 1)) * 2, strict=True):
        assert record.levelno == logging.WARNING
        counter = f"({count}/{worker_module.HEARTBEAT_MAX_FAILURES} consecutive)"
        assert counter in record.getMessage()
        assert f"job={JOB.id}" in record.getMessage()
        assert "type=StatementError" in record.getMessage()
    assert "lost the lease" in records[-1].getMessage()
    assert not any(r.levelno >= logging.ERROR for r in records)


async def test_heartbeat_stops_after_repeated_errors_without_leaking_them(
    worker_env, monkeypatch, caplog
) -> None:
    limit = worker_module.HEARTBEAT_MAX_FAILURES
    worker_env.queue.heartbeat.side_effect = [_private_error("generic")] * limit
    _bounded_worker_sleeps(monkeypatch, limit)
    caplog.set_level(logging.INFO, logger="src.infrastructure.queue.worker")

    await worker_env.worker._heartbeat_loop(JOB)

    assert worker_env.queue.heartbeat.await_count == limit
    records = _safe_worker_records(caplog)
    assert len(records) == limit + 1
    assert all("type=RuntimeError" in r.getMessage() for r in records[:-1])
    assert records[-1].levelno == logging.ERROR
    assert "giving up on the lease" in records[-1].getMessage()
    assert f"job={JOB.id}" in records[-1].getMessage()


async def test_finished_heartbeat_task_error_does_not_escape_cleanup(caplog) -> None:
    error = _private_error("statement")

    async def failed_heartbeat() -> None:
        raise error

    task = asyncio.create_task(failed_heartbeat())
    await asyncio.wait({task})
    assert task.done(), "exercise cancel() on an already failed task, not a running one"
    caplog.set_level(logging.INFO, logger="src.infrastructure.queue.worker")

    await worker_module._stop_heartbeat(task, JOB, LOG)

    assert task.exception() is error
    records = _safe_worker_records(caplog)
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
    assert f"job={JOB.id}" in records[0].getMessage()
    assert "type=StatementError" in records[0].getMessage()


async def test_reaper_and_dead_call_errors_are_private_and_do_not_stop_the_loop(
    worker_env, monkeypatch, caplog
) -> None:
    expired = ExpiredJob(id=JOB.id, call_id=JOB.call_id, kind=JOB.kind, status=JobStatus.DEAD)
    worker_env.queue.requeue_expired.side_effect = [
        _private_error("statement"),
        [expired],
        asyncio.CancelledError(),
    ]
    mark_dead = AsyncMock(side_effect=_private_error("generic"))
    monkeypatch.setattr(worker_env.worker, "_mark_call_dead", mark_dead)
    _bounded_worker_sleeps(monkeypatch, 3)
    caplog.set_level(logging.INFO, logger="src.infrastructure.queue.worker")

    with pytest.raises(asyncio.CancelledError):
        await worker_env.worker._reap_loop()

    assert worker_env.queue.requeue_expired.await_count == 3
    mark_dead.assert_awaited_once_with(JOB.call_id, JOB.kind)
    records = _safe_worker_records(caplog)
    assert len(records) == 3
    assert all(r.levelno == logging.ERROR for r in records)
    assert "reaper failed type=StatementError" in records[0].getMessage()
    assert f"job={JOB.id}" in records[1].getMessage()
    assert f"call={JOB.call_id}" in records[1].getMessage()
    assert f"call={JOB.call_id}" in records[2].getMessage()
    assert "type=RuntimeError" in records[2].getMessage()


def test_storage_error_defaults_remain_backward_compatible() -> None:
    error = StorageError("synthetic failure")
    assert str(error) == "synthetic failure"
    assert error.reason == "provider_error"
    assert error.status_code is None
    classified = StorageError("synthetic missing object", reason="not_found", status_code=404)
    assert classified.reason == "not_found"
    assert classified.status_code == 404


@pytest.mark.parametrize(
    ("outcome", "reason", "error_class", "terminal"),
    [
        pytest.param(404, "not_found", JobErrorClass.NOT_FOUND, True, id="404-terminal"),
        pytest.param(403, "provider_error", JobErrorClass.PROVIDER_ERROR, False, id="403-retry"),
        pytest.param(429, "provider_error", JobErrorClass.PROVIDER_ERROR, False, id="429-retry"),
        pytest.param(503, "provider_error", JobErrorClass.PROVIDER_ERROR, False, id="503-retry"),
        pytest.param(500, "provider_error", JobErrorClass.PROVIDER_ERROR, False, id="500-retry"),
        pytest.param(httpx.ReadTimeout, "timeout", JobErrorClass.TIMEOUT, False, id="read-timeout"),
        pytest.param(
            httpx.ConnectTimeout, "timeout", JobErrorClass.TIMEOUT, False, id="connect-timeout"
        ),
        pytest.param(httpx.PoolTimeout, "timeout", JobErrorClass.TIMEOUT, False, id="pool-timeout"),
        pytest.param(
            httpx.ConnectError,
            "provider_error",
            JobErrorClass.PROVIDER_ERROR,
            False,
            id="connection-error",
        ),
    ],
)
async def test_actual_storage_fetch_preserves_failure_class_and_retry_policy(
    tmp_path, outcome, reason: str, error_class: JobErrorClass, terminal: bool
) -> None:
    requests: list[httpx.Request] = []
    responses: list[httpx.Response] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if not isinstance(outcome, int):
            raise outcome(PROVIDER_BODY, request=request)
        response = httpx.Response(outcome, text=PROVIDER_BODY)
        responses.append(response)
        return response

    path = tmp_path / "call.mp3"
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        storage = SpeechAudioStorage(STORAGE_CONFIG, client=client)
        deps = build_stage_deps(settings=None, sessionmaker=None, storage=storage)
        with pytest.raises(StageError) as raised:
            await TranscribeHandler()._fetch(deps, OBJECT_KEY, path)
        assert not client.is_closed, "storage must not close a caller-owned client"

    error = raised.value
    assert error.error_class is error_class
    assert is_terminal_error(JobKind.TRANSCRIBE, error.error_class) is terminal
    storage_error = error.__cause__
    assert isinstance(storage_error, StorageError)
    assert storage_error.reason == reason
    assert storage_error.status_code == (outcome if isinstance(outcome, int) else None)
    for public_error in (error, storage_error):
        assert PROVIDER_BODY not in str(public_error)
        assert OBJECT_KEY not in str(public_error)
    if not isinstance(outcome, int):
        assert isinstance(storage_error.__cause__, outcome)
    assert len(requests) == 1
    assert requests[0].method == "GET"
    assert requests[0].url.path == f"/{STORAGE_CONFIG.bucket}/{OBJECT_KEY}"
    assert requests[0].headers["Authorization"].startswith("AWS4-HMAC-SHA256 ")
    assert all(response.is_closed for response in responses)
    assert not path.exists(), "an error response body must never be saved as audio"


async def test_timeout_while_streaming_audio_is_also_retryable(tmp_path) -> None:
    class InterruptedAudio(httpx.AsyncByteStream):
        closed = False

        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield b"ID3-partial-audio"
            raise httpx.ReadTimeout(PROVIDER_BODY)

        async def aclose(self) -> None:
            self.closed = True

    stream = InterruptedAudio()
    transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=stream))
    async with httpx.AsyncClient(transport=transport) as client:
        deps = build_stage_deps(
            settings=None,
            sessionmaker=None,
            storage=SpeechAudioStorage(STORAGE_CONFIG, client=client),
        )
        with pytest.raises(StageError) as raised:
            await TranscribeHandler()._fetch(deps, OBJECT_KEY, tmp_path / "partial.mp3")

    assert stream.closed
    assert raised.value.error_class is JobErrorClass.TIMEOUT
    assert not is_terminal_error(JobKind.TRANSCRIBE, raised.value.error_class)
    assert isinstance(raised.value.__cause__, StorageError)
    assert raised.value.__cause__.reason == "timeout"
    assert raised.value.__cause__.status_code is None
    assert PROVIDER_BODY not in str(raised.value)


@pytest.mark.parametrize("status", [200, 206])
async def test_actual_storage_fetch_still_writes_successful_audio(tmp_path, status: int) -> None:
    audio = b"ID3\x00\xffsynthetic-audio"
    response = httpx.Response(status, content=audio)
    transport = httpx.MockTransport(lambda request: response)
    path = tmp_path / "success.mp3"
    async with httpx.AsyncClient(transport=transport) as client:
        deps = build_stage_deps(
            settings=None,
            sessionmaker=None,
            storage=SpeechAudioStorage(STORAGE_CONFIG, client=client),
        )
        assert await TranscribeHandler()._fetch(deps, OBJECT_KEY, path) == path
    assert response.is_closed
    assert path.read_bytes() == audio


async def test_speech_engine_hides_bound_parameters_without_connecting(monkeypatch) -> None:
    settings = SimpleNamespace(
        db_pool_size=1,
        db_max_overflow=0,
        db_pool_timeout_seconds=1,
        db_pool_recycle_seconds=60,
        db_write_trace_enabled=False,
    )
    # Bypass only the cache: run the real factory without replacing a cached
    # engine or leaving a synthetic one behind for another test.
    engine = db_session._speech_ms_engine.__wrapped__(
        "postgresql+asyncpg://synthetic:synthetic@database.invalid/offline_speech",
        settings.db_pool_size,
        settings.db_max_overflow,
        settings.db_pool_timeout_seconds,
        settings.db_pool_recycle_seconds,
        settings.db_write_trace_enabled,
    )
    try:
        assert engine.sync_engine.hide_parameters is True
        error = StatementError(
            "synthetic failure",
            "INSERT INTO speech_transcript_segments (text_raw) VALUES (:text_raw)",
            {"text_raw": RAW_TEXT},
            RuntimeError("synthetic driver failure"),
            hide_parameters=engine.sync_engine.hide_parameters,
        )
        assert error.params == {"text_raw": RAW_TEXT}, "redaction must not alter stored data"
        assert RAW_TEXT not in str(error)
        assert RAW_TEXT not in repr(error)
        assert "SQL parameters hidden" in str(error)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("vad", [True, False], ids=["vad-on", "vad-off"])
async def test_whisper_reads_in_thread_and_sends_exact_bytes_and_explicit_vad(
    tmp_path, monkeypatch, vad: bool
) -> None:
    audio = b"RIFF\x00\xff\x80synthetic-WAVE-payload\r\n"
    path = tmp_path / "synthetic.wav"
    path.write_bytes(audio)
    main_thread = threading.get_ident()
    reads: list[tuple[Path, int]] = []
    domain_requests: list[TranscriptionRequest] = []
    wire_requests: list[httpx.Request] = []
    real_read_bytes = Path.read_bytes
    real_stat = Path.stat
    real_build = OpenAICompatibleAudioProvider.build_http_request

    def read_bytes(file: Path) -> bytes:
        reads.append((file, threading.get_ident()))
        return real_read_bytes(file)

    def stat(file: Path, *args, **kwargs):
        if file == path:
            pytest.fail("Whisper must derive size from loaded content, not a separate stat")
        return real_stat(file, *args, **kwargs)

    def build_request(provider, request):
        domain_requests.append(request)
        return real_build(provider, request)

    def respond(request: httpx.Request) -> httpx.Response:
        wire_requests.append(request)
        return httpx.Response(
            200,
            json={
                "text": "synthetic reply",
                "language": "en",
                "duration": 1.25,
                "segments": [{"start": 0.0, "end": 1.25, "text": "synthetic reply"}],
            },
        )

    to_thread = AsyncMock(wraps=asyncio.to_thread)
    monkeypatch.setattr(Path, "read_bytes", read_bytes)
    monkeypatch.setattr(Path, "stat", stat)
    monkeypatch.setattr(whisper_module, "asyncio", SimpleNamespace(to_thread=to_thread))
    monkeypatch.setattr(OpenAICompatibleAudioProvider, "build_http_request", build_request)
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    monkeypatch.setattr(whisper_module, "httpx", SimpleNamespace(AsyncClient=lambda: http_client))
    settings = SimpleNamespace(
        speech_whisper_base_url="https://whisper.invalid/v1/",
        speech_whisper_api_key="synthetic-whisper-key",
        speech_whisper_model="synthetic-offline-model",
        speech_whisper_language="en",
        speech_whisper_timeout_seconds=5.0,
        speech_whisper_vad_filter=vad,
    )
    client = whisper_module.build_whisper_client(settings)
    try:
        result = await client.transcribe(path)
        assert http_client.is_closed
    finally:
        await http_client.aclose()

    to_thread.assert_awaited_once_with(path.read_bytes)
    assert len(reads) == 1
    assert reads[0][0] == path
    assert reads[0][1] != main_thread, "file reading must not block the event loop"
    assert len(domain_requests) == 1
    request = domain_requests[0]
    assert request.content == audio
    assert isinstance(request.content, bytes)
    assert request.size_bytes == len(audio)
    assert request.filename == path.name
    assert request.audio_format == "wav"
    assert request.mime_type == "audio/wav"
    assert request.language == "en"
    assert client.config.extra_options == {"vad_filter": "true" if vad else "false"}
    assert client.config.response_format == "verbose_json"
    assert client.config.audio_window_seconds is None

    assert len(wire_requests) == 1
    wire = wire_requests[0]
    assert wire.method == "POST"
    assert str(wire.url) == "https://whisper.invalid/v1/audio/transcriptions"
    assert wire.headers["Authorization"] == "Bearer synthetic-whisper-key"
    multipart = BytesParser(policy=policy.default).parsebytes(
        f"Content-Type: {wire.headers['Content-Type']}\r\n\r\n".encode() + wire.content
    )
    assert multipart.is_multipart()
    parts = {
        part.get_param("name", header="content-disposition"): part
        for part in multipart.iter_parts()
    }
    assert set(parts) == {"file", "model", "language", "response_format", "vad_filter"}
    assert parts["file"].get_payload(decode=True) == audio
    assert parts["file"].get_filename() == path.name
    assert parts["file"].get_content_type() == "audio/wav"
    assert parts["model"].get_payload(decode=True) == b"synthetic-offline-model"
    assert parts["language"].get_payload(decode=True) == b"en"
    assert parts["response_format"].get_payload(decode=True) == b"verbose_json"
    assert parts["vad_filter"].get_payload(decode=True) == (b"true" if vad else b"false")
    assert result.text == "synthetic reply"
    assert result.model == "synthetic-offline-model"
    assert result.duration_seconds == 1.25
    assert len(result.segments) == 1
    assert result.segments[0].text == "synthetic reply"
