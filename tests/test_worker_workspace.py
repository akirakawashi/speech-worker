"""Проверки очистки аудио при ошибках, конкуренции и аварийном завершении."""

from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path

import pytest

from src.infrastructure.media.workspace import TemporaryRecordingWorkspace


def test_workspace_removes_audio_after_success(tmp_path: Path) -> None:
    """Успешный этап не оставляет скачанное аудио и производные файлы."""
    workspace = TemporaryRecordingWorkspace(tmp_path / "scratch")
    with workspace.directory(prefix="speech-transcribe-") as directory:
        (directory / "original.mp3").write_bytes(b"synthetic original")
        (directory / "mono.wav").write_bytes(b"synthetic wav")
    assert not directory.exists()


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
def test_workspace_removes_audio_after_failure(
    tmp_path: Path, failure: type[BaseException]
) -> None:
    """Обычная ошибка и отмена этапа проходят через очистку контекста."""
    workspace = TemporaryRecordingWorkspace(tmp_path / "scratch")
    with pytest.raises(failure), workspace.directory(prefix="speech-ingest-") as directory:
        (directory / "partial.mp3").write_bytes(b"partial synthetic audio")
        raise failure()
    assert not directory.exists()


def test_cleanup_keeps_files_used_by_another_workspace(tmp_path: Path) -> None:
    """Другой экземпляр воркера не удаляет активное задание с удерживаемым lock."""
    root = tmp_path / "scratch"
    owner = TemporaryRecordingWorkspace(root)
    cleaner = TemporaryRecordingWorkspace(root)
    with owner.directory(prefix="speech-transcribe-") as directory:
        audio = directory / "mono.wav"
        audio.write_bytes(b"synthetic active audio")
        assert cleaner.cleanup_abandoned() == 0
        assert audio.read_bytes() == b"synthetic active audio"
    assert not directory.exists()


def test_cleanup_removes_audio_left_by_a_crashed_process(tmp_path: Path) -> None:
    """Выход процесса без finally освобождает lock; следующий запуск убирает аудио."""
    root = tmp_path / "scratch"
    script = """
import os
import sys
from pathlib import Path
from src.infrastructure.media.workspace import TemporaryRecordingWorkspace

with TemporaryRecordingWorkspace(Path(sys.argv[1])).directory(prefix="speech-transcribe-") as path:
    (path / "mono.wav").write_bytes(b"synthetic unfinished audio")
    print(path, flush=True)
    os._exit(37)
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, str(root)],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 37, result.stderr
    abandoned = Path(result.stdout.strip())
    assert (abandoned / "mono.wav").is_file()
    workspace = TemporaryRecordingWorkspace(root)
    assert workspace.cleanup_abandoned() == 1
    assert not abandoned.exists()
    assert workspace.cleanup_abandoned() == 0


def test_cleanup_keeps_unmarked_directories_and_symlinks(tmp_path: Path) -> None:
    """Уборка ограничена собственными каталогами и не следует за ссылками."""
    root = tmp_path / "scratch"
    root.mkdir()
    unmarked = root / "job-foreign"
    unmarked.mkdir()
    (unmarked / "recording.mp3").write_bytes(b"foreign audio")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "recording.mp3").write_bytes(b"outside audio")
    (root / "job-link").symlink_to(outside, target_is_directory=True)
    assert TemporaryRecordingWorkspace(root).cleanup_abandoned() == 0
    assert (unmarked / "recording.mp3").is_file()
    assert (outside / "recording.mp3").is_file()


@pytest.mark.parametrize("prefix", ["", "../outside", "nested/path", "nested\\path"])
def test_workspace_rejects_a_prefix_with_path_components(tmp_path: Path, prefix: str) -> None:
    """Метка этапа не может направить временные записи за пределы корня."""
    with pytest.raises(ValueError), TemporaryRecordingWorkspace(tmp_path).directory(prefix=prefix):
        pytest.fail("invalid prefix was accepted")
