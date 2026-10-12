"""Временное аудио на Linux-хосте и уборка каталогов после аварии."""

import fcntl
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

_JOB_PREFIX = "job-"
_JOB_LOCK = ".speech-worker-job.lock"
_ROOT_LOCK = ".speech-worker-root.lock"


class TemporaryRecordingWorkspace:
    """Выделять каталоги этапов и защищать используемые файлы файловой блокировкой."""

    def __init__(self, root: Path | None = None) -> None:
        """Запомнить корень временных файлов без создания каталогов.

        Args:
            root: Выделенный каталог воркера; None использует системный tmp/speech-worker.
        """
        self._root = root if root is not None else Path(tempfile.gettempdir()) / "speech-worker"

    @contextmanager
    def _root_lock(self) -> Iterator[None]:
        """Сериализовать создание и уборку каталогов процессов с общим корнем."""
        self._root.mkdir(parents=True, exist_ok=True, mode=0o700)
        with (self._root / _ROOT_LOCK).open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    @contextmanager
    def directory(self, *, prefix: str) -> Iterator[Path]:
        """Создать каталог этапа и удалить его после успеха, ошибки или отмены.

        Args:
            prefix: Метка этапа без разделителей пути.

        Yields:
            Путь к отдельному каталогу, защищённому до выхода из контекста.

        Raises:
            ValueError: Метка пуста или содержит разделитель пути.
            OSError: Каталог невозможно создать, заблокировать или очистить.
        """
        if not prefix or "/" in prefix or "\\" in prefix:
            raise ValueError("workspace prefix must be a non-empty path component")
        with self._root_lock():
            directory = Path(tempfile.mkdtemp(prefix=_JOB_PREFIX + prefix, dir=self._root))
            try:
                lock = (directory / _JOB_LOCK).open("x+b")
            except BaseException:
                shutil.rmtree(directory)
                raise
            try:
                fcntl.flock(lock, fcntl.LOCK_EX)
            except BaseException:
                lock.close()
                shutil.rmtree(directory)
                raise
        try:
            yield directory
        finally:
            try:
                with self._root_lock():
                    shutil.rmtree(directory)
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)
                lock.close()

    def cleanup_abandoned(self) -> int:
        """Удалить оставшиеся каталоги, если их блокировки больше никто не держит.

        Проверяются только подкаталоги собственного корня с меткой задания
        и файлом блокировки. Каталоги без метки, ссылки и активные задания
        пропускаются. ОС освобождает блокировку при аварийной остановке процесса.

        Returns:
            Количество удалённых каталогов.

        Raises:
            OSError: Проверка или удаление временных файлов не удались.
        """
        removed = 0
        with self._root_lock():
            for directory in self._root.iterdir():
                if (
                    not directory.name.startswith(_JOB_PREFIX)
                    or directory.is_symlink()
                    or not directory.is_dir()
                ):
                    continue
                lock_path = directory / _JOB_LOCK
                if lock_path.is_symlink() or not lock_path.is_file():
                    continue
                with lock_path.open("r+b") as lock:
                    try:
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        continue
                    try:
                        shutil.rmtree(directory)
                        removed += 1
                    finally:
                        fcntl.flock(lock, fcntl.LOCK_UN)
        return removed
