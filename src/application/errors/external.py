"""Нормализованные ошибки внешних операций, доступные сценариям."""

from __future__ import annotations


class MediaError(Exception):
    """Файл не разбирается как аудио, или разобрать его нечем."""


class StorageError(Exception):
    """Отказ хранилища. Тело ответа наружу не выносится."""

    def __init__(
        self, message: str, *, reason: str = "provider_error", status_code: int | None = None
    ) -> None:
        """Настроить StorageError из переданных параметров и зависимостей."""
        super().__init__(message)
        self.reason = reason
        self.status_code = status_code


class StorageNotConfiguredError(StorageError):
    """Хранилище не настроено — это не сбой, а незаконченная настройка."""


class RecordingNotAvailable(Exception):
    """У источника нет записи этого звонка.

    Часть контракта порта, а не деталь одного адаптера. Отличить «записи нет»
    от «источник недоступен» обязан этап скачивания: первое после нескольких
    попыток означает, что записи не будет вовсе (ING-04), второе — что надо
    повторять. Без объявленной здесь ошибки каждый адаптер выражал бы это
    по-своему — файловый `FileNotFoundError`, REST — `404`, — и этап ловил бы
    `Exception`, то есть считал бы отсутствием записи любую поломку.
    """


class WhisperNotConfigured(RuntimeError):
    """Адрес Whisper не задан.

    Отдельный тип, потому что это ошибка развёртывания, а не отказ этапа:
    повторять её бессмысленно, и класс отказа задания она получает другой.
    """
