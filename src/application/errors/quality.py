"""Нормализованные отказы порта языковой модели оценки."""

_MISCONFIGURED: frozenset[str] = frozenset({"http_401", "http_403", "http_404"})


class QualityModelError(Exception):
    """Модель не ответила или ответила не тем.

    Текст ответа наружу не выносится: в нём пересказ разговора (BR-13).
    """

    def __init__(self, reason: str, *, retryable: bool) -> None:
        """Настроить QualityModelError из переданных параметров и зависимостей."""
        super().__init__(reason)
        self.reason = reason
        self.retryable = retryable

    @property
    def misconfigured(self) -> bool:
        """Отказ, который повторится на каждом звонке: ключ, адрес или имя модели.

        Отдельно от `retryable`: слишком длинный разговор (`400`) или ответ,
        упёршийся в `max_tokens`, повтором тоже не лечатся, но касаются одного
        звонка. Останавливать из-за них весь пакет значило бы, что один
        трёхчасовой звонок не даёт оценить остальные — при каждом запуске.
        """
        return self.reason in _MISCONFIGURED


class QualityModelNotConfigured(RuntimeError):
    """Адрес модели не задан — ошибка развёртывания."""
