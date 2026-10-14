# Настройки воркера

Поля и значения определены в [Settings](../src/setting/config.py).
Pydantic Settings читает `.env` из рабочего каталога, окружение имеет приоритет.
Кеш `get_settings()` обновляется при перезапуске. Рабочие `.env` при очистке
не редактировались; неизвестные старые поля игнорируются через `extra="ignore"`.

## Для основного процесса

| Операция | Настройки |
| --- | --- |
| Общая очередь | `SPEECH_MS_DATABASE_URL`, параметры пула `DB_*` |
| Решения ingest и доставка ASR | `SPEECH_BACKEND_BASE_URL`, `SPEECH_WORKER_API_TOKEN`, `SPEECH_BACKEND_TIMEOUT_SECONDS` |
| Хранение/чтение аудио | `SPEECH_STORAGE_BASE_URL`, bucket, регион, ключи и таймаут MinIO |
| Whisper | `SPEECH_WHISPER_BASE_URL`, модель, язык, VAD, ключ и таймаут `SPEECH_WHISPER_*` |
| Временные файлы | `SPEECH_WORKER_TEMP_DIR`; Docker использует отдельный том |
| Скачивание Телесейлза | `SPEECH_NCC_SOURCE_KIND`; для `sequence`/`reporting_view` — REST URL, имя пользователя, токен и таймаут |
| Локальная выгрузка | `SPEECH_NCC_SOURCE_KIND=local_export`, `SPEECH_NCC_EXPORT_DIR`; каталог доступен с тем же путём, что у бэкенда |
| Скачивание Телфина | Только `SPEECH_TELPHIN_TIMEOUT_SECONDS`; секретный URL приходит в команде бэкенда |

При ограничении `--stages` DI собирает ресурсы только выбранных этапов.
Для `ingest` обязательны служебный API и скачиватель; для `transcribe` — API,
MinIO и Whisper. Основная команда не создаёт LLM-клиенты.

`SPEECH_BACKEND_BASE_URL` — корень API без `/internal/v1`, токен совпадает
с бэкендом. Управление использует `/internal/v1/ingest/prepare` и `resolve`;
ASR — `/internal/v1/transcription-results`. Не настроенный адрес/ключ
останавливает выбранный этап до claim. Ошибка управления откладывает работу.
[Контракт](../../speech-backend/docs/intake-ownership.md).

Whisper вызывается через OpenAI-совместимый `/audio/transcriptions` с
`verbose_json`. Адрес обычно заканчивается на `/v1`. Дополнительного провайдера,
fallback, CUDA/PyTorch или GigaAM в воркере нет. Каждый запрос передаёт одну запись.
Внешняя отмена и бюджет HTTP сохраняют прежнее поведение транспорта.

## Что перенесено в бэкенд 14.10.2026

Из Settings и шаблона воркера удалены reporting DSN/пароль/представления/часовой
пояс NCC, `SPEECH_NCC_SESSION_ANCHOR`, пороги источников, API App ID/Secret
Телфина, исключения добавочных/групп, параметры истории и `SPEECH_PHONE_HASH_PEPPER`.
Фильтрация, нормализация классификации и номера клиента выполняются бэкендом.
Скачивателю `reporting_view` больше не нужна reporting-база.

Порог короткой записи, глубина истории и прежнее `daily_ingest_at` больше
не читаются основным воркером из `speech_settings`. Параметры технического ASR,
ограничение Whisper и бюджеты попыток очереди сохранены.
LLM-настройки `SPEECH_ANALYSIS_LLM_*`/`SPEECH_QUALITY_LLM_*` удалены ранее.

Расписание запускается в `speech-process` бэкенда. У воркера больше нет
`--sources`, `--queue-only`, `speech-ingest`, `speech-backfill` или `worker-plan`.
[Запуск](deployment.md). Справочники и отдельная команда хранения остаются
переходными до следующего шага; их правила пока не объявляем перенесёнными.
