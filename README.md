# um-ingest-server

HTTP-сервис для парсинга больших CSV файлов и отправки батчей в 1С.

## Описание

Сервис получает HTTP-задания от 1С, читает CSV-файлы с локального диска, парсит их потоково, приводит типы, режет на батчи и отправляет в HTTP endpoint 1С. Поддерживает файлы до ~900MB с 4–7 млн строк.

## Возможности

- Потоковое чтение CSV (не загружает весь файл в память)
- Поддержка кодировок: UTF-8, Windows-1251
- Настраиваемый разделитель (`,` или `;`)
- Маппинг полей по порядку или по заголовкам
- Приведение типов: string, int, number, date
- Батчинг с настраиваемым размером
- Retry с exponential backoff для HTTP запросов
- Backpressure через bounded channels
- Gzip сжатие запросов
- Обработка ошибок без падения job
- Отмена заданий через context

## Установка

### Сборка и деплой через Makefile (рекомендуется)

Для автоматизации сборки и деплоя используйте Makefile:

```bash
# Получить последние изменения
git pull

# Запустить тесты
make test

# Собрать бинарник с версионированием
make build

# Проверить версию собранного бинарника
make version

# Развернуть на VM (требует sudo)
make deploy
```

**Важно:**
- `make deploy` требует права `sudo` на VM для управления systemd сервисом
- Версия бинарника берётся из git tags (`git describe --tags --always`)
- Бинарник не хранится в репозитории (добавлен в `.gitignore`)

**Доступные цели Makefile:**
- `make help` — показать справку по всем целям
- `make pull` — выполнить `git pull`
- `make test` — запустить тесты
- `make build` — собрать бинарник с версионированием
- `make version` — показать версию собранного бинарника
- `make deploy` — развернуть сервис (test → build → stop → install → start → status)
- `make status` — показать статус сервиса
- `make restart` — перезапустить сервис
- `make clean` — удалить локальный бинарник
- `make check` — проверить доступность сервиса через `/version`

### Ручная сборка

#### Базовая сборка

```bash
go build -o um-ingest-server ./cmd/server
```

#### Сборка с версионированием

Для сборки с информацией о версии используйте `-ldflags`:

```bash
VERSION=$(git describe --tags --always 2>/dev/null || echo "dev")
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)

go build -ldflags "\
  -X 'github.com/ryabkov82/um-ingest-server/internal/version.Version=${VERSION}' \
  -X 'github.com/ryabkov82/um-ingest-server/internal/version.GitCommit=${GIT_COMMIT}' \
  -X 'github.com/ryabkov82/um-ingest-server/internal/version.BuildTime=${BUILD_TIME}'" \
  -o um-ingest-server ./cmd/server
```

**Примечание:** Если `git` недоступен, переменные получат значения по умолчанию (`dev`, `unknown`).

## Конфигурация

Сервис настраивается через переменные окружения:

- `ALLOWED_BASE_DIR` - **граница безопасности** (security boundary) для входных файлов (по умолчанию: `/data/incoming`). Все входные файлы обязаны находиться внутри этого каталога. Сервис проверяет, что `inputPath` находится внутри `ALLOWED_BASE_DIR` с учётом нормализации путей и разрешения симлинков.
- `PORT` - порт HTTP сервера (по умолчанию: `8080`)
- `UM_INGEST_API_KEY` - API ключ для аутентификации (если не задан, auth отключен)
- `UM_1C_BASIC_USER` - имя пользователя для Basic аутентификации при отправке батчей в 1С (обязательно, если не указан `delivery.auth`)
- `UM_1C_BASIC_PASS` - пароль для Basic аутентификации при отправке батчей в 1С (обязательно, если не указан `delivery.auth`)
- `UM_TIMINGS` - включить сбор метрик времени выполнения (опционально, значение: `1`). По умолчанию выключено. Если задан `UM_TIMINGS=1`, сервис собирает метрики по стадиям пайплайна (CSV read, Transform, Batch assembly, JSON marshal, gzip, HTTP) и логирует сводку в конце каждого job.
- `UM_PPROF_ADDR` - адрес для HTTP сервера pprof (опционально, например: `localhost:6060`). Если задан, запускается HTTP сервер для профилирования производительности. Доступны стандартные endpoints Go pprof: `/debug/pprof/`, `/debug/pprof/profile`, `/debug/pprof/heap` и т.д.
- `UM_CPU_PROFILE` - путь к файлу для записи CPU профиля (опционально, например: `/tmp/cpu.prof`). Если задан, сервис записывает CPU профиль в указанный файл на время работы процесса. Для анализа используйте `go tool pprof`.

**Примечание:** Basic Authentication используется для аутентификации при обращении к HTTP endpoint 1С (уровень публикации/платформы). `um-ingest-server` формирует заголовок `Authorization: Basic ...` при отправке батчей данных и ошибок в 1С. Это не связано с аутентификацией на уровне IIS или веб-сервера.

## Запуск

```bash
export ALLOWED_BASE_DIR=/data/incoming
export PORT=8080
export UM_INGEST_API_KEY=your-secret-key
export UM_1C_BASIC_USER=1c_user
export UM_1C_BASIC_PASS=1c_password
./um-ingest-server
```

## Проверка версии

### Через CLI

```bash
./um-ingest-server --version
```

Вывод:
```
um-ingest-server v1.2.3 (commit abcdef1, built 2026-01-28T10:00:00Z)
```

Для JSON формата:
```bash
./um-ingest-server --build-info
```

### Через HTTP API

```bash
curl http://localhost:8080/version
```

Ответ:
```json
{
  "name": "um-ingest-server",
  "version": "1.2.3",
  "gitCommit": "abcdef1",
  "buildTime": "2026-01-28T10:00:00Z",
  "goVersion": "go1.22.3"
}
```

**Примечание:** Endpoint `/version` не требует аутентификации, что позволяет быстро проверить версию развернутого сервиса.

## API

### packageId как бизнес-ключ

`packageId` является бизнес-ключом задания:
- **Обязателен** при создании задания (POST /jobs)
- **Уникален** для активных заданий: нельзя запустить два задания с одинаковым `packageId` параллельно
- Позволяет 1С работать **без хранения jobId** (используя endpoints по packageId)
- `jobId` остаётся внутренним идентификатором конкретной попытки выполнения

При попытке создать задание с уже активным `packageId` (статус `queued` или `running`) сервис вернёт **HTTP 409 Conflict**.

### POST /jobs

Создать новое задание.

**Важно:** 
- `packageId` обязателен и не может быть пустым.
- `inputPath` — **абсолютный путь** к CSV-файлу, который обязан находиться внутри каталога `ALLOWED_BASE_DIR`.
- Если `delivery.auth` не указан, должны быть заданы переменные окружения `UM_1C_BASIC_USER` и `UM_1C_BASIC_PASS`. В противном случае запрос будет отклонён с ошибкой `400 Bad Request`.

**Проверка безопасности `inputPath`:**
- Сервис нормализует путь (преобразует в абсолютный через `filepath.Abs`)
- Разрешает симлинки через `filepath.EvalSymlinks` для защиты от обхода через символические ссылки
- Проверяет, что итоговый путь находится внутри `ALLOWED_BASE_DIR`
- Запрещает path traversal (проверка на `..` и `..` + разделитель)
- При нарушении условий возвращает **400 Bad Request** с сообщением `Invalid input path`

**Пример:**
При конфигурации `ALLOWED_BASE_DIR=/mnt/pilot` допустимый `inputPath`:
```json
{
  "inputPath": "/mnt/pilot/Sirax/2025-07/T10/mis_daily_t10_072025_7cbeed30.csv"
}
```

**Заголовки:**
- `Content-Type: application/json`
- `X-API-Key: your-secret-key` (если настроен)

**Тело запроса:**
```json
{
  "packageId": "package-123",
  "inputPath": "/data/incoming/data.csv",
  "csv": {
    "encoding": "utf-8",
    "delimiter": ";",
    "hasHeader": true,
    "mapBy": "header"
  },
  "schema": {
    "register": "РегистрСведений.Данные",
    "includeRowNo": true,
    "rowNoField": "НомерСтрокиФайла",
    "fields": [
      {
        "out": "ИмяПоля",
        "type": "string",
        "required": false,
        "source": {
          "by": "order",
          "index": 0
        },
        "constraints": {
          "maxLen": 100
        }
      },
      {
        "out": "ДатаОпер",
        "type": "date",
        "required": true,
        "source": {
          "by": "header",
          "name": "OperationDate"
        },
        "dateFormat": "DD.MM.YYYY",
        "dateFallbacks": ["DD.MM.YY", "YYYY-MM-DD"]
      },
      {
        "out": "Amount",
        "type": "number",
        "source": {
          "by": "header",
          "name": "Amount"
        },
        "decimalSeparator": ","
      }
    ]
  },
  "delivery": {
    "endpoint": "https://1c-host/hs/um_ingest/load_batch",
    "errorsEndpoint": "https://1c-host/hs/um_ingest/load_errors",
    "gzip": true,
    "batchSize": 2000,
    "timeoutSeconds": 60,
    "maxRetries": 8,
    "backoffMs": 500,
    "backoffMaxMs": 10000,
    "auth": {
      "type": "basic",
      "user": "1c_user",
      "pass": "1c_password"
    }
  },
  "errorsJsonl": "/var/log/um_ingest_errors.jsonl",
  "progressEvery": 50000
}
```

**Примечание:** Поле `delivery.auth` является **DEPRECATED** (устаревшим). Рекомендуется использовать переменные окружения `UM_1C_BASIC_USER` и `UM_1C_BASIC_PASS` вместо передачи креденшалов в теле запроса. Если `delivery.auth` не указан, сервис будет использовать креденшалы из переменных окружения. Если указан `delivery.auth`, он имеет приоритет над переменными окружения (для обратной совместимости).

**Ответ:**
```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "queued"
}
```

### GET /jobs/{jobId}

Получить статус задания.

**Ответ:**
```json
{
  "status": "running",
  "startedAt": "2026-01-31T10:00:00Z",
  "rowsRead": 150000,
  "rowsSent": 148000,
  "rowsSkipped": 2000,
  "batchesSent": 74,
  "currentBatchNo": 74,
  "inputPath": "/data/incoming/data.csv",
  "fileType": "csv"
}
```

**Статусы:**
- `queued` - в очереди
- `running` - выполняется
- `succeeded` - успешно завершено
- `failed` - завершено с ошибкой
- `canceled` - отменено

### POST /jobs/{jobId}/cancel

Отменить задание.

**Ответ:**
```json
{
  "status": "canceled"
}
```

### GET /packages/{packageId}/job

Получить статус задания по `packageId` (для работы 1С без хранения jobId).

**Поведение:**
1. Если есть активное задание (статус `queued` или `running`):
   - **200 OK** с телом, аналогичным `GET /jobs/{jobId}`, но дополнительно включающим поле `jobId`
2. Если активного задания нет, но есть последнее завершённое задание для этого `packageId`:
   - **200 OK** с телом последнего задания (включая `finishedAt`, `lastError` и т.д.)
3. Если заданий для `packageId` никогда не было:
   - **404 Not Found**

**Назначение:** Это позволяет 1С получать статус загрузки пакета без хранения `jobId`, используя только бизнес-ключ `packageId`.

**Примечание:** История заданий хранится в памяти сервиса и может быть потеряна при его перезапуске. После перезапуска для ранее завершённых заданий будет возвращаться **404 Not Found**.

**Ответ:**

**Пример ответа (активное задание):**
```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "startedAt": "2026-01-31T10:00:00Z",
  "rowsRead": 150000,
  "rowsSent": 148000,
  "rowsSkipped": 2000,
  "batchesSent": 74,
  "currentBatchNo": 74,
  "errorsTotal": 50,
  "errorsSent": 48,
  "inputPath": "/data/incoming/data.csv",
  "fileType": "csv"
}
```

**Пример ответа (последнее завершённое задание):**
```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "succeeded",
  "startedAt": "2026-01-31T10:00:00Z",
  "finishedAt": "2026-01-31T10:05:00Z",
  "rowsRead": 500000,
  "rowsSent": 500000,
  "rowsSkipped": 0,
  "batchesSent": 250,
  "currentBatchNo": 250,
  "errorsTotal": 0,
  "errorsSent": 0,
  "inputPath": "/data/incoming/data.csv",
  "fileType": "csv"
}
```

**Пример ответа (последнее задание с ошибкой):**
```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "failed",
  "startedAt": "2026-01-31T10:00:00Z",
  "finishedAt": "2026-01-31T10:02:00Z",
  "lastError": "Connection timeout",
  "rowsRead": 100000,
  "rowsSent": 95000,
  "rowsSkipped": 5000,
  "batchesSent": 47,
  "currentBatchNo": 48,
  "errorsTotal": 10,
  "errorsSent": 8,
  "inputPath": "/data/incoming/data.csv",
  "fileType": "csv"
}
```

### POST /packages/{packageId}/cancel

Отменить активное задание по `packageId` (для работы 1С без хранения jobId).

**Ответ:**
- Если активное задание найдено:
  - **200 OK**:
    ```json
    {
      "status": "canceled",
      "jobId": "550e8400-e29b-41d4-a716-446655440000"
    }
    ```
- Если активного задания нет:
  - **404 Not Found**

## Примеры

### Создание задания без delivery.auth (используются env переменные)

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-key" \
  -d '{
    "packageId": "package-123",
    "inputPath": "/data/incoming/data.csv",
    "csv": {
      "encoding": "utf-8",
      "delimiter": ";",
      "hasHeader": true,
      "mapBy": "header"
    },
    "schema": {
      "register": "РегистрСведений.Данные",
      "fields": []
    },
    "delivery": {
      "endpoint": "http://1c-server/load_batch",
      "batchSize": 1000
    }
  }'
```

В этом примере Basic аутентификация для 1С будет взята из переменных окружения `UM_1C_BASIC_USER` и `UM_1C_BASIC_PASS`.

## Примеры (legacy с delivery.auth) использования

### Создание задания

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-key" \
  -d '{
    "packageId": "test-123",
    "inputPath": "/data/incoming/test.csv",
    "csv": {
      "encoding": "utf-8",
      "delimiter": ",",
      "hasHeader": true,
      "mapBy": "header"
    },
    "schema": {
      "register": "TestRegister",
      "includeRowNo": false,
      "fields": [
        {
          "out": "Name",
          "type": "string",
          "source": {"by": "header", "name": "Name"}
        },
        {
          "out": "Age",
          "type": "int",
          "source": {"by": "header", "name": "Age"}
        }
      ]
    },
    "delivery": {
      "endpoint": "http://localhost:8081/load_batch",
      "errorsEndpoint": "http://localhost:8081/load_errors",
      "gzip": false,
      "batchSize": 1000,
      "timeoutSeconds": 30,
      "maxRetries": 3,
      "backoffMs": 500,
      "backoffMaxMs": 5000,
      "auth": {
        "type": "basic",
        "user": "user",
        "pass": "pass"
      }
    },
    "progressEvery": 10000
  }'
```

### Проверка статуса

По `jobId`:
```bash
curl http://localhost:8080/jobs/550e8400-e29b-41d4-a716-446655440000 \
  -H "X-API-Key: your-secret-key"
```

По `packageId` (для 1С):
```bash
curl http://localhost:8080/packages/package-123/job \
  -H "X-API-Key: your-secret-key"
```

### Отмена задания

По `jobId`:
```bash
curl -X POST http://localhost:8080/jobs/550e8400-e29b-41d4-a716-446655440000/cancel \
  -H "X-API-Key: your-secret-key"
```

По `packageId` (для 1С):
```bash
curl -X POST http://localhost:8080/packages/package-123/cancel \
  -H "X-API-Key: your-secret-key"
```

### Ошибка при дублировании packageId

Если попытаться создать задание с уже активным `packageId`:
```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-key" \
  -d '{"packageId": "package-123", ...}'
```

**Ответ 409 Conflict:**
```json
{
  "error": "job_already_running",
  "packageId": "package-123",
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running"
}
```

## Формат батча

Сервис отправляет батчи в следующем формате:

```json
{
  "packageId": "package-123",
  "batchNo": 1,
  "register": "РегистрСведений.Данные",
  "rows": [
    {
      "НомерСтрокиФайла": 1,
      "ИмяПоля": "abc",
      "ДатаОпер": "2026-01-31",
      "Amount": 123.45
    },
    {
      "НомерСтрокиФайла": 2,
      "ИмяПоля": "def",
      "ДатаОпер": "2026-02-01",
      "Amount": 10.0
    }
  ]
}
```

**Особенности формата:**
- `rows` — массив объектов, где ключи — это имена полей из `schema.fields` (и `rowNoField` если `includeRowNo=true`)
- Даты отправляются в формате ISO (YYYY-MM-DD)
- Типы: `string`, `int`, `number` — как есть, `date` — строка ISO

### HTTP заголовки батча

При отправке каждого батча в 1С (POST на `delivery.endpoint`) сервис автоматически добавляет следующие HTTP заголовки:

- **`X-UM-PackageId`** — `packageId` из задания (совпадает с полем `packageId` в JSON теле запроса)
- **`X-UM-BatchNo`** — номер батча (десятичное число без пробелов, начиная с 1)
- **`X-UM-RowsCount`** — количество строк в этом батче (`len(rows)`)

**Назначение:** 1С может использовать только заголовки для определения пакета/батча без парсинга JSON тела запроса, что значительно ускоряет обработку больших батчей.

**Важно:** Заголовки отправляются при **каждой попытке** отправки батча, включая retry при ошибках (429, 503, 5xx).

**Пример HTTP запроса:**
```
POST /hs/um_ingest/load_batch HTTP/1.1
Host: 1c-host
Content-Type: application/json
Content-Encoding: gzip
X-UM-PackageId: package-123
X-UM-BatchNo: 1
X-UM-RowsCount: 2000
Authorization: Basic dXNlcjpwYXNz

{...gzipped JSON...}
```

## Формат батча ошибок

Если задан `delivery.errorsEndpoint`, ошибки отправляются в следующем формате:

```json
{
  "packageId": "package-123",
  "errors": [
    {
      "rowNo": 123,
      "class": "Техническая",
      "severity": "Ошибка",
      "code": "НеПреобразуетсяВДату",
      "field": "Date",
      "value": "31-31-2026",
      "message": "ожидаемый формат DD.MM.YYYY",
      "ts": "2026-01-26T22:10:00Z"
    }
  ]
}
```

**Коды ошибок:**
- `ОшибкаРазбораCSV` — общая ошибка парсинга
- `НеверноеЧислоКолонок` — неверное количество колонок
- `НеПреобразуетсяВДату` — ошибка парсинга даты
- `НеПреобразуетсяВЧисло` — ошибка парсинга числа
- `СлишкомДлинноеЗначение` — превышение maxLen
- `ПустоеОбязательноеПоле` — обязательное поле (`required: true`) пустое после `TrimSpace`
- `НедопустимоеЗначение` — прочие ошибки валидации

## Обработка ошибок

- Ошибки парсинга строк логируются в JSONL файл (если указан `errorsJsonl`)
- Если задан `delivery.errorsEndpoint`, ошибки отправляются батчами в 1С
- Строки с row-level ошибками (required, type, maxLen, index out of range) **не пропускаются** — они попадают в batch, а ошибки фиксируются отдельно
- Счетчики `errorsTotal` и `errorsSent` отображаются в статусе job

### Семантика HTTP-кодов при отправке батчей в 1С

При отправке батчей данных на `delivery.endpoint` и ошибок на `delivery.errorsEndpoint` сервис обрабатывает HTTP-коды ответа следующим образом:

| HTTP код | Семантика | Поведение сервиса |
|----------|-----------|-------------------|
| **200 OK** | Батч принят (в т.ч. дубликат) | Успех, батч считается отправленным |
| **400 Bad Request** | Ошибка контракта (неверный формат данных) | **Не ретраится**, job завершается с ошибкой |
| **401 Unauthorized** | Ошибка аутентификации | **Не ретраится**, job завершается с ошибкой |
| **409 Conflict** | Конфликт/дубликат | Можно считать успешным (батч уже обработан) |
| **429 Too Many Requests** | Превышен лимит запросов | **Ретраится** с exponential backoff |
| **503 Service Unavailable** | Временная недоступность | **Ретраится** с exponential backoff |
| **5xx** | Временная ошибка сервера | **Ретраится** с exponential backoff |

**Важно для 1С:** Обработчик батчей в 1С должен возвращать соответствующие HTTP-коды согласно этой семантике. В частности:
- **200 OK** — батч успешно обработан (даже если это дубликат)
- **409 Conflict** — батч уже был обработан ранее (idempotency)
- **400/401** — ошибка, которая не будет исправлена повторной отправкой
- **429/503/5xx** — временная ошибка, требующая повторной отправки

## Профилирование и метрики

### pprof HTTP сервер

Для профилирования производительности в реальном времени:

```bash
export UM_PPROF_ADDR=localhost:6060
./um-ingest-server
```

После запуска доступны стандартные endpoints Go pprof:
- `http://localhost:6060/debug/pprof/` - обзор профилей
- `http://localhost:6060/debug/pprof/profile` - CPU профиль (30 секунд)
- `http://localhost:6060/debug/pprof/heap` - профиль памяти
- `http://localhost:6060/debug/pprof/goroutine` - профиль горутин

Пример использования:
```bash
# Получить CPU профиль за 30 секунд
curl http://localhost:6060/debug/pprof/profile > cpu.prof
go tool pprof cpu.prof
```

### CPU профиль в файл

Для записи CPU профиля на время работы процесса:

```bash
export UM_CPU_PROFILE=/tmp/cpu.prof
./um-ingest-server
# ... работа сервиса ...
# Ctrl+C для остановки
go tool pprof /tmp/cpu.prof
```

### Метрики времени выполнения

**Включение метрик:**

Метрики времени выполнения собираются только если задан env-флаг `UM_TIMINGS=1`:

```bash
export UM_TIMINGS=1
./um-ingest-server
```

По умолчанию метрики выключены для минимизации overhead.

**Пример использования на тесте:**

```bash
# Включить метрики для анализа производительности
export UM_TIMINGS=1
export ALLOWED_BASE_DIR=/tmp
export UM_1C_BASIC_USER=test_user
export UM_1C_BASIC_PASS=test_pass
./um-ingest-server
# ... создать job через POST /jobs ...
# В логах будет сводка метрик после завершения job
```

После завершения каждого job (если `UM_TIMINGS=1`) сервис автоматически логирует сводку метрик времени выполнения по стадиям пайплайна:

```
Job abc-123: Timings summary: CSV read: total=2.5s count=1000000 avg=2.5µs; Transform: total=5.2s count=1000000 avg=5.2µs; Batch assembly: total=50ms count=500 avg=100µs; Data marshal: total=200ms count=500 avg=400µs; Data gzip: total=150ms count=500 avg=300µs; Data HTTP: total=10s count=500 avg=20ms; Errors HTTP: total=500ms count=10 avg=50ms
```

**Метрики включают:**
- **CSV read** - суммарное время чтения строк из CSV (`csv.Reader.Read()`)
- **Transform** - суммарное время трансформации строк (`TransformRow`)
- **Batch assembly** - суммарное время сборки батчей
- **Data marshal** - суммарное время JSON маршалинга для data batches
- **Data gzip** - суммарное время gzip сжатия для data batches (если включено)
- **Data HTTP** - суммарное время HTTP round-trip для data batches
- **Errors marshal/gzip/HTTP** - аналогичные метрики для error batches

Для каждой метрики выводится: общее время, количество операций, среднее время на операцию.

**Примечание:** Метрики собираются только во время выполнения job и не влияют на производительность (используется только stdlib, без внешних зависимостей).

## Тестирование

```bash
go test ./...
```

## Развертывание

### Автоматический деплой через Makefile

Для автоматического развертывания на VM используйте:

```bash
make deploy
```

Эта команда последовательно выполняет:
1. `make test` — запускает тесты
2. `make build` — собирает бинарник с версионированием
3. Останавливает systemd сервис
4. Устанавливает бинарник в `/opt/um-ingest-server/`
5. Запускает systemd сервис
6. Показывает статус сервиса
7. Проверяет доступность через `/version` (best-effort)

**Требования:**
- Права `sudo` на VM
- Настроенный systemd unit файл (см. `deploy/um-ingest.service`)
- Пользователь и группа `um-ingest` должны существовать

**Настройка переменных:**
```bash
make deploy SERVICE=um-ingest.service INSTALL_DIR=/opt/um-ingest-server
```

### Ручной деплой

См. `deploy/um-ingest.service` для примера systemd unit файла.

После сборки бинарника:
```bash
sudo install -o um-ingest -g um-ingest -m 0755 ./um-ingest-server /opt/um-ingest-server/um-ingest-server
sudo systemctl restart um-ingest.service
sudo systemctl status um-ingest.service
```
