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

```bash
go build -o um-ingest-server ./cmd/server
```

## Конфигурация

Сервис настраивается через переменные окружения:

- `ALLOWED_BASE_DIR` - базовая директория для входных файлов (по умолчанию: `/data/incoming`)
- `PORT` - порт HTTP сервера (по умолчанию: `8080`)
- `UM_INGEST_API_KEY` - API ключ для аутентификации (если не задан, auth отключен)
- `UM_1C_BASIC_USER` - имя пользователя для Basic аутентификации при отправке батчей в 1С (обязательно, если не указан `delivery.auth`)
- `UM_1C_BASIC_PASS` - пароль для Basic аутентификации при отправке батчей в 1С (обязательно, если не указан `delivery.auth`)

**Примечание:** Basic аутентификация используется для аутентификации на стороне 1С (платформа/публикация), а не для IIS.

## Запуск

```bash
export ALLOWED_BASE_DIR=/data/incoming
export PORT=8080
export UM_INGEST_API_KEY=your-secret-key
export UM_1C_BASIC_USER=1c_user
export UM_1C_BASIC_PASS=1c_password
./um-ingest-server
```

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
- Если `delivery.auth` не указан, должны быть заданы переменные окружения `UM_1C_BASIC_USER` и `UM_1C_BASIC_PASS`. В противном случае запрос будет отклонён с ошибкой `400 Bad Request`.

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
- `НедопустимоеЗначение` — прочие ошибки валидации

## Обработка ошибок

- Ошибки парсинга строк логируются в JSONL файл (если указан `errorsJsonl`)
- Если задан `delivery.errorsEndpoint`, ошибки отправляются батчами в 1С
- Строки с ошибками пропускаются, job продолжает работу
- HTTP ошибки 429/503 и 5xx ретраятся с exponential backoff
- HTTP ошибки 4xx (кроме 429) считаются фатальными и останавливают job
- Счетчики `errorsTotal` и `errorsSent` отображаются в статусе job

## Тестирование

```bash
go test ./...
```

## Развертывание

См. `deploy/um-ingest.service` для примера systemd unit файла.
