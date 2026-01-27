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

## Запуск

```bash
export ALLOWED_BASE_DIR=/data/incoming
export PORT=8080
export UM_INGEST_API_KEY=your-secret-key
./um-ingest-server
```

## API

### POST /jobs

Создать новое задание.

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

## Примеры использования

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

```bash
curl http://localhost:8080/jobs/550e8400-e29b-41d4-a716-446655440000 \
  -H "X-API-Key: your-secret-key"
```

### Отмена задания

```bash
curl -X POST http://localhost:8080/jobs/550e8400-e29b-41d4-a716-446655440000/cancel \
  -H "X-API-Key: your-secret-key"
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
