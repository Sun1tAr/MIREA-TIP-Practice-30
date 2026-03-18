# Практическое занятие №14 (30). Реализация очереди задач (producer-consumer): retries, DLQ, идемпотентность

## Выполнил: Туев Д. ЭФМО-01-25

## Содержание

1. [Описание проекта](#описание-проекта)
2. [Архитектура очереди задач](#архитектура-очереди-задач)
3. [Настройка RabbitMQ: DLX и очереди](#настройка-rabbitmq-dlx-и-очереди)
4. [Формат сообщения задачи](#формат-сообщения-задачи)
5. [Producer: постановка задач в очередь](#producer-постановка-задач-в-очередь)
6. [Consumer: обработка с retry и DLQ](#consumer-обработка-с-retry-и-dlq)
7. [Идемпотентность обработки](#идемпотентность-обработки)
8. [Запуск системы](#запуск-системы)
9. [Скриншоты выполнения](#скриншоты-выполнения)
10. [Выводы](#выводы)
11. [Контрольные вопросы](#контрольные-вопросы)

---

## Описание проекта

В рамках практического занятия №14 на базе сервисов **Tasks** (producer) и **Worker** (consumer) реализована очередь задач с поддержкой повторных попыток (retries), dead letter queue (DLQ) и идемпотентной обработкой.

**Цель работы:** Построить рабочую очередь задач, которая устойчиво обрабатывает ошибки: временные ошибки ретраятся, "плохие" сообщения уходят в DLQ, а обработчик устойчив к дублям (идемпотентен).

### Отличие от предыдущего занятия (ПЗ 29)

| Характеристика | ПЗ 29 (события) | ПЗ 30 (задачи) |
|----------------|------------------|----------------|
| **Тип сообщения** | Событие (event) | Задача (job) |
| **Обработка** | Лёгкая, мгновенная | Тяжёлая, 2-5 сек |
| **Retry** | Нет | Да, с задержкой |
| **DLQ** | Нет | Да |
| **Идемпотентность** | Нет | Да (по message_id) |

---

## Архитектура очереди задач

### Компоненты системы

| Компонент | Роль | Порт |
|-----------|------|------|
| **RabbitMQ** | Брокер сообщений с DLX | 5672 (AMQP), 15672 (Management) |
| **tasks** | Producer, ставит задачи в очередь | 8082 |
| **worker** | Consumer, обрабатывает задачи с retry/DLQ | - |
| **PostgreSQL** | Хранилище данных задач | 5432 |
| **auth** | Аутентификация и cookies | 8081, 50051 |

### Схема взаимодействия с retry и DLQ

```mermaid
graph TD
    Client[Клиент] -->|POST /v1/jobs/process-task| Tasks[Tasks Service]
    Tasks -->|1. Публикация job| MainQueue[Основная очередь task_jobs]
    
    MainQueue -->|2. Чтение| Worker[Worker Service]
    Worker -->|3. Обработка| Process{Успех?}
    
    Process -->|Да| Success[✅ Логируем успех]
    Process -->|Нет| CheckAttempts{attempt <= max?}
    
    CheckAttempts -->|Да| RetryQueue[Retry очередь с TTL]
    RetryQueue -->|Через TTL| MainQueue
    
    CheckAttempts -->|Нет| DLQ[Dead Letter Queue task_jobs_dlq]
```

### Очереди и их назначение

| Очередь | Назначение | Параметры |
|---------|------------|-----------|
| `task_jobs` | Основная очередь задач | durable, с DLX |
| `task_jobs_retry` | Очередь для повторных попыток | durable, TTL 10 сек, с DLX |
| `task_jobs_dlq` | Dead Letter Queue для "плохих" сообщений | durable |

### Dead Letter Exchanges (DLX)

- **Для retry очереди:** при истечении TTL сообщение через DLX возвращается в основную очередь
- **Для основной очереди:** при превышении попыток сообщение через DLX отправляется в DLQ

---

## Настройка RabbitMQ: DLX и очереди

### Конфигурация в docker-compose

```yaml
worker:
  build:
    context: ../services/worker
    dockerfile: Dockerfile
  container_name: worker-service
  environment:
    RABBITMQ_URL: amqp://guest:guest@rabbitmq:5672/
    MAIN_QUEUE: task_jobs
    RETRY_QUEUE: task_jobs_retry
    DLQ: task_jobs_dlq
    RABBITMQ_PREFETCH: 1
    MAX_ATTEMPTS: 3
    RETRY_DELAY_SECONDS: 10
    LOG_LEVEL: debug
```

### Инициализация инфраструктуры в коде

При старте worker создаёт все необходимые элементы:

```go
func setupInfrastructure(ch *amqp.Channel, mainQueue, retryQueue, dlq string, retryDelay int) error {
    // 1. DLX для retry очереди
    retryDLX := mainQueue + "_dlx"
    ch.ExchangeDeclare(retryDLX, "direct", true, false, false, false, nil)

    // 2. Retry очередь с TTL и DLX
    ch.QueueDeclare(retryQueue, true, false, false, false, amqp.Table{
        "x-dead-letter-exchange":    retryDLX,
        "x-dead-letter-routing-key": mainQueue,
        "x-message-ttl":             int32(retryDelay * 1000),
    })

    // 3. Привязка retry DLX к mainQueue
    ch.QueueBind(mainQueue, mainQueue, retryDLX, false, nil)

    // 4. DLX для основной очереди
    mainDLX := mainQueue + "_dlx"
    ch.ExchangeDeclare(mainDLX, "direct", true, false, false, false, nil)

    // 5. Основная очередь с DLX
    ch.QueueDeclare(mainQueue, true, false, false, false, amqp.Table{
        "x-dead-letter-exchange":    mainDLX,
        "x-dead-letter-routing-key": dlq,
    })

    // 6. DLQ
    ch.QueueDeclare(dlq, true, false, false, false, nil)

    // 7. Привязка DLQ к main DLX
    ch.QueueBind(dlq, dlq, mainDLX, false, nil)
}
```

---

## Формат сообщения задачи

Для задачи выбран формат JSON с обязательными полями для retry и идемпотентности.

### Структура `JobMessage` (общий пакет `shared/models`)

```go
package models

type JobMessage struct {
    Job       string `json:"job"`        // тип задачи (process_task)
    TaskID    string `json:"task_id"`     // ID задачи для обработки
    Attempt   int    `json:"attempt"`     // номер попытки
    MessageID string `json:"message_id"`  // уникальный ID для идемпотентности
    CreatedAt string `json:"created_at,omitempty"` // время создания
}
```

### Пример сообщения

```json
{
  "job": "process_task",
  "task_id": "t_001",
  "attempt": 1,
  "message_id": "550e8400-e29b-41d4-a716-446655440000",
  "created_at": "2026-03-18T15:30:00Z"
}
```

### Назначение полей

| Поле | Описание |
|------|----------|
| `job` | Тип задачи (позволяет в будущем добавлять другие типы) |
| `task_id` | Идентификатор задачи для обработки |
| `attempt` | Счётчик попыток (увеличивается при каждой retry) |
| `message_id` | **Ключевое поле для идемпотентности** |
| `created_at` | Временная метка для отладки |

---

## Producer: постановка задач в очередь

### Новый endpoint в tasks

```go
// CreateJobHandler обрабатывает POST /v1/jobs/process-task
func (h *TaskHandler) CreateJobHandler(w http.ResponseWriter, r *http.Request) {
    var req struct {
        TaskID string `json:"task_id"`
    }
    json.NewDecoder(r.Body).Decode(&req)

    // Создаём задачу в очереди
    job, err := h.taskService.CreateJob(r.Context(), req.TaskID)
    
    w.WriteHeader(http.StatusAccepted) // 202 Accepted
    json.NewEncoder(w).Encode(job)
}
```

### Сервисный метод CreateJob

```go
func (s *TaskService) CreateJob(ctx context.Context, taskID string) (*JobResponse, error) {
    messageID := uuid.New().String()
    
    job := models.JobMessage{
        Job:       "process_task",
        TaskID:    taskID,
        Attempt:   1,
        MessageID: messageID,
        CreatedAt: time.Now().UTC().Format(time.RFC3339),
    }

    // Публикация в очередь task_jobs
    if err := s.producer.PublishJob(ctx, job); err != nil {
        return nil, err
    }

    return &JobResponse{
        Job:       job.Job,
        TaskID:    job.TaskID,
        Attempt:   job.Attempt,
        MessageID: job.MessageID,
        Status:    "queued",
    }, nil
}
```

### Публикация в RabbitMQ

```go
func (p *Producer) PublishJob(ctx context.Context, job models.JobMessage) error {
    body, _ := json.Marshal(job)
    
    return p.channel.PublishWithContext(ctx,
        "",              // exchange
        "task_jobs",     // routing key
        true,            // mandatory
        false,           // immediate
        amqp091.Publishing{
            ContentType:  "application/json",
            DeliveryMode: amqp091.Persistent,
            MessageId:    job.MessageID,
            Timestamp:    time.Now(),
            Body:         body,
        })
}
```

---

## Consumer: обработка с retry и DLQ

### Алгоритм обработки

1. Получить сообщение из основной очереди
2. Проверить идемпотентность (по `message_id`)
3. Имитировать "тяжёлую" обработку (sleep 2 сек)
4. Если ошибка:
    - Увеличить `attempt`
    - Если `attempt <= max_attempts` → отправить в retry очередь
    - Иначе → отправить в DLQ
5. Если успех:
    - Запомнить `message_id` как обработанный
    - Отправить ack

### Код обработки

```go
func (c *Consumer) processMessage(msg amqp.Delivery) {
    var job models.JobMessage
    json.Unmarshal(msg.Body, &job)

    // Идемпотентность
    if isProcessed(job.MessageID) {
        msg.Ack(false) // пропускаем дубль
        return
    }

    // Имитация тяжёлой работы
    time.Sleep(2 * time.Second)

    // Имитация ошибки для демонстрации
    shouldFail := (job.TaskID == "t_fail" || job.TaskID == "fail")

    if shouldFail {
        job.Attempt++
        
        if job.Attempt <= c.maxAttempts {
            // Отправляем в retry очередь
            c.sendToRetryQueue(job)
            msg.Ack(false)
        } else {
            // Отправляем в DLQ
            c.sendToDLQ(job)
            msg.Ack(false)
        }
        return
    }

    // Успех
    markProcessed(job.MessageID)
    msg.Ack(false)
}
```

### Отправка в retry очередь

```go
func (c *Consumer) sendToRetryQueue(job models.JobMessage) error {
    body, _ := json.Marshal(job)
    
    return c.channel.Publish(
        "",            
        c.retryQueue,  // routing key
        true,          
        false,         
        amqp.Publishing{
            ContentType:  "application/json",
            DeliveryMode: amqp.Persistent,
            MessageId:    job.MessageID,
            Body:         body,
            Headers: amqp.Table{
                "x-attempt": job.Attempt,
            },
        })
}
```

### Отправка в DLQ

```go
func (c *Consumer) sendToDLQ(job models.JobMessage) error {
    body, _ := json.Marshal(job)
    
    return c.channel.Publish(
        "",    
        c.dlq, // routing key
        true,
        false,
        amqp.Publishing{
            ContentType:  "application/json",
            DeliveryMode: amqp.Persistent,
            MessageId:    job.MessageID,
            Body:         body,
            Headers: amqp.Table{
                "x-final-attempt": job.Attempt,
                "x-error":         "max_attempts_exceeded",
            },
        })
}
```

---

## Идемпотентность обработки

### Проблема at-least-once доставки

В RabbitMQ при использовании ручных подтверждений (manual ack) возможна ситуация, когда:
1. Worker получил сообщение
2. Обработал его успешно
3. **Упал до отправки ack**
4. RabbitMQ переотправит сообщение другому worker'у

### Решение: хранение обработанных message_id

```go
// Хранилище обработанных ID (в памяти для демо)
var processedIDs = struct {
    sync.RWMutex
    ids map[string]bool
}{ids: make(map[string]bool)}

func isProcessed(messageID string) bool {
    processedIDs.RLock()
    defer processedIDs.RUnlock()
    return processedIDs.ids[messageID]
}

func markProcessed(messageID string) {
    processedIDs.Lock()
    defer processedIDs.Unlock()
    processedIDs.ids[messageID] = true
}
```

### Использование в обработчике

```go
if isProcessed(job.MessageID) {
    logEntry.Info("⏭️ Job already processed, skipping (idempotency)")
    msg.Ack(false)
    return
}

// ... обработка ...

markProcessed(job.MessageID)
```

### Ограничения учебной реализации

- Хранение в памяти (не сохраняется при рестарте worker)
- Нет очистки старых ID
- Для продакшена нужно использовать Redis или БД

---

## Запуск системы

### Предварительные требования

- Docker и Docker Compose
- Git
- Postman (для тестирования)

### Запуск всех сервисов

```bash
# Клонирование репозитория
cd C:\_Projects\ExamplesCluster\MIREA-TIP-Practice-30

# Запуск всех сервисов
cd tech-ip-sem2\deploy
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d

# Проверка статуса
docker-compose ps
```


### Просмотр логов

```bash
# Логи worker (в реальном времени)
docker-compose logs -f worker

# Логи tasks сервиса
docker-compose logs -f tasks

# Все логи сразу
docker-compose logs -f
```

### Переменные окружения

**Для tasks сервиса:**
- `RABBITMQ_URL`: amqp://guest:guest@rabbitmq:5672/
- `RABBITMQ_QUEUE`: task_events (для событий)

**Для worker сервиса:**
- `RABBITMQ_URL`: amqp://guest:guest@rabbitmq:5672/
- `MAIN_QUEUE`: task_jobs
- `RETRY_QUEUE`: task_jobs_retry
- `DLQ`: task_jobs_dlq
- `MAX_ATTEMPTS`: 3
- `RETRY_DELAY_SECONDS`: 10
- `RABBITMQ_PREFETCH`: 1

---

## Скриншоты выполнения

### 1. Запуск всех сервисов через docker-compose

![img.png](about/img.png)

### 2. Логи worker после старта

![img_1.png](about/img_1.png)

### 3. Создание задачи через Postman (успешная)

![img_2.png](about/img_2.png)

### 4. Логи worker при успешной обработке

![img_3.png](about/img_3.png)


### 5. Создание задачи, которая будет ретраиться

![img_4.png](about/img_4.png)

### 6. Логи worker при retry (3 попытки)

![img_5.png](about/img_5.png)


### 7. RabbitMQ Management UI - очереди

![img_6.png](about/img_6.png)

### 8. Содержимое сообщения в DLQ

![img_7.png](about/img_7.png)


---

## Выводы

В ходе выполнения практического занятия №14 были достигнуты следующие результаты:

### Реализованная функциональность

1. **Очередь задач с retry** — при ошибке задача автоматически отправляется в retry очередь с задержкой 10 секунд
2. **Dead Letter Queue (DLQ)** — после 3 неудачных попыток задача попадает в DLQ для ручного анализа
3. **Идемпотентность обработки** — защита от дублей на основе уникального `message_id`
4. **Dead Letter Exchanges (DLX)** — настроена маршрутизация сообщений между очередями
5. **Prefetch** — ограничение на 1 сообщение для контроля нагрузки

### Ключевые особенности реализации

| Компонент | Реализация |
|-----------|------------|
| **Retry policy** | Max 3 попытки, задержка 10 сек через TTL |
| **Dead Letter Queue** | Отдельная очередь для "плохих" сообщений |
| **Идемпотентность** | In-memory хранилище обработанных message_id |
| **At-least-once** | Ручные подтверждения (manual ack) |
| **Трассировка** | Сквозной request-id через заголовки |

### Преимущества подхода с DLX и TTL

1. **Задержка реализуется RabbitMQ** — не нужно хранить таймеры в коде
2. **Надёжность** — даже если worker упадёт, сообщение вернётся в очередь через TTL
3. **Масштабирование** — retry очередь могут читать несколько worker'ов
4. **Отсутствие потерь** — все сообщения либо обработаны, либо в DLQ

### Демонстрация работы

- ✅ **Успешная обработка** — задача выполняется, message_id запоминается
- 🔄 **Retry** — при ошибке задача возвращается в очередь через 10 сек (3 попытки)
- 💀 **DLQ** — после 3 попыток задача отправляется в мёртвую очередь
- ⏭️ **Идемпотентность** — повторные сообщения с тем же ID игнорируются

### Возможные улучшения

1. **Постоянное хранилище** для processed_ids (Redis/PostgreSQL)
2. **Экспоненциальная задержка** вместо фиксированной
3. **Метрики** количества задач в очередях (Prometheus)
4. **Dead Letter Queue с анализом** — автоматическое перемещение обратно при исправлении ошибки

---

## Контрольные вопросы

### 1. Чем "job queue" отличается от "event queue"?

| Характеристика | Event Queue | Job Queue |
|----------------|-------------|-----------|
| **Цель** | Уведомление о событии | Выполнение работы |
| **Обработка** | Лёгкая, быстрая | Тяжёлая, может быть долгой |
| **Retry** | Обычно не требуется | Обязателен при ошибках |
| **Идемпотентность** | Не критично | Критично (могут быть повторы) |
| **Пример** | "Задача создана" | "Сгенерировать отчёт по задаче" |

В **event queue** важна скорость и уведомление, в **job queue** — гарантия выполнения и устойчивость к ошибкам.

### 2. Почему система очередей часто работает как at-least-once?

**At-least-once** означает, что сообщение будет доставлено как минимум один раз, но возможны повторы.

Это происходит из-за подтверждений (ack):
1. Worker получает сообщение
2. Worker обрабатывает его успешно
3. Worker **падает до отправки ack**
4. RabbitMQ считает, что сообщение не обработано, и переотправляет его другому worker'у

**Почему так сделано?** Это компромисс между надёжностью и производительностью. Гарантия "exactly-once" требует распределённых транзакций, что сильно снижает производительность.

### 3. Как DLQ помогает эксплуатации?

**Dead Letter Queue (DLQ)** — очередь для сообщений, которые не удалось обработать.

Преимущества DLQ:

1. **Не теряются данные** — "плохие" сообщения не исчезают, а сохраняются
2. **Не блокируют основную очередь** — проблема с одним сообщением не останавливает обработку других
3. **Возможность анализа** — можно посмотреть, почему сообщение не обработалось
4. **Ручная переобработка** — после исправления ошибки можно вернуть сообщение из DLQ
5. **Мониторинг** — рост DLQ сигнализирует о проблемах

### 4. Почему ретраи нельзя делать бесконечно?

Бесконечные ретраи приводят к проблемам:

1. **Циклические ошибки** — если ошибка постоянная (например, невалидные данные), сообщение будет вечно ходить по кругу
2. **Загрузка системы** — каждое повторение потребляет ресурсы CPU и сети
3. **Маскировка проблем** — ошибка может сигнализировать о серьёзной проблеме, которую нужно решать, а не ретраить
4. **Задержка новых задач** — старые задачи занимают место в очереди

**Решение:** ограниченное число попыток (обычно 3-5), после которых сообщение отправляется в DLQ.

### 5. Что такое идемпотентность и как её реализовать минимально?

**Идемпотентность** — свойство операции, при котором многократное применение даёт тот же результат, что и однократное.

В контексте очередей: если одно и то же сообщение пришло дважды (из-за at-least-once), система должна обработать его только один раз.

**Минимальная реализация:**

1. **Уникальный идентификатор** — каждое сообщение получает `message_id` (UUID)
2. **Хранилище обработанных ID** — сохраняем ID успешно обработанных сообщений
3. **Проверка перед обработкой** — если ID уже есть, пропускаем

```go
if isProcessed(msg.MessageID) {
    // уже обработано, просто подтверждаем
    msg.Ack(false)
    return
}

// обработка...
markProcessed(msg.MessageID)
msg.Ack(false)
```
