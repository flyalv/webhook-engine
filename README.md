# Webhook Engine

**A reliable webhook delivery service built with Go and PostgreSQL.**

[English](#english) | [Русский](#русский)

---

<a id="english"></a>

## English

Webhook Engine is an asynchronous webhook delivery service built with Go and PostgreSQL. It accepts events through an HTTP API, stores deliveries in a durable queue, and delivers them to destination endpoints with automatic retries.

### Features

- Durable PostgreSQL-backed delivery queue
- Concurrent job claiming with `FOR UPDATE SKIP LOCKED`
- Exponential backoff with jitter
- Delivery attempt history
- Lease-based recovery of expired jobs
- Graceful shutdown
- SSRF-aware outbound HTTP client
- Integration tests for delivery, retries, and concurrency

### Tech Stack

Go · Gin · PostgreSQL · pgx · sqlc · Docker Compose

### Quick Start

```bash
docker compose up -d --build
curl http://localhost:8080/health
```

Submit an event:

```bash
curl -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{
    "event_type": "order.created",
    "payload": {"order_id": "order-123"},
    "destination_url": "http://webhook-receiver:8081/webhook"
  }'
```

The API returns `202 Accepted`; delivery happens asynchronously.

### Delivery Semantics

The service provides **at-least-once delivery**, so duplicate deliveries are possible. Failed requests are retried with exponential backoff and jitter. After five unsuccessful attempts, a delivery is marked `dead`.

### Testing

```bash
go test ./... -count=1
```

See the Russian section below for the full description.

---

<a id="русский"></a>

## Русский

Webhook Engine — сервис асинхронной доставки вебхуков на Go и PostgreSQL. Он принимает события через HTTP API, сохраняет доставки в надёжной очереди и отправляет их на указанные адреса с автоматическими повторными попытками.

### Возможности

- Очередь доставок на PostgreSQL
- Конкурентный захват задач через `FOR UPDATE SKIP LOCKED`
- Экспоненциальный backoff с jitter
- История попыток доставки
- Восстановление задач с истёкшей арендой (lease)
- Корректное завершение сервера и воркера
- HTTP-клиент с защитой от SSRF
- Интеграционные тесты доставки, повторных попыток и конкурентного захвата задач

### Технологии

Go · Gin · PostgreSQL · pgx · sqlc · Docker Compose

### Быстрый запуск

```bash
docker compose up -d --build
curl http://localhost:8080/health
```

Отправить событие:

```bash
curl -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{
    "event_type": "order.created",
    "payload": {"order_id": "order-123"},
    "destination_url": "http://webhook-receiver:8081/webhook"
  }'
```

API возвращает `202 Accepted`, после чего воркер доставляет событие асинхронно.

### Гарантии доставки

Сервис реализует семантику **at-least-once**: одно событие может быть доставлено несколько раз. При неудаче выполняются повторные попытки с экспоненциальной задержкой и jitter. После пяти неудачных попыток доставка получает статус `dead`.

### Тестирование

```bash
go test ./... -count=1
```
