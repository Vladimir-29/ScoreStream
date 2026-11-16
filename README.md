# ScoreStream

Учебный backend-проект для обработки результатов участников и построения рейтингов в режиме реального времени.

## О проекте

ScoreStream принимает события о начислении очков через REST API, публикует их в Kafka, а Worker асинхронно сохраняет события и текущие результаты в PostgreSQL. Повторная доставка одного и того же события не приводит к повторному начислению очков (идемпотентная обработка).

Основная архитектурная идея — разделение приёма событий, их обработки и хранения результатов.

## Реализованный стек

- **Go** — backend-разработка.
- **REST API** — приём событий `POST /api/v1/scores` и healthcheck `GET /healthz`.
- **Apache Kafka** — асинхронная доставка событий (топик `score-events`).
- **PostgreSQL** — хранение истории событий (`score_events`) и текущих сумм очков (`leaderboard_scores`).
- **Docker Compose** — локальное окружение (Kafka + PostgreSQL).

Планируются: Redis (рейтинги), gRPC, Protocol Buffers, GitHub Actions.

## Быстрый старт

### 1. Запуск инфраструктуры

```bash
docker compose up -d
```

Поднимет Kafka (`localhost:9092`) и PostgreSQL (`localhost:5432`, БД `scorestream`, пользователь и пароль `scorestream`).

### 2. Применение миграции

```bash
docker exec -i scorestream-postgres psql -U scorestream -d scorestream \
  < migrations/001_create_score_tables.sql
```

### 3. Запуск сервисов

```bash
go run ./cmd/api      # REST API на :8080
go run ./cmd/worker   # Consumer Kafka -> PostgreSQL
```

### 4. Отправка события

```bash
curl -X POST http://localhost:8080/api/v1/scores \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"event-001","leaderboard_id":"weekly","user_id":"user-1","points":100}'
```

## Конфигурация

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Адрес REST API |
| `DATABASE_URL` | `postgres://scorestream:scorestream@localhost:5432/scorestream?sslmode=disable` | Подключение к PostgreSQL (только Worker) |
| `KAFKA_BROKERS` | `localhost:9092` | Список брокеров Kafka через запятую |
| `KAFKA_TOPIC` | `score-events` | Топик событий |
| `KAFKA_GROUP_ID` | `scorestream-workers` | Consumer Group Worker |

## Тестирование

```bash
go test ./...
```

Интеграционные тесты PostgreSQL (`internal/postgres`) требуют реальной базы и подключаются через `TEST_DATABASE_URL`:

```bash
export TEST_DATABASE_URL='postgres://scorestream:scorestream@localhost:5432/scorestream?sslmode=disable'
go test ./internal/postgres/ -v
```

Без `TEST_DATABASE_URL` эти тесты пропускаются, остальные тесты работают без внешних зависимостей.

## Обработка событий

Worker обрабатывает сообщения последовательно. Для каждого события в одной транзакции PostgreSQL:

1. Событие вставляется в `score_events` с `ON CONFLICT DO NOTHING` — `event_id` гарантирует идемпотентность.
2. Для нового события выполняется UPSERT в `leaderboard_scores` с `RETURNING total_score`.
3. Для дубликата (тот же `event_id` и содержимое) счёт не изменяется.
4. Тот же `event_id` с другим содержимым отклоняется с `ErrEventIDConflict`.

Kafka offset подтверждается только после успешного коммита транзакции. Некорректные сообщения и конфликты `event_id` логируются и пропускаются; инфраструктурные ошибки завершают Worker без подтверждения offset.

## Статус

Этап 2 завершён: REST API → Kafka → Worker → PostgreSQL с идемпотентной обработкой событий.
