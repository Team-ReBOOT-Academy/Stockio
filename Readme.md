# Stockio

Stockio — учебный Go-сервис, который подключается к платформе через Kafka. Сейчас он проходит регистрацию, отправляет heartbeat примерно раз в 5 секунд, читает закрытые свечи и публикует одну рекомендацию `HOLD` на каждую полученную свечу. Стратегия Momentum пока не реализована. Реальные торговые заявки сервис не отправляет.

Автор: **Max Burenko**. Подробности о границах и устройстве системы — в [описании проекта](docs/project-description.md). Диаграммы: [компоненты](docs/diagrams/components.mmd) и [контекст](docs/diagrams/context.puml).

## Требования

- Go 1.23 или новее для локального запуска;
- Docker и Docker Compose для контейнерного запуска;
- выданная преподавателем Kafka-пара, `CANDLE_TOPIC` и учебный `GROUP_ID`.

## Настройка

Скопируйте `.env.example` в `.env` и заполните значения из платформы и от преподавателя. Не коммитьте `.env`: он исключен из Git и Docker build context.

`KAFKA_USERNAME` — ключ/логин Kafka, `KAFKA_PASSWORD` — секрет/пароль Kafka. В `ACCOUNT_SECRET_KEY` указывается именно ключ, совпадающий с логином, а не пароль. `GROUP_ID` — учебная группа; `KAFKA_GROUP_ID` — отдельный уникальный технический Kafka group ID. У каждого студента должен быть свой `KAFKA_GROUP_ID`.

`ACCOUNT_ID`, `ACCOUNT_NAME`, `GROUP_ID`, `STRATEGY_ID`, `STRATEGY_NAME` и `SERVICE_NAME` попадают в запрос регистрации. Сохраняйте идентификаторы при следующих запусках. `CANDLE_TOPIC` нужно согласовать с преподавателем: он должен соответствовать потоку, назначенному портфелю платформой. Изменение подписки сервиса само по себе не меняет тему портфеля.

Настройки Kafka по умолчанию: брокер `omsu.7bits.it:9094`, `SASL_SSL`, `SCRAM-SHA-256`. Названия служебных тем имеют значения по умолчанию в коде; их можно переопределить переменными из `.env.example`.

## Запуск локально

В zsh загрузите `.env` в окружение и запустите сервис:

```bash
set -a
source .env
set +a
go run ./cmd/server
```

Для проверки компиляции и запуска:

```bash
go build ./...
go run ./cmd/server
```

## Запуск в Docker

В корне проекта:

```bash
docker compose up --build
```

Compose пересобирает образ, передает переменные из `.env` контейнеру и показывает его логи. Для отдельной сборки:

```bash
docker build -t stockio-momentum-strategy -f Dockerfile .
docker run --rm --env-file .env -p 8080:8080 stockio-momentum-strategy
```

Остановить Compose можно через Ctrl+C, затем выполнить `docker compose down`.

## Регистрация и обмен Kafka

1. Клиент подписывается на свечи и темы результатов и ждёт назначения раздела `account.registration.result` (до 30 секунд).
2. Он публикует `account.registration.request` и ждёт свой ответ `REGISTERED`, связанный с запросом через `causationId`.
3. После подтверждения сервис публикует heartbeat в `account.service.heartbeat` примерно раз в 5 секунд.
4. Для события `market.candle.closed` из `CANDLE_TOPIC` сервис публикует `strategy.recommendation.created` с `HOLD` и исходными `runId`, тикером, `sequenceNumber` и идентификатором свечи.
5. Сервис также читает собственные `account.registration.result` и `strategy.recommendation.result`.

Сообщения используют JSON-конверт версии `1.0`. Kafka-пароль передается только при подключении и не включается в события. В логах успешная обработка свечи выглядит как `published HOLD recommendation` с тикером и номером свечи. Подробные цены свечи текущая версия в лог не выводит.

Чтобы следить за логами:

```bash
docker compose logs -f momentum-strategy
```

Если регистрации достаточно, но нет сообщений `published HOLD recommendation`, проверьте, идет ли активный прогон на платформе и совпадает ли `CANDLE_TOPIC` с темой, которую преподаватель назначил портфелю.

## HTTP health

Сервис слушает `HTTP_PORT` (по умолчанию `:8080`). Проверка доступна без авторизации:

```bash
curl http://localhost:8080/health
```

Endpoint возвращает JSON со статусом процесса, именем сервиса, версией и компонентами. Это базовая проверка HTTP-доступности; статус Kafka не является активной проверкой соединения. В текущей заглушке `database` отмечена как `DOWN`, потому что база данных не используется.

## Структура проекта

```text
cmd/server/                    точка сборки и запуска приложения
internal/config/               загрузка и проверка переменных окружения
internal/domain/                конверты и модели событий платформы
internal/ports/                 интерфейсы публикации и чтения Kafka
internal/usecases/              регистрация, heartbeat, обработка свечей
internal/adapters/kafka/         Kafka producer и consumer на franz-go
internal/adapters/http/          HTTP endpoint /health
docs/project-description.md      актуальное описание архитектуры и поведения
docs/diagrams/                  контекстная и компонентная диаграммы
```
