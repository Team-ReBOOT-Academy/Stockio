# Stockio

Учебный серверный сервис для проверки торговой стратегии на потоке рыночных свечей. На текущем этапе Stockio регистрируется на платформе, принимает свечи и отправляет по одной безопасной рекомендации `HOLD` на каждую свечу. Реальные торговые заявки не отправляются. Momentum — предварительно выбранная стратегия для последующей работы, она пока не реализована.

## Автор и описание проекта

Автор: **Max Burenko**.

Границы сервиса, внешние взаимодействия, роли, допущения и инструменты описаны в [документе проекта](docs/project-description.md).

## Требования

- Go 1.23 или новее;
- доступ к учебному Kafka broker;
- Docker — только для контейнерного запуска.

## Настройка

Скопируйте `.env.example` в `.env` и задайте выданные преподавателем значения. `KAFKA_USERNAME` — логин Kafka, `KAFKA_PASSWORD` — пароль Kafka; `SECRET_KEY` — ключ регистрации из той же пары, а не пароль. `GROUP_ID` — учебная группа в сообщениях, `KAFKA_GROUP_ID` — отдельное уникальное имя Kafka consumer group. Не коммитьте `.env`.

Укажите стабильные `ACCOUNT_ID`, `STRATEGY_ID`, `SERVICE_NAME`, а также согласованный `CANDLE_TOPIC`. Имена топиков протокола уже имеют значения по умолчанию. Сервис подключается по `SASL_SSL` и `SCRAM-SHA-256` к `omsu.7bits.it:9094`.

В zsh загрузите настройки и запустите приложение:

```bash
set -a
source .env
set +a
go run ./cmd/server
```

Имена и описания с пробелами в `.env` заключайте в кавычки, например `ACCOUNT_NAME="Имя Фамилия"` и `STRATEGY_NAME="Stockio Momentum"`. Файл `.env` — shell-совместимый файл для приведенной команды загрузки.

## Сборка и запуск

```bash
go build ./...
go run ./cmd/server
```

Сервер проверки состояния слушает `HTTP_ADDR` (по умолчанию `:8080`). Для проверки:

```bash
curl http://localhost:8080/health
```

`/health` не требует авторизации и возвращает `status`, `service`, `version` и объект `components`. Недоступность Kafka отражается как `DEGRADED`; временный сбой брокера не должен завершать процесс.

При остановке через Ctrl+C приложение корректно завершает HTTP сервер и Kafka клиент.

## Docker

```bash
docker build -t stockio -f deploy/Dockerfile .
docker run --rm --env-file .env -p 8080:8080 stockio
```

## Kafka обмен

После назначения раздела темы `account.registration.result` приложение отправляет `account.registration.request` и ждёт собственное подтверждение `REGISTERED`. Затем оно отправляет heartbeat примерно раз в 5 секунд, читает `market.candle.closed` из `CANDLE_TOPIC`, публикует `strategy.recommendation.created` со значением `HOLD` и читает `strategy.recommendation.result`. Рекомендация содержит идентификаторы, `runId`, тикер, номер среза и ссылку на исходную свечу. Идентификаторы сервиса должны сохраняться между запусками.

Подробные примеры протокола приведены в документации платформы; настройки выдаёт преподаватель. План задач ведётся в системе управления задачами курса — ссылка не включена, так как адрес проекта в этой среде не предоставлен.

## Структура

```text
cmd/server/             запуск процесса и сборка компонентов
internal/config/        загрузка окружения
internal/health/        HTTP endpoint и health статусы
internal/kafka/         TLS/SCRAM клиент и consumer group
internal/messaging/     Kafka envelope и payload типы
internal/strategy/      регистрация, heartbeat, HOLD и обработка результатов
docs/project-description.md
docs/diagrams/
```
