# Stockio

Учебный серверный сервис торговой стратегии.

## Автор

**Max Burenko**

## Назначение

Stockio предназначен для учебной проверки алгоритма торговой стратегии на рыночных данных.

Сервис не подключается к реальной бирже и не совершает реальные торговые операции.

В качестве предварительной торговой стратегии используется **Momentum**.

Стратегия сравнивает текущую цену закрытия с ценой закрытия `N` свечей назад:

```text
momentum = close[t] / close[t-N] - 1
```

В зависимости от полученного значения формируется:

* `BUY`;
* `SELL`;
* `HOLD`.

## Документация

Подробное описание проекта находится в:

[`docs/project-description.md`](docs/project-description.md)

Документ содержит:

* назначение проекта;
* цели и задачи;
* границы системы;
* внешние системы;
* компоненты;
* роли;
* используемые инструменты;
* допущения и открытые вопросы.

## Требования

Для локального запуска необходимы:

* Go 1.24 или новее;
* Git.

Для запуска в контейнере:

* Docker.

## Конфигурация

Основные параметры задаются через переменные окружения:

```text
APP_NAME
APP_VERSION
HTTP_PORT
KAFKA_BOOTSTRAP_SERVERS
KAFKA_USERNAME
KAFKA_PASSWORD
KAFKA_GROUP_ID
CANDLE_TOPIC
SECRET_KEY
ACCOUNT_ID
ACCOUNT_NAME
GROUP_ID
STRATEGY_ID
STRATEGY_NAME
SERVICE_NAME
```

Пример конфигурации находится в:

```text
.env.example
```

Секретные значения не должны храниться в Git.

## Сборка

Собрать проект без среды разработки:

```bash
go build ./...
```

## Тестирование

Запустить все тесты:

```bash
go test ./...
```

Проверить форматирование:

```bash
go fmt ./...
```

## Локальный запуск

Запустить сервер:

```bash
go run ./cmd/server
```

По умолчанию сервер работает на:

```text
http://localhost:8080
```

## Проверка состояния

Сервис предоставляет endpoint:

```http
GET /health
```

Пример:

```bash
curl http://localhost:8080/health
```

Ответ:

```json
{
  "status": "UP",
  "service": "momentum-strategy",
  "version": "1.0.0",
  "components": {}
}
```

Endpoint не требует авторизации.

## Docker

Собрать Docker-образ:

```bash
docker build -t stockio -f deploy/Dockerfile .
```

Запустить:

```bash
docker run --rm -p 8080:8080 stockio
```

Перед подключением к учебной платформе передайте значения окружения через защищенное хранилище или локальный `.env` (не коммитьте его):

```bash
docker run --rm --env-file .env -p 8080:8080 stockio
```

После запуска проверить:

```bash
curl http://localhost:8080/health
```

## Структура проекта

```text
Stockio/
├── cmd/
│   └── server/
│       └── main.go
├── internal/
│   ├── infrastructure/
│   │   ├── config/
│   │   └── kafka/
│   ├── contracts/
│   │   ├── candle.go
│   │   ├── envelope.go
│   │   ├── heartbeat.go
│   │   ├── recommendation.go
│   │   └── registration.go
│   ├── domain/
│   │   └── strategy/
│   └── interfaces/
│       └── httpapi/
├── docs/
│   ├── diagrams/
│   └── project-description.md
├── .env.example
├── .gitignore
├── deploy/Dockerfile
├── README.md
└── go.mod
```

## Внешние взаимодействия

Сервис подключается к выданному преподавателем Kafka broker (`SASL_SSL` / `SCRAM-SHA-256`), регистрирует стабильные идентификаторы, читает `market.candle.closed`, публикует `strategy.recommendation.created` и отправляет heartbeat каждые пять секунд после регистрации. В первом интеграционном этапе ответ стратегии — `HOLD`. Уникальный `KAFKA_GROUP_ID` нужен каждому сервису, а `CANDLE_TOPIC` и секреты предоставляются преподавателем.

## Архитектурные схемы

* Контекст: [`docs/diagrams/context.puml`](docs/diagrams/context.puml)
* Компоненты: [`docs/diagrams/components.mmd`](docs/diagrams/components.mmd)

## План работ

План работ следует вести в выбранной системе управления задачами и добавить сюда ссылку после создания проекта. Минимальные задачи: подключение Kafka; регистрация; прием и логирование свечи; публикация `HOLD`; последующая реализация Momentum. Для каждой задачи указываются ожидаемый результат и проверяемые критерии готовности. Ссылка на внешний проект задач является открытым пунктом, поскольку конкретный проект не предоставлен.
