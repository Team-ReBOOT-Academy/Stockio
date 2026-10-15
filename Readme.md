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
│   ├── config/
│   │   └── config.go
│   ├── contracts/
│   │   ├── candle.go
│   │   ├── envelope.go
│   │   ├── heartbeat.go
│   │   ├── recommendation.go
│   │   └── registration.go
│   ├── httpapi/
│   │   └── health.go
│   └── strategy/
├── docs/
│   └── project-description.md
├── .env.example
├── .gitignore
├── Dockerfile
├── README.md
└── go.mod
```
