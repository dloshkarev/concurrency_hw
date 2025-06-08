# Переменные
VERSION ?= 0.0.1
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
GO_LDFLAGS ?= -X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME)
# Путь к конфигу
CONFIG_PATH ?= $(shell pwd)/config/config.yaml
TEST_CONFIG_PATH ?= $(shell pwd)/config/config-master-test.yaml

.PHONY: all build clean test run lint vendor help

## fmt: Выполнить форматирование всех файлов
fmt:
	@go fmt ./...

## build: Собрать бинарный файл tcp-сервера
buildServer:
	@go build -ldflags "$(GO_LDFLAGS)" -o server ./cmd/server

## runServer: Запустить приложение tcp-сервера
runServer:
	@echo "Config path: $(CONFIG_PATH)"
	@CONDB_CONFIG_PATH=$(CONFIG_PATH) ./server

## build: Собрать бинарный файл клиента
buildClient:
	@go build -ldflags "$(GO_LDFLAGS)" -o client ./cmd/client

## runClient: Запустить приложение клиента
runClient:
	./client --address=localhost:3223

## test: Запустить Unit-тесты
test-unit:
	export CONDB_CONFIG_PATH=$(TEST_CONFIG_PATH) && \
	go test -race -tags=unit -v ./...

## test: Запустить интеграционные тесты
test-integration:
	export CONDB_CONFIG_PATH=$(TEST_CONFIG_PATH) && \
	go test -race -tags=integration -v ./...

## test-e2e: Запустить e2e-тест
test-e2e:
	export CONDB_CONFIG_PATH=$(TEST_CONFIG_PATH) && \
	go test -tags=e2e -v ./...

## test-cover: Запустить тесты с покрытием
test-cover:
	@go test -v -race -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html

## lint: Проверить код линтером
lint:
	@golangci-lint run ./...

## help: Показать справку
help:
	@echo "Доступные команды:"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'