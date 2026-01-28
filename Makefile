.PHONY: help pull test build version deploy status restart clean check

# Default values
SERVICE ?= um-ingest.service
INSTALL_DIR ?= /opt/um-ingest-server
BINARY ?= um-ingest-server
HOST ?= http://localhost:8081

# Version variables
VERSION ?= $(shell git describe --tags --always 2>/dev/null || echo "dev")
GIT_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

# LDFLAGS for version injection
LDFLAGS = -X 'github.com/ryabkov82/um-ingest-server/internal/version.Version=$(VERSION)' \
          -X 'github.com/ryabkov82/um-ingest-server/internal/version.GitCommit=$(GIT_COMMIT)' \
          -X 'github.com/ryabkov82/um-ingest-server/internal/version.BuildTime=$(BUILD_TIME)'

help: ## Показать справку по доступным целям
	@echo "Доступные цели:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'
	@echo ""
	@echo "Переменные:"
	@echo "  SERVICE=$(SERVICE)"
	@echo "  INSTALL_DIR=$(INSTALL_DIR)"
	@echo "  BINARY=$(BINARY)"
	@echo "  HOST=$(HOST)"
	@echo "  VERSION=$(VERSION)"
	@echo "  GIT_COMMIT=$(GIT_COMMIT)"
	@echo "  BUILD_TIME=$(BUILD_TIME)"

pull: ## Выполнить git pull
	@echo "Выполняю git pull..."
	git pull

test: ## Запустить тесты
	@echo "Запускаю тесты..."
	go test ./...

build: ## Собрать бинарник с версионированием
	@echo "Собираю бинарник..."
	@echo "  VERSION=$(VERSION)"
	@echo "  GIT_COMMIT=$(GIT_COMMIT)"
	@echo "  BUILD_TIME=$(BUILD_TIME)"
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/server
	@echo "Бинарник собран: ./$(BINARY)"

version: build ## Показать версию собранного бинарника
	@echo "Версия бинарника:"
	@./$(BINARY) --version
	@echo ""
	@echo "Детальная информация о сборке:"
	@./$(BINARY) --build-info

deploy: test build ## Развернуть сервис (test -> build -> stop -> install -> start -> status)
	@echo "Начинаю деплой..."
	@echo "  SERVICE=$(SERVICE)"
	@echo "  INSTALL_DIR=$(INSTALL_DIR)"
	@echo "  BINARY=$(BINARY)"
	@echo ""
	@echo "1. Останавливаю сервис..."
	@sudo systemctl stop $(SERVICE) || echo "  Сервис не был запущен"
	@echo ""
	@echo "2. Устанавливаю бинарник..."
	@sudo install -o um-ingest -g um-ingest -m 0755 ./$(BINARY) $(INSTALL_DIR)/$(BINARY)
	@echo ""
	@echo "3. Запускаю сервис..."
	@sudo systemctl start $(SERVICE)
	@echo ""
	@echo "4. Проверяю статус сервиса..."
	@sudo systemctl status $(SERVICE) --no-pager -l || true
	@echo ""
	@echo "5. Проверяю доступность сервиса (best-effort)..."
	@$(MAKE) check || echo "  Проверка недоступна (сервис может ещё запускаться)"

status: ## Показать статус сервиса
	@sudo systemctl status $(SERVICE) --no-pager -l

restart: ## Перезапустить сервис
	@echo "Перезапускаю сервис $(SERVICE)..."
	@sudo systemctl restart $(SERVICE)
	@sudo systemctl status $(SERVICE) --no-pager -l || true

clean: ## Удалить локальный бинарник
	@echo "Удаляю бинарник ./$(BINARY)..."
	@rm -f ./$(BINARY)
	@echo "Готово"

check: ## Проверить доступность сервиса через /version
	@echo "Проверяю доступность сервиса на $(HOST)/version..."
	@curl -s $(HOST)/version | jq '.' || curl -s $(HOST)/version
	@echo ""

