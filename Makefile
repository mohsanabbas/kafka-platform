COMPOSE := docker compose -f infra/compose.yaml
BROKER ?= 3

.DEFAULT_GOAL := help
.PHONY: help up down reset status logs build test lint broker-stop broker-start add-broker

help: ## list targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-z-]+:.*## / {printf "  %-13s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: build ## build and start the whole lab
	$(COMPOSE) up -d --build
	@echo
	@echo "  Kafka UI           http://localhost:8080"
	@echo "  Cruise Control UI  http://localhost:8081"
	@echo "  opsd API           http://localhost:8090/v1/cluster/health"
	@echo "  CLI                bin/labctl health"
	@echo
	@echo "  Cruise Control needs about 5 minutes of metrics before proposals are ready."

down: ## stop the lab and keep images
	$(COMPOSE) --profile add-broker down

reset: ## stop the lab and delete volumes
	$(COMPOSE) --profile add-broker down -v --remove-orphans
	rm -rf bin

status: ## containers and cluster health
	$(COMPOSE) --profile add-broker ps
	@bin/labctl health || true

logs: ## follow logs, SERVICE=opsd for one service
	$(COMPOSE) logs -f $(SERVICE)

build: ## build labctl and opsd into bin/
	go build -o bin/ ./cmd/...

test: ## vet and unit tests
	go vet ./...
	go test ./...

lint: ## golangci-lint; install with: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	golangci-lint run ./...

broker-stop: ## stop a broker, BROKER=3 by default
	$(COMPOSE) stop kafka-$(BROKER)

broker-start: ## start a stopped broker, BROKER=3 by default
	$(COMPOSE) start kafka-$(BROKER)

add-broker: ## start kafka-4 for the scale-out drill
	$(COMPOSE) --profile add-broker up -d kafka-4
