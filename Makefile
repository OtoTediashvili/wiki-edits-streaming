.DEFAULT_GOAL := help
SHELL := /bin/bash

FIXTURE := tests/fixtures/recentchange_sample.sse
TOPIC   := wiki.recentchange
RPK     := docker compose exec -T redpanda rpk

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ------------------------------------------------------------------ develop

install: ## Create .venv from the lockfile and install the git hooks
	uv sync --locked
	uv run pre-commit install

fmt: ## Auto-format and auto-fix
	uv run ruff format . && uv run ruff check --fix .

lint: ## Lint without fixing
	uv run ruff check . && uv run ruff format --check .

typecheck: ## Static type check (strict)
	uv run mypy

test: ## Unit tests with coverage (no services needed)
	uv run pytest -m "not integration"

test-integration: ## Round trip through the real Redpanda (needs make up)
	uv run pytest -m integration --no-cov

check: lint typecheck test ## Everything CI's quality job runs

# ------------------------------------------------------------------ stack

up: ## Start Redpanda and Console (http://localhost:8090)
	docker compose up -d --wait redpanda console
	@echo "Console: http://localhost:8090   Kafka: localhost:19092   Schema registry: http://localhost:18081"

down: ## Stop everything (topics and resume position are kept)
	docker compose down

reset: ## Stop everything and delete all topic data and the resume position
	docker compose down --volumes
	rm -rf data/state

ps: ## What is running
	docker compose ps

# ------------------------------------------------------------------ produce

produce: ## Stream live edits into Redpanda (builds the producer image)
	@grep -qE '^WIKISTREAM_CONTACT=.+' .env 2>/dev/null \
		|| { echo "Set WIKISTREAM_CONTACT in .env first: cp .env.example .env, then edit it."; exit 1; }
	docker compose up -d --build producer
	@echo "Producer started. Follow it with: make logs"

stop: ## Stop the producer (it confirms delivery and saves its position first)
	docker compose stop producer

logs: ## Follow the producer's logs
	docker compose logs -f producer

replay: ## Send the recorded 95-event capture into Redpanda (from this machine)
	uv run wikistream replay $(FIXTURE)

# ------------------------------------------------------------------ inspect

topics: ## List topics with partitions and retention
	$(RPK) topic list --detailed
	$(RPK) topic describe $(TOPIC) --print-configs | grep -E 'retention|cleanup|compression'

consume: ## Print the newest 5 messages, decoded through the schema registry
	$(RPK) topic consume $(TOPIC) --offset -5 --num 5 --use-schema-registry=value \
		--format '%p/%o  key=%k\n%v\n\n'

partitions: ## Messages per partition: offsets and leaders
	$(RPK) topic describe $(TOPIC) --print-partitions

clean: ## Remove local caches and the venv
	rm -rf .venv .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov coverage.xml

.PHONY: help install fmt lint typecheck test test-integration check up down reset ps \
	produce stop logs replay topics consume partitions clean
