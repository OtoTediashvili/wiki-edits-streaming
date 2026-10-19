.DEFAULT_GOAL := help
SHELL := /bin/bash

FIXTURE := tests/fixtures/recentchange_sample.sse
TOPIC   := wiki.recentchange
RPK     := docker compose exec -T redpanda rpk

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

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

test: ## Unit tests with coverage (no services; the Spark ones need Java 17+)
	uv run pytest -m "not integration and not e2e"

test-integration: ## Round trip through the real Redpanda (needs make up)
	uv run pytest -m integration --no-cov

test-e2e: ## Recording -> Redpanda -> Spark -> Iceberg, checked with PyIceberg (needs make up)
	docker compose build spark
	uv run pytest -m e2e --no-cov

check: lint typecheck test ## Everything CI's quality job runs

# ------------------------------------------------------------------ stack

up: ## Start Redpanda, Console and SeaweedFS (S3 + Iceberg catalog)
	docker compose up -d --wait redpanda console seaweedfs
	@echo "Console:          http://localhost:8090"
	@echo "SeaweedFS admin:  http://localhost:12646"
	@echo "Kafka: localhost:19092   Schema registry: http://localhost:18081"
	@echo "Iceberg catalog: http://localhost:8181   S3: http://localhost:8333"

down: ## Stop everything (topics, tables, checkpoints and resume position are kept)
	docker compose down

reset: ## Stop everything and delete ALL data: topics, tables, checkpoints, position
	docker compose down --volumes
	rm -rf data/state data/checkpoints

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

# ------------------------------------------------------------------ stream

stream: ## Run the Spark job: Redpanda into Iceberg (builds the image; UI on :4040)
	docker compose up -d --build spark
	@echo "Spark job started. Follow it with: make stream-logs   Spark UI: http://localhost:4040"

stream-stop: ## Stop the Spark job (it resumes from its checkpoints next time)
	docker compose stop spark

stream-logs: ## Follow the Spark job's logs
	docker compose logs -f spark

peek: ## Table sizes and the newest minutes, read from Iceberg
	uv run python -m wikistream.streaming.peek

maintain: ## Compact the days that ended and expire old snapshots, now (the job also does it hourly)
	docker compose run --rm -T spark maintain

lake-reset: ## Delete the Iceberg tables and the job's checkpoint; keeps Redpanda's topics
	docker compose rm --stop --force spark seaweedfs
	docker volume rm --force wiki-edits-streaming_seaweedfs-data wiki-edits-streaming_spark-checkpoints
	@echo "Lake emptied. make up, then make stream rebuilds it from what Redpanda holds (48 h)."

# ------------------------------------------------------------------ dashboard

dashboard: ## Start the live dashboard on http://localhost:8501 (builds the image)
	docker compose up -d --build --wait dashboard
	@echo "Dashboard: http://localhost:8501"

dashboard-stop: ## Stop the dashboard
	docker compose stop dashboard

dashboard-logs: ## Follow the dashboard's logs
	docker compose logs -f dashboard

dashboard-dev: ## Run the dashboard from this checkout; reloads on save (needs make up)
	uv run python -m wikistream.dashboard --server.address 127.0.0.1 --server.runOnSave true

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

.PHONY: help install fmt lint typecheck test test-integration test-e2e check up down reset ps \
	produce stop logs replay stream stream-stop stream-logs peek maintain lake-reset \
	dashboard dashboard-stop dashboard-logs dashboard-dev topics consume partitions clean
