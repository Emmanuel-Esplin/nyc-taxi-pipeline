# Usage: make run MONTH=2025-01 | make backfill START=2025-01 END=2025-03 | make test
MONTH ?= 2025-01
START ?= 2025-01
END   ?= 2025-03
IMAGE ?= nyc-taxi-pipeline
PYTHON ?= .venv/bin/python
export PYTHONPATH := $(CURDIR)/src
# dbt reads the same paths the pipeline uses; artifacts and logs go under data/dbt/.
DBT = PIPELINE_DATA_DIR="$(CURDIR)/data" PIPELINE_WAREHOUSE_PATH="$(CURDIR)/data/warehouse.duckdb" \
	.venv/bin/dbt
DBT_ARGS = --project-dir dbt --profiles-dir dbt \
	--target-path "$(CURDIR)/data/dbt/target" --log-path "$(CURDIR)/data/dbt/logs"
DOCKER_RUN = docker run --rm -v "$(CURDIR)/data:/app/data" $(DOCKER_ENV) $(IMAGE)

.PHONY: help install lint format test run backfill dbt-build docs docker-build docker-run docker-backfill clean

help:
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-16s %s\n", $$1, $$2}'

install: ## Create .venv and install the package with dev tools
	python3 -m venv .venv
	$(PYTHON) -m pip install -c requirements.lock -e '.[dev]'

lint: ## Ruff lint + format check
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format: ## Auto-fix lint and formatting
	$(PYTHON) -m ruff check --fix .
	$(PYTHON) -m ruff format .

test: ## Unit + integration tests
	$(PYTHON) -m pytest

run: ## Run one month locally: make run MONTH=2025-01
	$(PYTHON) -m pipeline run --month $(MONTH)

backfill: ## Run a range locally: make backfill START=2025-01 END=2025-03
	$(PYTHON) -m pipeline run --start $(START) --end $(END)

dbt-build: ## Rebuild and test every warehouse model over all staged months
	$(DBT) build $(DBT_ARGS)

docs: ## Generate dbt docs (lineage graph) and serve them on http://localhost:8080
	$(DBT) docs generate $(DBT_ARGS)
	$(DBT) docs serve $(DBT_ARGS) --port 8080

docker-build: ## Build the pipeline image
	docker build -t $(IMAGE) .

docker-run: docker-build ## Run one month in Docker: make docker-run MONTH=2025-01
	@mkdir -p data
	$(DOCKER_RUN) run --month $(MONTH)

docker-backfill: docker-build ## Backfill in Docker: make docker-backfill START=2025-01 END=2025-03
	@mkdir -p data
	$(DOCKER_RUN) run --start $(START) --end $(END)

clean: ## Delete all pipeline outputs (raw, staging, warehouse)
	rm -rf data/raw data/staging data/dbt data/warehouse.duckdb
