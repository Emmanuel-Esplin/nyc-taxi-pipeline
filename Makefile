# Usage: make run MONTH=2025-01 | make backfill START=2025-01 END=2025-03 | make test
#        make airflow-up | make airflow-backfill START=2025-01 END=2025-03 | make airflow-down
#        make up (Airflow + Metabase) | make bi-up | make bi-bootstrap | make publish | make down
MONTH ?= 2025-01
START ?= 2025-01
END   ?= 2025-03
IMAGE ?= nyc-taxi-pipeline
PYTHON ?= .venv/bin/python
export PYTHONPATH := $(CURDIR)/src
# Serving copy for BI, via the compose Postgres published on localhost:5433.
SERVING_URL ?= postgresql://analytics_writer:local-dev-writer@localhost:5433/analytics
CHROME ?= /Applications/Google Chrome.app/Contents/MacOS/Google Chrome

# dbt reads the same paths the pipeline uses; artifacts and logs go under data/dbt/.
DBT = PIPELINE_DATA_DIR="$(CURDIR)/data" PIPELINE_WAREHOUSE_PATH="$(CURDIR)/data/warehouse.duckdb" \
	.venv/bin/dbt
DBT_ARGS = --project-dir dbt --profiles-dir dbt \
	--target-path "$(CURDIR)/data/dbt/target" --log-path "$(CURDIR)/data/dbt/logs"
DOCKER_RUN = docker run --rm -v "$(CURDIR)/data:/app/data" $(DOCKER_ENV) $(IMAGE)

.PHONY: help install lint format test run backfill dbt-build docs quality-docs docker-build docker-run docker-backfill \
	airflow-up airflow-down airflow-backfill airflow-logs up down bi-up bi-bootstrap publish screenshots clean

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

quality-docs: ## Open the Great Expectations Data Docs (validation history per month)
	open "$(CURDIR)/data/gx/uncommitted/data_docs/local_site/index.html" 2>/dev/null || \
		xdg-open "$(CURDIR)/data/gx/uncommitted/data_docs/local_site/index.html"

docker-build: ## Build the pipeline image
	docker build -t $(IMAGE) .

docker-run: docker-build ## Run one month in Docker: make docker-run MONTH=2025-01
	@mkdir -p data
	$(DOCKER_RUN) run --month $(MONTH)

docker-backfill: docker-build ## Backfill in Docker: make docker-backfill START=2025-01 END=2025-03
	@mkdir -p data
	$(DOCKER_RUN) run --start $(START) --end $(END)

up: ## Start everything: Airflow (:8080), Postgres and Metabase (:3000)
	@mkdir -p data
	docker compose up -d --build --wait

down: ## Stop everything (keeps volumes; add -v to drop them)
	docker compose down

bi-up: ## Start only Postgres + Metabase on http://localhost:3000
	docker compose up -d --wait metabase

bi-bootstrap: ## Create/update the Metabase dashboard from metabase/bootstrap.py
	python3 metabase/bootstrap.py

publish: ## Copy the serving tables from DuckDB to the compose Postgres
	PIPELINE_SERVING_URL="$(SERVING_URL)" $(PYTHON) -m pipeline publish

screenshots: ## Refresh README screenshots (Metabase dashboard + GX Data Docs) with headless Chrome
	"$(CHROME)" --headless=new --hide-scrollbars --run-all-compositor-stages-before-draw \
		--window-size=1600,2000 --virtual-time-budget=60000 \
		--screenshot="$(CURDIR)/docs/img/metabase_dashboard.png" "$$(cat data/metabase_public_url.txt)"
	"$(CHROME)" --headless=new --hide-scrollbars --window-size=1400,900 --virtual-time-budget=8000 \
		--screenshot="$(CURDIR)/docs/img/gx_data_docs.png" \
		"file://$(CURDIR)/data/gx/uncommitted/data_docs/local_site/index.html"

airflow-up: ## Start Airflow (Postgres, api-server, scheduler) on http://localhost:8080
	@mkdir -p data
	docker compose up -d --build --wait

airflow-down: ## Stop Airflow (keeps the metadata DB volume; add -v to drop it)
	docker compose down

airflow-backfill: ## Backfill via Airflow: make airflow-backfill START=2025-01 END=2025-03
	docker compose exec airflow-scheduler airflow dags unpause nyc_taxi_monthly
	docker compose exec airflow-scheduler airflow backfill create --dag-id nyc_taxi_monthly \
		--from-date $(START)-01 --to-date $(END)-01 --reprocess-behavior completed --max-active-runs 1

airflow-logs: ## Follow scheduler logs
	docker compose logs -f airflow-scheduler

clean: ## Delete all pipeline outputs (raw, staging, warehouse)
	rm -rf data/raw data/staging data/dbt data/warehouse.duckdb
