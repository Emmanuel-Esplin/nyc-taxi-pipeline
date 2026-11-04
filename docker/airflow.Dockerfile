# syntax=docker/dockerfile:1
# Airflow image with the pipeline installed. Build context is the repo root:
#   docker build -f docker/airflow.Dockerfile -t nyc-taxi-airflow .
FROM apache/airflow:3.3.2-python3.12

USER root
RUN mkdir -p /opt/pipeline /opt/dbt-venv && chown airflow:root /opt/pipeline /opt/dbt-venv
USER airflow

# dbt lives in its own virtualenv: its dependency pins (protobuf, jinja2, ...) conflict with
# Airflow's constraints. The pipeline calls it through PIPELINE_DBT_BIN.
COPY --chown=airflow:root requirements.lock /opt/pipeline/
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir -c /opt/pipeline/requirements.lock dbt-duckdb duckdb

# The pipeline package itself, into Airflow's environment. --no-deps keeps Airflow's pinned
# versions; its runtime needs beyond Airflow are duckdb and Great Expectations, installed
# alongside apache-airflow so pip must find versions that satisfy both.
COPY --chown=airflow:root pyproject.toml README.md /opt/pipeline/
COPY --chown=airflow:root src /opt/pipeline/src
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" duckdb==1.5.6 \
        great_expectations==1.24.1 \
    && pip install --no-cache-dir --no-deps /opt/pipeline

COPY --chown=airflow:root config.yaml /opt/pipeline/
COPY --chown=airflow:root dbt /opt/pipeline/dbt
COPY --chown=airflow:root airflow/dags /opt/airflow/dags

ENV PIPELINE_CONFIG=/opt/pipeline/config.yaml \
    PIPELINE_DBT_DIR=/opt/pipeline/dbt \
    PIPELINE_DBT_BIN=/opt/dbt-venv/bin/dbt \
    PIPELINE_DATA_DIR=/opt/airflow/data \
    PIPELINE_WAREHOUSE_PATH=/opt/airflow/data/warehouse.duckdb
