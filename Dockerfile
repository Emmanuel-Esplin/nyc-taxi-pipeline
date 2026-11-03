# syntax=docker/dockerfile:1
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first so code changes don't invalidate the layer.
COPY requirements.lock pyproject.toml README.md ./
COPY src ./src
RUN pip install -c requirements.lock .

COPY config.yaml ./
COPY sql ./sql

RUN useradd --create-home --uid 1000 pipeline && mkdir -p /app/data && chown pipeline /app/data
USER pipeline
VOLUME ["/app/data"]

ENTRYPOINT ["python", "-m", "pipeline"]
CMD ["--help"]
