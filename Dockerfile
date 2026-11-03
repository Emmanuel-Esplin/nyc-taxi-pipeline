# syntax=docker/dockerfile:1
# Runtime image for the pipeline. Usage: docker run -v $PWD/data:/app/data IMAGE run --month 2025-01
FROM python:3.12-slim AS runtime

# No .pyc files, unbuffered logs (so docker logs stream live), no pip cache in the image.
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

# Run as an unprivileged user; outputs go to the mounted data volume.
RUN useradd --create-home --uid 1000 pipeline && mkdir -p /app/data && chown pipeline /app/data
USER pipeline
VOLUME ["/app/data"]

ENTRYPOINT ["python", "-m", "pipeline"]
CMD ["--help"]
