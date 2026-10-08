# One image for producer/consumer/scheduler — the command differs per service.
# Runtime contains only the venv + src (no build tools, no secrets).

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

FROM python:3.12-slim-bookworm AS runtime
RUN useradd --create-home app
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
# Scheduler needs the dbt project + quality dir in-image. Both must be writable
# by `app` (dbt target/logs/packages, GX JSON reports).
COPY dbt ./dbt
COPY quality ./quality
RUN mkdir -p /app/quality/reports /app/quality/suites \
    && chown -R app:app /app/dbt /app/quality
USER app
ENV PATH="/app/.venv/bin:$PATH" PYTHONPATH="/app/src"
HEALTHCHECK --interval=60s --timeout=10s --retries=3 \
    CMD python -c "import pipeline.common.config, pipeline.common.models"
CMD ["python", "-m", "pipeline.producer"]
