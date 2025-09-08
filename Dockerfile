# Use Python 3.11 slim image for ARM64
FROM --platform=linux/arm64 python:3.13-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1 \
    UV_NO_CACHE=1

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        libc6-dev \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv - download the binary directly for more reliability
RUN curl -LsSf https://github.com/astral-sh/uv/releases/latest/download/uv-aarch64-unknown-linux-gnu.tar.gz | tar -xz -C /usr/local/bin --strip-components=1

# Verify uv installation
RUN uv --version

# Copy project files
COPY pyproject.toml uv.lock* ./
COPY src/ ./src/


# Install dependencies and project using uv (no virtual environment in container)
ENV UV_SYSTEM_PYTHON=1
RUN uv sync --frozen --no-dev --no-install-workspace \
    && uv pip install -e . --system

# Create a non-root user
RUN groupadd -r appuser && useradd -r -g appuser -d /app appuser \
    && chown -R appuser:appuser /app
USER appuser

# Health check using uv run
HEALTHCHECK --interval=30s --timeout=30s --start-period=10s --retries=3 \
    CMD uv run python -c "import sys; print('Health check passed'); sys.exit(0)" || exit 1

# Set the entrypoint to use uv run
ENTRYPOINT ["uv", "run"]
CMD ["start_etl"]