FROM python:3.11-slim

WORKDIR /app

# Set environment variables for proper stdio handling and pip configuration
ENV PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_DEFAULT_TIMEOUT=100 \
    PIP_RETRIES=3 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    GOOGLE_APPLICATION_CREDENTIALS=/app/key.json

# Install toolchain first to leverage caching
RUN pip install --no-cache-dir poetry==1.7.1 && \
    poetry config virtualenvs.create false

# Copy dependency files only to separate layers
COPY pyproject.toml poetry.lock* README.md ./

# Install MCP and FastMCP separately with retries
RUN for i in 1 2 3 4 5; do \
      pip install --no-cache-dir mcp==1.6.0 fastmcp==0.1.0 && break || sleep 15; \
    done

# Install dependencies with retries (without the package itself)
RUN for i in 1 2 3 4 5; do \
      pip install --no-cache-dir --upgrade pip && \
      poetry install --no-dev --no-root && break || sleep 15; \
    done

# Copy application code after dependencies are installed
COPY src/ ./src/

# Install the package itself
RUN poetry install --no-dev

# Create volume mount point for credentials
VOLUME /credentials

# Use the entrypoint script to handle environment variables properly
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# Use the entrypoint script as the main entry point
ENTRYPOINT ["/app/docker-entrypoint.sh"]

# Default command arguments (these can be overridden at runtime)
CMD ["--project-id", "amazon-study-db", "--location", "US"]
