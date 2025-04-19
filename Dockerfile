FROM python:3.11-slim

WORKDIR /app

# Install toolchain first to leverage caching
RUN pip install --no-cache-dir poetry==1.7.1

# Copy dependency files only to separate layers
COPY pyproject.toml poetry.lock* README.md ./

# Configure Poetry to use system environment
RUN poetry config virtualenvs.create false

# Copy application code before installation
COPY src/ ./src/

# Install the package with dependencies
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir mcp==1.6.0 \
 && poetry install --no-dev

# Set environment variables for proper stdio handling
ENV PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1

# Create volume mount point for credentials
VOLUME /credentials

# Use Python module directly as entrypoint since we're in the app directory
ENTRYPOINT ["python", "-m", "mcp_bigquery_server"]

# Default to stdio mode for Claude Desktop compatibility
CMD ["--stdio"]
