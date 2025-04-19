FROM python:3.10-slim

WORKDIR /app

# Install Poetry
RUN pip install poetry==1.7.1

# Copy project files
COPY pyproject.toml poetry.lock* README.md ./
COPY src/ ./src/

# Configure Poetry to not create a virtual environment
RUN poetry config virtualenvs.create false

# Install dependencies and ensure latest MCP version
RUN pip install --upgrade pip && \
    pip install mcp==1.6.0 && \
    poetry install --no-dev

# Expose port for HTTP transport
EXPOSE 8000

# Set environment variables for proper stdio handling
ENV PYTHONUNBUFFERED=1
ENV PYTHONFAULTHANDLER=1

# Create volume mount point for credentials
VOLUME /credentials

# Set entrypoint
ENTRYPOINT ["python", "-m", "mcp_bigquery_server"]

# Default command (can be overridden)
CMD ["--http", "--host", "0.0.0.0", "--port", "8000"]
