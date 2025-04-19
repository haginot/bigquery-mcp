# BigQuery MCP Server

A fully-compliant Model Context Protocol (MCP) server that surfaces Google BigQuery functionality to LLM agents and other MCP clients. Implements MCP specification rev 2025-03-26.

## Features

- Implements MCP specification rev 2025-03-26
- Supports stdio transport (default) and optional HTTP transport
- Exposes BigQuery operations through MCP Tools
- Supports pagination for long result sets
- Implements logging utilities
- Handles errors according to JSON-RPC standards
- Docker support for easy deployment

## Installation

```bash
# Install from source
git clone https://github.com/haginot/bigquery-mcp.git
cd bigquery-mcp
pip install .

# Or using Poetry
poetry install
```

## Authentication

The server uses Google Cloud authentication. You need to set up authentication credentials:

```bash
# Set the environment variable to your service account key file
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json

# Or use gcloud to authenticate
gcloud auth application-default login
```

## Usage

### Command Line

```bash
# Start with stdio transport (default)
mcp-bigquery-server

# Start with HTTP transport
mcp-bigquery-server --http --port 8000

# Enable resource exposure
mcp-bigquery-server --expose-resources

# Set query timeout
mcp-bigquery-server --query-timeout-ms 60000
```

### Python API

```python
from mcp_bigquery_server.server import BigQueryMCPServer

# Create and start the server
server = BigQueryMCPServer(
    expose_resources=True,
    http_enabled=True,
    host="localhost",
    port=8000,
    query_timeout_ms=30000,
)
server.start()
```

### Docker

You can run the MCP server in a Docker container:

```bash
# Build the Docker image
docker build -t mcp-bigquery-server .

# Run with stdio transport (for use with Claude Desktop)
docker run -i --rm \
  -v /path/to/credentials:/credentials \
  -e GOOGLE_APPLICATION_CREDENTIALS=/credentials/service-account-key.json \
  mcp-bigquery-server --stdio

# Run with HTTP transport
docker run -p 8000:8000 --rm \
  -v /path/to/credentials:/credentials \
  -e GOOGLE_APPLICATION_CREDENTIALS=/credentials/service-account-key.json \
  mcp-bigquery-server
```

You can also use docker-compose:

```bash
# Create a credentials directory and copy your service account key
mkdir -p credentials
cp /path/to/your/service-account-key.json credentials/service-account-key.json

# Start the server with docker-compose
docker-compose up
```

### Using with Claude Desktop

To use this MCP server with Claude Desktop:

1. Run the server with stdio transport:
   ```bash
   # Using Docker directly
   docker run -i --rm \
     -v /path/to/credentials:/credentials \
     -e GOOGLE_APPLICATION_CREDENTIALS=/credentials/service-account-key.json \
     mcp-bigquery-server --stdio
   
   # Or using docker-compose (after uncommenting the stdio settings)
   # Edit docker-compose.yml to enable stdin_open, tty and change command to --stdio
   docker-compose up
   ```

2. In Claude Desktop:
   - Go to Settings > Tools
   - Select "Add Tool" > "Add MCP Tool"
   - Choose "Connect to running MCP server"
   - Select "stdio" as the transport
   - Click "Connect" and select the terminal running your Docker container

Claude will now have access to all BigQuery operations through the MCP server.


## Available Tools

The server exposes the following BigQuery operations as MCP tools:

- `execute_query`: Submit a SQL query to BigQuery, optionally as dry-run
- `get_job_status`: Poll job execution state
- `cancel_job`: Cancel a running BigQuery job
- `fetch_results_chunk`: Page through results
- `list_datasets`: Enumerate datasets visible to the service account
- `get_table_schema`: Retrieve schema for a table

## Resources (Optional)

When enabled with `--expose-resources`, the server exposes:

- Dataset & table schemas as read-only resources (`bq://<project>/<dataset>/schema`)
- Query result sets (chunk URIs)

## License

MIT
