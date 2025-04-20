# BigQuery MCP Server Test Instructions

This document provides detailed instructions for testing the BigQuery MCP server locally, both with HTTP and stdio transports.

## Prerequisites

1. Google Cloud service account key with BigQuery access
2. Docker installed
3. Python 3.8+ installed

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/haginot/bigquery-mcp.git
   cd bigquery-mcp
   ```

2. Create a credentials directory and copy your service account key:
   ```bash
   mkdir -p credentials
   cp /path/to/your/service-account-key.json credentials/service-account-key.json
   ```

## Testing with Docker

### HTTP Transport Test

1. Build the Docker image:
   ```bash
   docker build -t mcp-bigquery-server .
   ```

2. Run the server with HTTP transport:
   ```bash
   docker run -p 8000:8000 --rm \
     -v $(pwd)/credentials:/credentials \
     -e GOOGLE_APPLICATION_CREDENTIALS=/credentials/service-account-key.json \
     mcp-bigquery-server --http --host 0.0.0.0 --port 8000
   ```

3. In a separate terminal, run the HTTP test script:
   ```bash
   python test_docker_connection.py
   ```

### Stdio Transport Test (Claude Desktop Simulation)

1. Build the Docker image (if not already built):
   ```bash
   docker build -t mcp-bigquery-server .
   ```

2. Run the test script that simulates Claude Desktop's initialization sequence:
   ```bash
   ./test_scripts/run_claude_init_test.sh
   ```

3. For a more comprehensive test that includes actual BigQuery operations:
   ```bash
   ./test_scripts/run_bigquery_connection_test.sh
   ```

## Testing with Docker Compose

1. Start the HTTP server:
   ```bash
   docker-compose up mcp-bigquery-server
   ```

2. Start the stdio server (for Claude Desktop):
   ```bash
   docker-compose up mcp-bigquery-server-stdio
   ```

## Testing with Claude Desktop

1. Run the server with stdio transport:
   ```bash
   docker run -i --rm \
     -v $(pwd)/credentials:/credentials \
     -e GOOGLE_APPLICATION_CREDENTIALS=/credentials/service-account-key.json \
     mcp-bigquery-server --stdio
   ```

2. In Claude Desktop:
   - Go to Settings > Tools
   - Select "Add Tool" > "Add MCP Tool"
   - Choose "Connect to running MCP server"
   - Select "stdio" as the transport
   - Click "Connect" and select the terminal running your Docker container

3. Test the connection by asking Claude to run a simple BigQuery query:
   ```
   Please run a simple BigQuery query using the MCP tool: SELECT 1 as test_value
   ```

## Test Scripts

The repository includes several test scripts:

- `test_docker_connection.py`: Tests HTTP transport
- `test_docker_stdio.py`: Tests stdio transport with basic requests
- `test_scripts/test_claude_init_sequence.py`: Simulates Claude Desktop's initialization sequence
- `test_scripts/test_bigquery_connection.py`: Tests actual BigQuery connection with queries

## Troubleshooting

- If the Docker container exits immediately with stdio transport, ensure you're using the latest version of the server that includes the fix for keeping the process alive.
- If you encounter authentication issues, verify that your service account key is correctly mounted in the Docker container.
- For HTTP transport issues, check that the port is correctly exposed and not blocked by a firewall.
