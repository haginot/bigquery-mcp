#!/bin/bash
# Create credentials directory if it doesn't exist
mkdir -p credentials

# Copy the service account key if it doesn't exist
if [ ! -f "credentials/service-account-key.json" ]; then
  cp ~/attachments/d005d977-c737-45cc-bee0-b7f6836b460d/query-management-and-answering-16941c344903.json credentials/service-account-key.json
fi

# Rebuild the Docker image
docker build -t mcp-bigquery-server .

# Run the Claude Desktop simulator against the Docker container
echo "Running Claude Desktop simulator against MCP BigQuery server..."
python3 $(dirname "$0")/claude_simulator.py | docker run -i --rm \
  -v $(pwd)/credentials:/credentials \
  -e GOOGLE_APPLICATION_CREDENTIALS=/credentials/service-account-key.json \
  mcp-bigquery-server --stdio
