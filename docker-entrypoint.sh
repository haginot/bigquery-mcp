#!/bin/bash
set -e

# Build command with optional project ID and location from environment variables
CMD_ARGS="--stdio"

if [ -n "$PROJECT_ID" ]; then
  CMD_ARGS="$CMD_ARGS --project-id $PROJECT_ID"
fi

if [ -n "$LOCATION" ]; then
  CMD_ARGS="$CMD_ARGS --location $LOCATION"
fi

echo "Environment variables:" >&2
echo "GOOGLE_APPLICATION_CREDENTIALS: $GOOGLE_APPLICATION_CREDENTIALS" >&2
echo "PROJECT_ID: $PROJECT_ID" >&2
echo "LOCATION: $LOCATION" >&2

# Execute the MCP server with the constructed arguments
exec python -m mcp_bigquery_server $CMD_ARGS
