
if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    echo "Warning: GOOGLE_APPLICATION_CREDENTIALS environment variable is not set."
    echo "The server will still start, but BigQuery operations will fail."
    echo "Set this variable to your service account key file path for full functionality."
    echo ""
fi

echo "Installing package in development mode..."
poetry install

echo "Starting MCP BigQuery server with stdio transport..."
poetry run python -m mcp_bigquery_server &
SERVER_PID=$!

sleep 2

echo "Running test client..."
poetry run python -m mcp_bigquery_server.test_client

echo "Stopping server..."
kill $SERVER_PID

echo "Test complete!"
