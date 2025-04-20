set -e

echo "Building Docker image for credentials fix test..."
docker build -t mcp-bigquery-server-creds-fix:latest .

echo "Running container to test credentials handling with MCP server..."
docker run -i --rm \
  -v ~/attachments/64ba55d6-602f-4772-9884-0619feefa042/amazon-study-db-c6ecbc10001a.json:/app/key.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/key.json \
  -e PROJECT_ID=amazon-study-db \
  -e LOCATION=US \
  mcp-bigquery-server-creds-fix:latest \
  --stdio << EOF
{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"claude-ai","version":"0.1.0"}},"id":1}
{"jsonrpc":"2.0","method":"tools/list","params":{},"id":2}
{"jsonrpc":"2.0","method":"tools/call","params":{"name":"execute_query","arguments":{"sql":"SELECT 1 as test"}},"id":3}
EOF

echo "Test completed"
