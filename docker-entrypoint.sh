set -e

CMD_ARGS="--stdio"

if [ -n "$PROJECT_ID" ]; then
  CMD_ARGS="$CMD_ARGS --project-id $PROJECT_ID"
fi

if [ -n "$LOCATION" ]; then
  CMD_ARGS="$CMD_ARGS --location $LOCATION"
fi

exec python -m mcp_bigquery_server $CMD_ARGS
