"""
Test script to verify that project ID duplication is prevented in INFORMATION_SCHEMA queries.
This simulates the Claude Desktop query pattern where projectId is provided in the query parameters.
"""
import json
import sys
import time
import argparse

def send_request(request):
    """Send a JSON-RPC request to the stdio server."""
    json_str = json.dumps(request)
    print(f"Sending request: {json_str}", file=sys.stderr)
    print(json_str, flush=True)
    time.sleep(1)  # Give the server time to process

def main():
    parser = argparse.ArgumentParser(description="Test project ID duplication prevention")
    parser.add_argument("--project-id", required=True, help="Google Cloud project ID")
    args = parser.parse_args()

    initialize_request = {
        "jsonrpc": "2.0",
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "claude-ai",
                "version": "0.1.0"
            }
        },
        "id": 0
    }

    execute_query_with_projectid = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "execute_query",
            "arguments": {
                "projectId": args.project_id,
                "location": "us",
                "sql": "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA LIMIT 5",
                "dryRun": False
            }
        },
        "id": 1
    }

    execute_query_with_projectid_in_sql = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "execute_query",
            "arguments": {
                "projectId": args.project_id,
                "location": "us",
                "sql": f"SELECT * FROM `{args.project_id}`.INFORMATION_SCHEMA.SCHEMATA LIMIT 5",
                "dryRun": False
            }
        },
        "id": 2
    }

    print("Testing MCP BigQuery server with projectId duplication prevention...", file=sys.stderr)
    send_request(initialize_request)
    send_request(execute_query_with_projectid)
    send_request(execute_query_with_projectid_in_sql)
    print("Test completed", file=sys.stderr)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Test terminated by user.", file=sys.stderr)

if __name__ == "__main__":
    main()
