"""
Test script to verify INFORMATION_SCHEMA query handling in the BigQuery MCP server.
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
    parser = argparse.ArgumentParser(description="Test INFORMATION_SCHEMA queries")
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

    execute_info_schema_datasets_request = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "execute_query",
            "arguments": {
                "projectId": args.project_id,
                "location": "us",
                "sql": "SELECT * FROM `region-us`.INFORMATION_SCHEMA.DATASETS",
                "dryRun": False
            }
        },
        "id": 1
    }

    print("Testing MCP BigQuery server with INFORMATION_SCHEMA queries...", file=sys.stderr)
    send_request(initialize_request)
    send_request(execute_info_schema_datasets_request)
    print("Test completed", file=sys.stderr)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Test terminated by user.", file=sys.stderr)

if __name__ == "__main__":
    main()
