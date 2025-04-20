"""
Test script to verify project ID and region configuration in the BigQuery MCP server.
"""
import json
import sys
import time

def send_request(request):
    """Send a JSON-RPC request to the stdio server."""
    json_str = json.dumps(request)
    print(f"Sending request: {json_str}", file=sys.stderr)
    print(json_str, flush=True)
    time.sleep(1)  # Give the server time to process

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

tools_list_request = {
    "jsonrpc": "2.0",
    "method": "tools/list",
    "params": {},
    "id": 1
}

list_datasets_default_request = {
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
        "name": "list_datasets",
        "arguments": {}
    },
    "id": 2
}

execute_query_default_request = {
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
        "name": "execute_query",
        "arguments": {
            "sql": "SELECT 1 as test",
            "dryRun": True
        }
    },
    "id": 3
}

execute_query_explicit_request = {
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
        "name": "execute_query",
        "arguments": {
            "projectId": "query-management-and-answering",
            "sql": "SELECT 1 as test",
            "dryRun": True
        }
    },
    "id": 4
}

if __name__ == "__main__":
    print("Testing MCP BigQuery server with project ID and region configuration...", file=sys.stderr)
    send_request(initialize_request)
    send_request(tools_list_request)
    send_request(list_datasets_default_request)
    send_request(execute_query_default_request)
    send_request(execute_query_explicit_request)
    print("Test completed", file=sys.stderr)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Test terminated by user.", file=sys.stderr)
