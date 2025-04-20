"""
Test script that simulates Claude Desktop's tools/call method.
This helps verify our server correctly handles the tools/call method format.
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

tools_call_request = {
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
        "name": "list_datasets",
        "arguments": {
            "projectId": "query-management-and-answering"
        }
    },
    "id": 111
}

tools_call_query_request = {
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
    "id": 112
}

if __name__ == "__main__":
    print("Testing MCP BigQuery server with Claude Desktop tools/call format...", file=sys.stderr)
    send_request(initialize_request)
    send_request(tools_list_request)
    send_request(tools_call_request)
    send_request(tools_call_query_request)
    print("Test completed", file=sys.stderr)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Test terminated by user.", file=sys.stderr)
