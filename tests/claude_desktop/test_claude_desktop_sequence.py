"""
Comprehensive test script for the BigQuery MCP server that simulates
the Claude Desktop initialization sequence and tests all tools.
"""
import json
import sys
import time
import argparse

def send_request(request, wait_time=2):
    """Send a JSON-RPC request and wait for the response."""
    json_str = json.dumps(request)
    print(f"Sending request: {json_str}", file=sys.stderr)
    print(json_str, flush=True)
    time.sleep(wait_time)  # Wait for response

def main():
    parser = argparse.ArgumentParser(description="Test the BigQuery MCP server with Claude Desktop initialization sequence")
    parser.add_argument("--project-id", default="query-management-and-answering", help="Google Cloud project ID")
    parser.add_argument("--wait-time", type=float, default=2.0, help="Wait time between requests in seconds")
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
    
    list_tools_request = {
        "jsonrpc": "2.0",
        "method": "tools/list",
        "params": {},
        "id": 1
    }
    
    list_datasets_request = {
        "jsonrpc": "2.0",
        "method": "call_tool",
        "params": {
            "tool": "list_datasets",
            "params": {
                "projectId": args.project_id
            }
        },
        "id": 2
    }
    
    execute_query_request = {
        "jsonrpc": "2.0",
        "method": "call_tool",
        "params": {
            "tool": "execute_query",
            "params": {
                "projectId": args.project_id,
                "sql": "SELECT 1 as test",
                "dryRun": True
            }
        },
        "id": 3
    }
    
    send_request(initialize_request, args.wait_time)
    
    send_request(list_tools_request, args.wait_time)
    
    send_request(list_datasets_request, args.wait_time)
    
    send_request(execute_query_request, args.wait_time)
    
    print("Test completed", file=sys.stderr)

if __name__ == "__main__":
    main()
