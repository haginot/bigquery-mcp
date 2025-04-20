"""
Test script to verify environment variable handling in the BigQuery MCP server.
This script checks if environment variables are correctly passed to the server.
"""
import os
import sys
import json

def main():
    """Print environment variables and send a test query."""
    print("Environment variables:", file=sys.stderr)
    print(f"PROJECT_ID: {os.environ.get('PROJECT_ID', 'Not set')}", file=sys.stderr)
    print(f"LOCATION: {os.environ.get('LOCATION', 'Not set')}", file=sys.stderr)
    print(f"GOOGLE_APPLICATION_CREDENTIALS: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'Not set')}", file=sys.stderr)
    
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
        "id": 1
    }
    print(json.dumps(initialize_request), flush=True)
    
    tools_list_request = {
        "jsonrpc": "2.0",
        "method": "tools/list",
        "params": {},
        "id": 2
    }
    print(json.dumps(tools_list_request), flush=True)
    
    execute_query_request = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "execute_query",
            "arguments": {
                "sql": "SELECT 1 as test"
            }
        },
        "id": 3
    }
    print(json.dumps(execute_query_request), flush=True)
    
    print("Test script is running. Press Ctrl+C to exit.", file=sys.stderr)
    try:
        while True:
            line = sys.stdin.readline()
            if not line:
                break
            try:
                response = json.loads(line)
                print(f"Received response: {json.dumps(response)}", file=sys.stderr)
            except json.JSONDecodeError:
                print(f"Received non-JSON response: {line}", file=sys.stderr)
    except KeyboardInterrupt:
        print("Test terminated by user.", file=sys.stderr)

if __name__ == "__main__":
    main()
