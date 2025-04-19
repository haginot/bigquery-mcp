"""
Test script for the MCP BigQuery server running in Docker with stdio transport.
This simulates how Claude Desktop would interact with the server.
"""
import json
import sys
import time
from typing import Dict, Any

def send_request(request: Dict[str, Any]) -> None:
    """Send a JSON-RPC request to the stdio server."""
    json_str = json.dumps(request)
    print(f"Sending request: {json_str}")
    print(json_str, flush=True)
    time.sleep(0.5)  # Give the server time to process

execute_query_request = {
    "jsonrpc": "2.0",
    "method": "call_tool",
    "params": {
        "tool": "execute_query",
        "params": {
            "query": "SELECT 1 as test",
            "dryRun": True
        }
    },
    "id": 1
}

list_datasets_request = {
    "jsonrpc": "2.0",
    "method": "call_tool",
    "params": {
        "tool": "list_datasets",
        "params": {}
    },
    "id": 2
}

if __name__ == "__main__":
    print("Testing MCP BigQuery server with stdio transport...")
    print("This simulates how Claude Desktop would interact with the server.")
    print("Run the server with: docker run -i --rm -v /path/to/credentials:/credentials -e GOOGLE_APPLICATION_CREDENTIALS=/credentials/service-account-key.json mcp-bigquery-server --stdio")
    print("Then pipe this script to the server: python test_docker_stdio.py | docker run -i --rm ...")
    print("\nSending requests to the server...")
    
    send_request(execute_query_request)
    send_request(list_datasets_request)
    
    print("\nTest complete. Check the server output for responses.")
