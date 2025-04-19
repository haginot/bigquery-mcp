#!/usr/bin/env python3
"""
Claude Desktop simulator for testing MCP server stdio transport.
"""
import json
import sys
import time

def send_request(request):
    """Send a JSON-RPC request to the server."""
    json_str = json.dumps(request)
    print(json_str, flush=True)
    print(f"Sent: {json_str}", file=sys.stderr)
    
    # Wait for response
    response = sys.stdin.readline().strip()
    print(f"Received: {response}", file=sys.stderr)
    return response

def main():
    """Simulate Claude Desktop's initialization sequence."""
    # Step 1: Initialize
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
    
    initialize_response = send_request(initialize_request)
    
    # Step 2: List Tools
    list_tools_request = {
        "jsonrpc": "2.0",
        "method": "tools/list",
        "params": {},
        "id": 1
    }
    
    list_tools_response = send_request(list_tools_request)
    
    # Step 3: Call Tool (list_datasets)
    call_tool_request = {
        "jsonrpc": "2.0",
        "method": "call_tool",
        "params": {
            "tool": "list_datasets",
            "params": {}
        },
        "id": 2
    }
    
    call_tool_response = send_request(call_tool_request)
    
    print("Claude Desktop simulation completed successfully", file=sys.stderr)

if __name__ == "__main__":
    main()
