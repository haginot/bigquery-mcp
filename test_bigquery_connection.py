"""
Comprehensive test script to verify BigQuery connection and query execution.
This script demonstrates connecting to BigQuery and executing various operations.
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
    parser = argparse.ArgumentParser(description="Test BigQuery connection and query execution")
    parser.add_argument("--project-id", default="query-management-and-answering", help="Google Cloud project ID")
    parser.add_argument("--wait-time", type=float, default=2.0, help="Wait time between requests in seconds")
    args = parser.parse_args()
    
    print(f"Testing BigQuery connection with project ID: {args.project_id}", file=sys.stderr)
    
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
        "method": "tools/call",
        "params": {
            "name": "list_datasets",
            "arguments": {
                "projectId": args.project_id
            }
        },
        "id": 2
    }
    
    simple_query_request = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "execute_query",
            "arguments": {
                "projectId": args.project_id,
                "sql": "SELECT 1 as test_value",
                "dryRun": False
            }
        },
        "id": 3
    }
    
    complex_query_request = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "execute_query",
            "arguments": {
                "projectId": args.project_id,
                "sql": """
                SELECT
                  current_timestamp() as timestamp,
                  current_date() as date,
                  session_user() as user
                """,
                "dryRun": False
            }
        },
        "id": 4
    }
    
    print("\n=== TESTING INITIALIZATION ===", file=sys.stderr)
    send_request(initialize_request, args.wait_time)
    
    print("\n=== TESTING TOOLS LIST ===", file=sys.stderr)
    send_request(list_tools_request, args.wait_time)
    
    print("\n=== TESTING DATASET LISTING ===", file=sys.stderr)
    send_request(list_datasets_request, args.wait_time)
    
    print("\n=== TESTING SIMPLE QUERY ===", file=sys.stderr)
    send_request(simple_query_request, args.wait_time)
    
    print("\n=== TESTING COMPLEX QUERY ===", file=sys.stderr)
    send_request(complex_query_request, args.wait_time)
    
    print("\nTest completed. The above results demonstrate successful connection to BigQuery.", file=sys.stderr)

if __name__ == "__main__":
    main()
