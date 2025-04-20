#!/usr/bin/env python3
"""
Test script for the MCP BigQuery server running in Docker with stdio transport.
This script tests if the Docker container stays alive when using stdio transport.
"""
import json
import subprocess
import sys
import time
import os
from typing import Dict, Any, Optional

def main():
    """Run the test."""
    print("Testing MCP BigQuery server in Docker with stdio transport...")
    
    # Check if credentials exist
    credentials_path = os.path.join(os.getcwd(), "credentials", "query-management-and-answering-16941c344903.json")
    if not os.path.exists(credentials_path):
        print(f"Error: Credentials file not found at {credentials_path}")
        print("Please make sure to copy the service account key to the credentials directory.")
        sys.exit(1)
    
    # Build the Docker image
    print("Building Docker image...")
    subprocess.run(
        ["docker", "build", "-t", "mcp-bigquery-server", "."],
        check=True,
    )
    
    # Start the Docker container with stdio transport
    print("Starting Docker container with stdio transport...")
    process = subprocess.Popen(
        [
            "docker", "run", "-i", "--rm",
            "-v", f"{os.path.abspath('credentials')}:/credentials",
            "-e", "GOOGLE_APPLICATION_CREDENTIALS=/credentials/query-management-and-answering-16941c344903.json",
            "mcp-bigquery-server",
            "--stdio"
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    
    # Give the container time to start
    time.sleep(2)
    
    # Check if the container is still running
    if process.poll() is not None:
        print(f"Error: Container exited with code {process.poll()}")
        stderr = process.stderr.read()
        print(f"Container stderr: {stderr}")
        sys.exit(1)
    
    print("Container is running. Sending initialize request...")
    
    # Send initialize request (like Claude Desktop would)
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
    
    process.stdin.write(json.dumps(initialize_request) + "\n")
    process.stdin.flush()
    
    # Wait for response
    time.sleep(2)
    
    # Check if the container is still running after initialize
    if process.poll() is not None:
        print(f"Error: Container exited after initialize request with code {process.poll()}")
        stderr = process.stderr.read()
        print(f"Container stderr: {stderr}")
        sys.exit(1)
    
    print("Container is still running after initialize request. Sending tools/list request...")
    
    # Send tools/list request
    list_tools_request = {
        "jsonrpc": "2.0",
        "method": "tools/list",
        "id": 2
    }
    
    process.stdin.write(json.dumps(list_tools_request) + "\n")
    process.stdin.flush()
    
    # Wait for response
    time.sleep(2)
    
    # Check if the container is still running after tools/list
    if process.poll() is not None:
        print(f"Error: Container exited after tools/list request with code {process.poll()}")
        stderr = process.stderr.read()
        print(f"Container stderr: {stderr}")
        sys.exit(1)
    
    print("Container is still running after tools/list request. Sending call_tool request...")
    
    # Send call_tool request
    call_tool_request = {
        "jsonrpc": "2.0",
        "method": "call_tool",
        "params": {
            "tool": "list_datasets",
            "params": {
                "projectId": "query-management-and-answering"
            }
        },
        "id": 3
    }
    
    process.stdin.write(json.dumps(call_tool_request) + "\n")
    process.stdin.flush()
    
    # Wait for response
    time.sleep(2)
    
    # Check if the container is still running after call_tool
    if process.poll() is not None:
        print(f"Error: Container exited after call_tool request with code {process.poll()}")
        stderr = process.stderr.read()
        print(f"Container stderr: {stderr}")
        sys.exit(1)
    
    print("Container is still running after call_tool request.")
    print("\n✅ Test passed! The Docker container stays alive with stdio transport.")
    
    # Clean up
    print("Terminating container...")
    process.terminate()
    process.wait(timeout=5)
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
