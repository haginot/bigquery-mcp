"""
Test script for the MCP BigQuery server with real BigQuery operations.
This script starts the server and runs a test client against it.
"""
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from typing import Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("bigquery-mcp-test")

if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    logger.error("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
    logger.error("Please set this variable to your service account key file path.")
    sys.exit(1)

try:
    with open(os.environ["GOOGLE_APPLICATION_CREDENTIALS"], "r") as f:
        service_account = json.load(f)
        project_id = service_account.get("project_id")
        if not project_id:
            logger.error("Could not find project_id in service account key file.")
            sys.exit(1)
        logger.info(f"Using project ID: {project_id}")
except Exception as e:
    logger.error(f"Error reading service account key file: {e}")
    sys.exit(1)

async def run_test_client():
    """Run the test client against the server."""
    from mcp_bigquery_server.test_client import run_test
    
    await run_test(
        transport_type="stdio",
        project_id=project_id,
    )

def main():
    """Main entry point for the test script."""
    logger.info("Starting MCP BigQuery server...")
    server_process = subprocess.Popen(
        ["poetry", "run", "python", "-m", "mcp_bigquery_server", "--expose-resources"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    
    time.sleep(2)
    
    try:
        logger.info("Running test client...")
        asyncio.run(run_test_client())
        logger.info("Test completed successfully!")
    except Exception as e:
        logger.error(f"Test failed: {e}")
    finally:
        logger.info("Stopping server...")
        server_process.terminate()
        server_process.wait(timeout=5)

if __name__ == "__main__":
    main()
