"""
Test script for the MCP BigQuery server with real BigQuery operations.
This script demonstrates direct usage of the MCP client to test BigQuery operations.
"""
import asyncio
import json
import logging
import os
import sys
from typing import Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

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

async def test_bigquery_operations():
    """Test BigQuery operations using the MCP client."""
    from mcp.client import ClientSession
    from mcp.stdio_client import StdioTransport
    from mcp_bigquery_server.server import BigQueryMCPServer
    
    logger.info("Starting MCP BigQuery server...")
    server = BigQueryMCPServer(
        expose_resources=True,
        http_enabled=False,
    )
    
    server_task = asyncio.create_task(server.start())
    
    await asyncio.sleep(1)
    
    transport = StdioTransport()
    client = ClientSession(transport=transport)
    
    try:
        logger.info("Initializing connection...")
        init_result = await client.initialize(
            protocol_version="2025-03-26",
            capabilities={
                "tools": {},
                "logging": {},
            },
        )
        logger.info(f"Server info: {init_result.server_info}")
        logger.info(f"Supported capabilities: {init_result.capabilities}")
        
        await client.send_initialized()
        
        await client.call_method(
            "logging/setLevel",
            params={"level": "info"},
        )
        
        logger.info("Listing available tools...")
        tools_result = await client.call_method("tools/list")
        logger.info(f"Available tools: {[tool['name'] for tool in tools_result.get('tools', [])]}")
        
        logger.info("Testing execute_query tool...")
        query_result = await client.call_method(
            "tools/call",
            params={
                "name": "execute_query",
                "args": {
                    "projectId": project_id,
                    "sql": "SELECT 1 as test",
                    "dryRun": True,
                },
            },
        )
        logger.info(f"Query result: {query_result}")
        
        logger.info("Testing list_datasets tool...")
        datasets_result = await client.call_method(
            "tools/call",
            params={
                "name": "list_datasets",
                "args": {
                    "projectId": project_id,
                },
            },
        )
        logger.info(f"Datasets result: {datasets_result}")
        
        logger.info("Test completed successfully!")
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise
    finally:
        await client.close()
        
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass

def main():
    """Main entry point for the test script."""
    try:
        asyncio.run(test_bigquery_operations())
    except Exception as e:
        logger.error(f"Test failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
