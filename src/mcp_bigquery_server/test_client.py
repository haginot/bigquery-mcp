"""
Test client for the MCP BigQuery server.
This script demonstrates how to interact with the server using the MCP Python SDK.
"""
import argparse
import asyncio
import json
import logging
import sys
from typing import Any, Dict, List, Optional

from mcp import MCPClient, StdioTransport, StreamableHTTPTransport

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("mcp-bigquery-test-client")


async def run_test(
    transport_type: str = "stdio",
    host: str = "localhost",
    port: int = 8000,
    project_id: Optional[str] = None,
):
    """Run a test against the MCP BigQuery server."""
    if transport_type == "stdio":
        transport = StdioTransport()
    elif transport_type == "http":
        transport = StreamableHTTPTransport(
            url=f"http://{host}:{port}/mcp",
        )
    else:
        raise ValueError(f"Unsupported transport type: {transport_type}")

    client = MCPClient(transport=transport)

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

        if project_id:
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
        else:
            logger.warning("No project_id provided, skipping BigQuery API tests")

    finally:
        await client.close()


def main():
    """Main entry point for the test client."""
    parser = argparse.ArgumentParser(description="MCP BigQuery Test Client")
    parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default="stdio",
        help="Transport type to use",
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Host for HTTP transport",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for HTTP transport",
    )
    parser.add_argument(
        "--project-id",
        help="Google Cloud project ID for testing BigQuery API calls",
    )
    args = parser.parse_args()

    asyncio.run(
        run_test(
            transport_type=args.transport,
            host=args.host,
            port=args.port,
            project_id=args.project_id,
        )
    )


if __name__ == "__main__":
    main()
