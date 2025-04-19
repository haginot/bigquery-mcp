"""Command-line interface for the MCP BigQuery server."""
import argparse
import asyncio
import sys

from mcp_bigquery_server.server_new import BigQueryMCPServer

def cli_main():
    """Entry point for the CLI."""
    parser = argparse.ArgumentParser(description="MCP BigQuery Server")
    parser.add_argument(
        "--expose-resources",
        action="store_true",
        help="Expose BigQuery schemas as resources",
    )
    
    transport_group = parser.add_mutually_exclusive_group()
    transport_group.add_argument(
        "--http",
        action="store_true",
        help="Enable HTTP transport",
    )
    transport_group.add_argument(
        "--stdio",
        action="store_true",
        help="Enable stdio transport (default)",
    )
    
    parser.add_argument(
        "--host",
        default="localhost",
        help="Host to bind HTTP server to",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind HTTP server to",
    )
    parser.add_argument(
        "--query-timeout-ms",
        type=int,
        default=30000,
        help="Timeout for BigQuery queries in milliseconds",
    )
    args = parser.parse_args()

    async def run_server():
        server = BigQueryMCPServer(
            expose_resources=args.expose_resources,
            query_timeout_ms=args.query_timeout_ms,
        )
        
        if args.http:
            await server.start_http(host=args.host, port=args.port)
        else:
            await server.start_stdio()

    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        print("Server stopped by user", file=sys.stderr)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    cli_main()
