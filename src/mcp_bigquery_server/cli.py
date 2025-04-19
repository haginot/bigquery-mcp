"""Command-line interface for the MCP BigQuery server."""
from mcp_bigquery_server.server_direct_stdio import main

def cli_main():
    """Entry point for the CLI."""
    main()

if __name__ == "__main__":
    cli_main()
