"""
MCP BigQuery Server with direct stdio handling for Claude Desktop compatibility.
Implements MCP specification rev 2025-03-26 using FastMCP for tool registration
but manual stdio handling for better Docker compatibility.
"""
import argparse
import json
import logging
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from fastmcp import FastMCP
from mcp_bigquery_server.utils import qualify_information_schema_query
from mcp_bigquery_server.env_utils import (
    get_project_id_from_env,
    get_location_from_env,
    get_credentials_path_from_env,
    load_credentials_from_file,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("mcp-bigquery-server")

sys.stdout.reconfigure(write_through=True)


class BigQueryMCPServer:
    """MCP server implementation for Google BigQuery with direct stdio handling."""

    def __init__(
        self,
        expose_resources: bool = False,
        http_enabled: bool = False,
        host: str = "localhost",
        port: int = 8000,
        query_timeout_ms: int = 30000,
        default_project_id: Optional[str] = None,
        default_location: Optional[str] = None,
    ):
        """Initialize the BigQuery MCP server.

        Args:
            expose_resources: Whether to expose BigQuery schemas as resources.
            http_enabled: Whether to enable HTTP transport.
            host: Host to bind HTTP server to.
            port: Port to bind HTTP server to.
            query_timeout_ms: Timeout for BigQuery queries in milliseconds.
            default_project_id: Default Google Cloud project ID to use.
            default_location: Default BigQuery location/region to use.
        """
        self.expose_resources = expose_resources
        self.http_enabled = http_enabled
        self.host = host
        self.port = port
        self.query_timeout_ms = query_timeout_ms
        
        self.default_project_id = default_project_id or get_project_id_from_env()
        self.default_location = default_location or get_location_from_env()
        self.credentials_path = get_credentials_path_from_env()
        
        logger.info(f"Using project ID: {self.default_project_id}")
        logger.info(f"Using location: {self.default_location}")
        logger.info(f"Using credentials path: {self.credentials_path}")
        
        credentials = None
        if self.credentials_path:
            try:
                credentials = load_credentials_from_file(self.credentials_path)
                
                if not self.default_project_id and credentials:
                    self.default_project_id = credentials.project_id
                    logger.info(f"Using project ID from service account: {self.default_project_id}")
            except FileNotFoundError as e:
                logger.error(f"Error loading credentials: {e}")
                raise
        
        self.bq_client = bigquery.Client(
            project=self.default_project_id,
            credentials=credentials,
        )
        
        logger.info(
            f"BigQuery client initialized for project '{self.bq_client.project}' "
            f"using service account '{self.bq_client._credentials.service_account_email}'"
        )

        self.mcp = FastMCP(
            name="mcp-bigquery-server"
        )

        self._register_tools()

    def _register_tools(self) -> None:
        """Register all BigQuery tools with the MCP server using decorators."""
        
        @self.mcp.tool()
        async def execute_query(
            sql: str,
            projectId: Optional[str] = None,
            location: Optional[str] = None,
            params: Optional[Dict[str, Any]] = None,
            dryRun: bool = False,
        ) -> Dict[str, Any]:
            """Submit a SQL query to BigQuery, optionally as dry-run."""
            try:
                project = projectId or self.default_project_id
                loc = location or self.default_location
                
                job_config = bigquery.QueryJobConfig(
                    dry_run=dryRun,
                    use_query_cache=True,
                )

                if params:
                    pass  # Handle query parameters if needed

                query_job = self.bq_client.query(
                    sql,
                    job_config=job_config,
                    project=project,
                    location=loc,
                )

                query_job.result(timeout=self.query_timeout_ms / 1000)

                if dryRun:
                    return {
                        "bytesProcessed": query_job.total_bytes_processed,
                        "isDryRun": True,
                        "projectId": project,
                        "location": loc
                    }
                else:
                    return {
                        "jobId": query_job.job_id,
                        "status": query_job.state,
                        "bytesProcessed": query_job.total_bytes_processed,
                        "projectId": project,
                        "location": loc
                    }
            except Exception as e:
                logger.error(f"Error executing query: {e}")
                raise Exception(f"BigQuery error: {str(e)}")

        @self.mcp.tool()
        async def list_datasets(
            projectId: Optional[str] = None,
            location: Optional[str] = None,
        ) -> Dict[str, Any]:
            """List all datasets in a project."""
            try:
                project = projectId or self.default_project_id
                
                datasets = list(self.bq_client.list_datasets(project=project))
                
                dataset_list = [
                    {
                        "id": ds.dataset_id,
                        "projectId": ds.project,
                        "location": ds.location,
                    }
                    for ds in datasets
                ]
                
                return {
                    "datasets": dataset_list,
                    "projectId": project,
                    "location": location
                }
            except Exception as e:
                logger.error(f"Error listing datasets: {e}")
                raise Exception(f"BigQuery error: {str(e)}")

    def send_response(self, id: int, result: Any, is_tools_call: bool = False) -> None:
        """Send a JSON-RPC response.
        
        Args:
            id: The request ID to respond to.
            result: The result data to send.
            is_tools_call: Whether this is a response to a tools/call request,
                which requires special formatting for Claude Desktop.
        """
        if id is None:
            id = 0
            
        if is_tools_call:
            result_json = json.dumps(result)
            formatted_result = {
                "content": [
                    {
                        "type": "text",
                        "text": result_json
                    }
                ]
            }
            response = {
                "jsonrpc": "2.0",
                "result": formatted_result,
                "id": id,
            }
        else:
            response = {
                "jsonrpc": "2.0",
                "result": result,
                "id": id,
            }
            
        json_str = json.dumps(response)
        logger.info(f"Sending response: {json_str}")
        print(json_str, flush=True)

    def send_error(self, id: int, code: int, message: str) -> None:
        """Send a JSON-RPC error response."""
        if id is None:
            id = 0
            
        response = {
            "jsonrpc": "2.0",
            "error": {
                "code": code,
                "message": message,
            },
            "id": id,
        }
        json_str = json.dumps(response)
        logger.info(f"Sending error: {json_str}")
        print(json_str, flush=True)

    def handle_initialize(self, params: Dict[str, Any], request_id: int) -> None:
        """Handle initialize request."""
        logger.info(f"Handling initialize request: {params}")
        
        self.send_response(request_id, {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "serverInfo": {
                "name": "mcp-bigquery-server",
                "version": "1.0.0",
            },
        })

    def handle_tools_list(self, params: Dict[str, Any], request_id: int) -> None:
        """Handle tools/list request."""
        logger.info(f"Handling tools/list request: {params}")
        
        tools = [
            {
                "name": "execute_query",
                "description": "Submit a SQL query to BigQuery, optionally as dry-run",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "projectId": {"type": "string"},
                        "location": {"type": "string"},
                        "sql": {"type": "string"},
                        "params": {
                            "type": "object",
                            "additionalProperties": True,
                        },
                        "dryRun": {"type": "boolean"},
                    },
                    "required": ["sql"],
                },
            },
            {
                "name": "list_datasets",
                "description": "List all datasets in a project",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "projectId": {"type": "string"},
                        "location": {"type": "string"},
                    },
                },
            },
        ]
        
        self.send_response(request_id, {"tools": tools})

    async def handle_call_tool(self, params: Dict[str, Any], request_id: int) -> None:
        """Handle call_tool request."""
        tool_name = params.get("tool")
        tool_params = params.get("params", {})
        
        logger.info(f"Handling call_tool request: {tool_name} with params {tool_params}")
        
        try:
            if tool_name == "execute_query":
                sql = tool_params.get("sql")
                project_id = tool_params.get("projectId") or self.default_project_id
                location = tool_params.get("location") or self.default_location
                query_params = tool_params.get("params")
                dry_run = tool_params.get("dryRun", False)
                
                region_specific = False
                region_location = None
                
                if "INFORMATION_SCHEMA" in sql.upper():
                    region_match = re.search(r'FROM\s+`?region-([a-z0-9-]+)`?\.INFORMATION_SCHEMA', sql, re.IGNORECASE)
                    if region_match:
                        region_specific = True
                        region_code = region_match.group(1)
                        region_location = region_code.upper()
                        logger.info(f"Detected region-specific query for region: {region_code}, using location: {region_location}")
                    
                    logger.info(f"Transforming INFORMATION_SCHEMA query: {sql}")
                    sql = qualify_information_schema_query(sql, project_id)
                    logger.info(f"Transformed query: {sql}")
                
                if region_specific and region_location:
                    logger.info(f"Using region-specific location: {region_location}")
                    location = region_location
                
                job_config = bigquery.QueryJobConfig(
                    dry_run=dry_run,
                    use_query_cache=True,
                )
                
                query_job = self.bq_client.query(
                    sql,
                    job_config=job_config,
                    project=project_id,
                    location=location,
                )
                
                query_job.result(timeout=self.query_timeout_ms / 1000)
                
                if dry_run:
                    result = {
                        "bytesProcessed": query_job.total_bytes_processed,
                        "isDryRun": True,
                        "projectId": project_id,
                        "location": location
                    }
                else:
                    result = {
                        "jobId": query_job.job_id,
                        "status": query_job.state,
                        "bytesProcessed": query_job.total_bytes_processed,
                        "projectId": project_id,
                        "location": location
                    }
                
                self.send_response(request_id, result, is_tools_call=True)
                
            elif tool_name == "list_datasets":
                project_id = tool_params.get("projectId") or self.default_project_id
                location = tool_params.get("location") or self.default_location
                
                datasets = list(self.bq_client.list_datasets(project=project_id))
                
                dataset_list = []
                for ds in datasets:
                    try:
                        dataset_ref = self.bq_client.dataset(ds.dataset_id, project=ds.project)
                        dataset = self.bq_client.get_dataset(dataset_ref)
                        ds_location = dataset.location
                    except Exception as e:
                        logger.warning(f"Could not get location for dataset {ds.dataset_id}: {e}")
                        ds_location = None
                        
                    dataset_list.append({
                        "id": ds.dataset_id,
                        "projectId": ds.project,
                        "location": ds_location,
                    })
                
                result = {
                    "datasets": dataset_list,
                }
                
                self.send_response(request_id, result, is_tools_call=True)
                
            else:
                self.send_error(request_id, -32601, f"Unknown tool: {tool_name}")
                
        except Exception as e:
            logger.error(f"Error calling tool: {e}")
            self.send_error(request_id, -32603, f"Error calling tool: {str(e)}")

    def start_http(self) -> None:
        """Start the MCP server with HTTP transport."""
        logger.info(f"Starting HTTP server on {self.host}:{self.port}...")
        self.mcp.run(transport="sse", host=self.host, port=self.port)

    def start_stdio(self) -> None:
        """Start the MCP server with stdio transport."""
        logger.info("Starting stdio server...")
        
        while True:
            try:
                line = sys.stdin.readline()
                if not line:
                    logger.info("Received EOF, exiting...")
                    break
                
                try:
                    request = json.loads(line)
                    method = request.get("method")
                    params = request.get("params", {})
                    request_id = request.get("id")
                    
                    logger.info(f"Received request: {method} with id {request_id}")
                    
                    if request_id is None:
                        request_id = 0
                        
                    if method == "initialize":
                        self.handle_initialize(params, request_id)
                    elif method == "tools/list":
                        self.handle_tools_list(params, request_id)
                    elif method == "call_tool" or method == "tools/call":
                        import asyncio
                        if method == "tools/call":
                            tool_name = params.get("name")
                            tool_params = params.get("arguments", {})
                            params = {"tool": tool_name, "params": tool_params}
                            logger.info(f"Converted tools/call to call_tool format: {params}")
                        asyncio.run(self.handle_call_tool(params, request_id))
                    elif method == "notifications/initialized" or method.startswith("notifications/"):
                        logger.info(f"Received notification: {method}")
                    elif method == "resources/list":
                        self.send_response(request_id, {"resources": []})
                    elif method == "prompts/list":
                        self.send_response(request_id, {"prompts": []})
                    else:
                        self.send_error(request_id, -32601, f"Method not found: {method}")
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON: {line}")
                    continue
                
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, exiting...")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                continue

    def start(self) -> None:
        """Start the MCP server with the configured transport."""
        if self.http_enabled:
            self.start_http()
        else:
            self.start_stdio()


def main():
    """Main entry point for the MCP BigQuery server."""
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
    parser.add_argument(
        "--project-id",
        help="Default Google Cloud project ID to use",
    )
    parser.add_argument(
        "--location",
        help="Default BigQuery location/region to use (e.g., 'US', 'asia-northeast1')",
    )
    args = parser.parse_args()

    logger.info("Starting BigQuery MCP server with direct stdio handling...")
    
    server = BigQueryMCPServer(
        expose_resources=args.expose_resources,
        http_enabled=args.http,
        host=args.host,
        port=args.port,
        query_timeout_ms=args.query_timeout_ms,
        default_project_id=args.project_id,
        default_location=args.location,
    )
    server.start()


if __name__ == "__main__":
    main()
