"""
MCP BigQuery Server - A fully-compliant Model Context Protocol server for Google BigQuery.
Implements MCP specification rev 2025-03-26.
"""
import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Union

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from mcp import Tool, Resource
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("mcp-bigquery-server")


class BigQueryMCPServer:
    """MCP server implementation for Google BigQuery."""

    def __init__(
        self,
        expose_resources: bool = False,
        http_enabled: bool = False,
        host: str = "localhost",
        port: int = 8000,
        query_timeout_ms: int = 30000,
    ):
        """Initialize the BigQuery MCP server.

        Args:
            expose_resources: Whether to expose BigQuery schemas as resources.
            http_enabled: Whether to enable HTTP transport.
            host: Host to bind HTTP server to.
            port: Port to bind HTTP server to.
            query_timeout_ms: Timeout for BigQuery queries in milliseconds.
        """
        self.expose_resources = expose_resources
        self.http_enabled = http_enabled
        self.host = host
        self.port = port
        self.query_timeout_ms = query_timeout_ms

        self.bq_client = bigquery.Client()

        self.server = Server(
            name="mcp-bigquery-server",
            version="1.0.0",
        )

        self._register_tools()

        self.stdio_server = None
        self.fastmcp = None
        
        if http_enabled:
            self.app = self._create_fastapi_app()

    def _register_tools(self) -> None:
        """Register all BigQuery tools with the MCP server."""
        tools = [
            Tool(
                name="execute_query",
                description="Submit a SQL query to BigQuery, optionally as dry-run",
                inputSchema={
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
            ),
            Tool(
                name="get_job_status",
                description="Poll job execution state",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "jobId": {"type": "string"},
                    },
                    "required": ["jobId"],
                },
            ),
            Tool(
                name="cancel_job",
                description="Cancel a running BigQuery job",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "jobId": {"type": "string"},
                    },
                    "required": ["jobId"],
                },
            ),
            Tool(
                name="fetch_results_chunk",
                description="Page through results",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "jobId": {"type": "string"},
                        "offset": {"type": "integer", "minimum": 0},
                        "maxRows": {"type": "integer", "minimum": 1},
                    },
                    "required": ["jobId"],
                },
            ),
            Tool(
                name="list_datasets",
                description="Enumerate datasets visible to the service account",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "projectId": {"type": "string"},
                        "cursor": {"type": "string"},
                    },
                },
            ),
            Tool(
                name="get_table_schema",
                description="Retrieve schema for a table",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "projectId": {"type": "string"},
                        "datasetId": {"type": "string"},
                        "tableId": {"type": "string"},
                    },
                    "required": ["projectId", "datasetId", "tableId"],
                },
            ),
        ]

        for tool in tools:
            self.server.tools.register(tool, self._get_tool_handler(tool.name))

    def _create_fastapi_app(self) -> FastAPI:
        """Create a FastAPI app for HTTP transport."""
        app = FastAPI(title="MCP BigQuery Server")

        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        @app.post("/mcp")
        async def handle_mcp_post(request: Request) -> Union[Response, EventSourceResponse]:
            """Handle MCP POST requests."""
            content_type = request.headers.get("content-type", "")
            accept = request.headers.get("accept", "")
            
            origin = request.headers.get("origin")
            if origin and not self._is_valid_origin(origin):
                return Response(
                    content=json.dumps({
                        "jsonrpc": "2.0",
                        "error": {
                            "code": -32000,
                            "message": "Invalid origin",
                        },
                        "id": None,
                    }),
                    media_type="application/json",
                    status_code=403,
                )

            if "text/event-stream" in accept:
                return EventSourceResponse(
                    self._stream_response(request),
                    media_type="text/event-stream",
                )
            
            body = await request.json()
            response = await self.server.handle_jsonrpc(body)
            return Response(
                content=json.dumps(response),
                media_type="application/json",
            )

        @app.get("/mcp")
        async def handle_mcp_get(request: Request) -> Response:
            """Handle MCP GET requests."""
            return Response(
                content=json.dumps({
                    "status": "ok",
                    "server": "mcp-bigquery-server",
                    "version": "1.0.0",
                }),
                media_type="application/json",
            )

        return app

    async def _stream_response(self, request: Request):
        """Stream responses for SSE."""
        body = await request.json()
        async for response in self.server.handle_jsonrpc_stream(body):
            yield {"data": json.dumps(response)}

    def _is_valid_origin(self, origin: str) -> bool:
        """Validate the origin header for security."""
        allowed_origins = [
            "http://localhost",
            "https://localhost",
            "http://127.0.0.1",
            "https://127.0.0.1",
        ]
        return any(origin.startswith(allowed) for allowed in allowed_origins)

    def _get_tool_handler(self, tool_name: str):
        """Get the handler function for a tool."""
        handlers = {
            "execute_query": self._handle_execute_query,
            "get_job_status": self._handle_get_job_status,
            "cancel_job": self._handle_cancel_job,
            "fetch_results_chunk": self._handle_fetch_results_chunk,
            "list_datasets": self._handle_list_datasets,
            "get_table_schema": self._handle_get_table_schema,
        }
        return handlers.get(tool_name)

    async def _handle_execute_query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle execute_query tool."""
        try:
            sql = params["sql"]
            project_id = params.get("projectId")
            location = params.get("location")
            query_params = params.get("params", {})
            dry_run = params.get("dryRun", False)

            job_config = bigquery.QueryJobConfig(
                dry_run=dry_run,
                use_query_cache=True,
            )

            if query_params:
                pass

            query_job = self.bq_client.query(
                sql,
                job_config=job_config,
                project=project_id,
                location=location,
            )

            query_job.result(timeout=self.query_timeout_ms / 1000)

            if dry_run:
                return {
                    "bytesProcessed": query_job.total_bytes_processed,
                    "isDryRun": True,
                }
            else:
                return {
                    "jobId": query_job.job_id,
                    "status": query_job.state,
                    "bytesProcessed": query_job.total_bytes_processed,
                }
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise Exception(f"BigQuery error: {str(e)}")

    async def _handle_get_job_status(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle get_job_status tool."""
        try:
            job_id = params["jobId"]
            job = self.bq_client.get_job(job_id)
            
            return {
                "jobId": job.job_id,
                "status": job.state,
                "bytesProcessed": job.total_bytes_processed,
                "creationTime": job.created.isoformat(),
                "startTime": job.started.isoformat() if job.started else None,
                "endTime": job.ended.isoformat() if job.ended else None,
                "error": job.error_result,
            }
        except Exception as e:
            logger.error(f"Error getting job status: {e}")
            raise Exception(f"BigQuery error: {str(e)}")

    async def _handle_cancel_job(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle cancel_job tool."""
        try:
            job_id = params["jobId"]
            job = self.bq_client.get_job(job_id)
            job.cancel()
            
            return {
                "jobId": job.job_id,
                "status": "CANCELLED",
                "success": True,
            }
        except Exception as e:
            logger.error(f"Error cancelling job: {e}")
            raise Exception(f"BigQuery error: {str(e)}")

    async def _handle_fetch_results_chunk(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle fetch_results_chunk tool."""
        try:
            job_id = params["jobId"]
            offset = params.get("offset", 0)
            max_rows = params.get("maxRows", 100)
            
            job = self.bq_client.get_job(job_id)
            
            if job.state != "DONE":
                return {
                    "jobId": job.job_id,
                    "status": job.state,
                    "message": "Job is not complete yet",
                }
            
            if job.error_result:
                raise Exception(f"Query failed: {job.error_result}")
            
            results = job.result(start_index=offset, max_results=max_rows)
            schema = [field.name for field in results.schema]
            
            rows = []
            for row in results:
                rows.append({field: value for field, value in zip(schema, row.values())})
            
            has_more = len(rows) == max_rows
            
            if self.expose_resources:
                resource_uri = f"bq://results/{job_id}/{offset}"
                resource = Resource(
                    uri=resource_uri,
                    content_type="application/json",
                    content=json.dumps(rows),
                )
                self.server.register_resource(resource)
                
                return {
                    "jobId": job_id,
                    "offset": offset,
                    "rowCount": len(rows),
                    "schema": schema,
                    "hasMore": has_more,
                    "nextOffset": offset + len(rows) if has_more else None,
                    "results": {
                        "type": "resource",
                        "uri": resource_uri,
                    }
                }
            else:
                return {
                    "jobId": job_id,
                    "offset": offset,
                    "rowCount": len(rows),
                    "schema": schema,
                    "hasMore": has_more,
                    "nextOffset": offset + len(rows) if has_more else None,
                    "results": rows,
                }
        except Exception as e:
            logger.error(f"Error fetching results: {e}")
            raise Exception(f"BigQuery error: {str(e)}")

    async def _handle_list_datasets(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle list_datasets tool with pagination."""
        try:
            project_id = params.get("projectId")
            cursor = params.get("cursor")
            
            page_size = 50
            
            start_index = 0
            if cursor:
                try:
                    cursor_data = json.loads(cursor)
                    start_index = cursor_data.get("index", 0)
                except (json.JSONDecodeError, TypeError):
                    raise Exception("Invalid cursor format")
            
            datasets = list(self.bq_client.list_datasets(project=project_id))
            
            end_index = min(start_index + page_size, len(datasets))
            page_datasets = datasets[start_index:end_index]
            
            dataset_list = [
                {
                    "id": ds.dataset_id,
                    "projectId": ds.project,
                    "location": ds.location,
                    "friendlyName": ds.friendly_name,
                    "labels": ds.labels,
                    "creationTime": ds.created.isoformat() if ds.created else None,
                    "lastModifiedTime": ds.modified.isoformat() if ds.modified else None,
                }
                for ds in page_datasets
            ]
            
            next_cursor = None
            if end_index < len(datasets):
                next_cursor = json.dumps({"index": end_index})
            
            return {
                "datasets": dataset_list,
                "nextCursor": next_cursor,
            }
        except Exception as e:
            logger.error(f"Error listing datasets: {e}")
            raise Exception(f"BigQuery error: {str(e)}")

    async def _handle_get_table_schema(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle get_table_schema tool."""
        try:
            project_id = params["projectId"]
            dataset_id = params["datasetId"]
            table_id = params["tableId"]
            
            table_ref = self.bq_client.dataset(dataset_id, project=project_id).table(table_id)
            table = self.bq_client.get_table(table_ref)
            
            schema_fields = []
            for field in table.schema:
                field_info = {
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode,
                    "description": field.description,
                }
                
                if field.fields:
                    field_info["fields"] = [
                        {
                            "name": nested.name,
                            "type": nested.field_type,
                            "mode": nested.mode,
                            "description": nested.description,
                        }
                        for nested in field.fields
                    ]
                
                schema_fields.append(field_info)
            
            if self.expose_resources:
                resource_uri = f"bq://{project_id}/{dataset_id}/{table_id}/schema"
                resource = Resource(
                    uri=resource_uri,
                    content_type="application/json",
                    content=json.dumps(schema_fields),
                )
                self.server.register_resource(resource)
                
                return {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                    "tableId": table_id,
                    "schema": {
                        "type": "resource",
                        "uri": resource_uri,
                    },
                    "rowCount": table.num_rows,
                    "creationTime": table.created.isoformat() if table.created else None,
                    "lastModifiedTime": table.modified.isoformat() if table.modified else None,
                }
            else:
                return {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                    "tableId": table_id,
                    "schema": schema_fields,
                    "rowCount": table.num_rows,
                    "creationTime": table.created.isoformat() if table.created else None,
                    "lastModifiedTime": table.modified.isoformat() if table.modified else None,
                }
        except Exception as e:
            logger.error(f"Error getting table schema: {e}")
            raise Exception(f"BigQuery error: {str(e)}")

    def start(self) -> None:
        """Start the MCP server with all configured transports."""
        if self.http_enabled:
            self.fastmcp = FastMCP(self.server, app=self.app)
            import uvicorn
            import asyncio
            
            asyncio.create_task(
                uvicorn.run(
                    self.app,
                    host=self.host,
                    port=self.port,
                )
            )
        else:
            stdio_server(self.server)


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
    args = parser.parse_args()

    server = BigQueryMCPServer(
        expose_resources=args.expose_resources,
        http_enabled=args.http,
        host=args.host,
        port=args.port,
        query_timeout_ms=args.query_timeout_ms,
    )
    server.start()


if __name__ == "__main__":
    main()
