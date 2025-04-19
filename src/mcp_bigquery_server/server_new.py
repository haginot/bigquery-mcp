"""
MCP BigQuery Server - A fully-compliant Model Context Protocol server for Google BigQuery.
Implements MCP specification rev 2025-03-26.
"""
import asyncio
import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Union, AsyncGenerator

from google.cloud import bigquery
from mcp.server import Server
from mcp import Tool, Resource

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
        query_timeout_ms: int = 30000,
    ):
        """Initialize the BigQuery MCP server.

        Args:
            expose_resources: Whether to expose BigQuery schemas as resources.
            query_timeout_ms: Timeout for BigQuery queries in milliseconds.
        """
        self.expose_resources = expose_resources
        self.query_timeout_ms = query_timeout_ms
        self.bq_client = bigquery.Client()
        
        self.server = Server(
            name="mcp-bigquery-server",
            version="1.0.0",
            lifespan=self._server_lifespan,
        )
        
        self._register_tools()

    @asynccontextmanager
    async def _server_lifespan(self, server: Server):
        """Server lifespan context manager."""
        logger.info("Starting BigQuery MCP server...")
        
        try:
            yield
        finally:
            logger.info("Shutting down BigQuery MCP server...")

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

            loop = asyncio.get_running_loop()
            query_job = await loop.run_in_executor(
                None,
                lambda: self.bq_client.query(
                    sql,
                    job_config=job_config,
                    project=project_id,
                    location=location,
                )
            )

            await loop.run_in_executor(
                None,
                lambda: query_job.result(timeout=self.query_timeout_ms / 1000)
            )

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
            
            loop = asyncio.get_running_loop()
            job = await loop.run_in_executor(
                None,
                lambda: self.bq_client.get_job(job_id)
            )
            
            return {
                "jobId": job.job_id,
                "status": job.state,
                "bytesProcessed": job.total_bytes_processed,
                "creationTime": job.created.isoformat() if job.created else None,
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
            
            loop = asyncio.get_running_loop()
            job = await loop.run_in_executor(
                None,
                lambda: self.bq_client.get_job(job_id)
            )
            
            await loop.run_in_executor(
                None,
                lambda: job.cancel()
            )
            
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
            
            loop = asyncio.get_running_loop()
            job = await loop.run_in_executor(
                None,
                lambda: self.bq_client.get_job(job_id)
            )
            
            if job.state != "DONE":
                return {
                    "jobId": job.job_id,
                    "status": job.state,
                    "message": "Job is not complete yet",
                }
            
            if job.error_result:
                raise Exception(f"Query failed: {job.error_result}")
            
            results = await loop.run_in_executor(
                None,
                lambda: job.result(start_index=offset, max_results=max_rows)
            )
            
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
                self.server.resources.register(resource)
                
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
            
            loop = asyncio.get_running_loop()
            datasets = await loop.run_in_executor(
                None,
                lambda: list(self.bq_client.list_datasets(project=project_id))
            )
            
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
            
            loop = asyncio.get_running_loop()
            table_ref = self.bq_client.dataset(dataset_id, project=project_id).table(table_id)
            table = await loop.run_in_executor(
                None,
                lambda: self.bq_client.get_table(table_ref)
            )
            
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
                self.server.resources.register(resource)
                
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

    async def start_stdio(self):
        """Start the server with stdio transport."""
        await self.server.stdio()

    async def start_http(self, host: str = "localhost", port: int = 8000):
        """Start the server with HTTP transport."""
        await self.server.http(host=host, port=port)

async def main_async():
    """Async main entry point for the MCP BigQuery server."""
    import argparse
    
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
        query_timeout_ms=args.query_timeout_ms,
    )
    
    if args.http:
        await server.start_http(host=args.host, port=args.port)
    else:
        await server.start_stdio()

def main():
    """Main entry point for the MCP BigQuery server."""
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
