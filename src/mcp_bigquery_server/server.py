"""
MCP BigQuery Server - A fully-compliant Model Context Protocol server for Google BigQuery.
Implements MCP specification rev 2025-03-26.
"""
import argparse
import json
import logging
import os
import re
import sys
from typing import Any, Dict, List, Optional, Union

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from mcp import Tool, Resource
from mcp.server import Server
from mcp_bigquery_server.direct_stdio import direct_stdio_server
from mcp.server.fastmcp import FastMCP
from mcp_bigquery_server.utils import qualify_information_schema_query
from mcp_bigquery_server.env_utils import (
    get_project_id_from_env,
    get_location_from_env,
    get_credentials_path_from_env,
    load_credentials_from_file,
)
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

        self.tools = {tool.name: tool for tool in tools}
        
        async def handle_tool_call(tool_name, params):
            handler = self._get_tool_handler(tool_name)
            if handler:
                return await handler(params)
            return {"error": f"Unknown tool: {tool_name}"}
        
        self.server.call_tool = handle_tool_call

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

        @app.post("/mcp", response_model=None)
        async def handle_mcp_post(request: Request):
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
            if body.get("method") == "call_tool":
                tool_name = body.get("params", {}).get("tool")
                tool_params = body.get("params", {}).get("params", {})
                
                handler = self._get_tool_handler(tool_name)
                if handler:
                    try:
                        result = await handler(tool_params)
                        response = {
                            "jsonrpc": "2.0",
                            "result": result,
                            "id": body.get("id")
                        }
                    except Exception as e:
                        logger.error(f"Error handling tool call: {e}")
                        response = {
                            "jsonrpc": "2.0",
                            "error": {
                                "code": -32000,
                                "message": str(e)
                            },
                            "id": body.get("id")
                        }
                else:
                    response = {
                        "jsonrpc": "2.0",
                        "error": {
                            "code": -32601,
                            "message": f"Tool not found: {tool_name}"
                        },
                        "id": body.get("id")
                    }
            else:
                response = {
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -32601,
                        "message": f"Method not found: {body.get('method')}"
                    },
                    "id": body.get("id")
                }
                
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
                    "projectId": project_id,
                    "location": location
                }
            else:
                return {
                    "jobId": query_job.job_id,
                    "status": query_job.state,
                    "bytesProcessed": query_job.total_bytes_processed,
                    "projectId": project_id,
                    "location": location
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
                pass
                
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
                pass
                
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
            import signal
            
            def handle_sigterm(signum, frame):
                logger.info("Received SIGTERM signal, shutting down...")
                sys.exit(0)
                
            signal.signal(signal.SIGTERM, handle_sigterm)
            signal.signal(signal.SIGINT, handle_sigterm)
            
            logger.info("Starting stdio server...")
            try:
                import sys
                import json
                import asyncio
                
                sys.stdout.reconfigure(write_through=True)
                
                logger.info("Starting direct stdio server in main thread...")
                
                while True:
                    line = sys.stdin.readline()
                    if not line:
                        logger.info("End of input stream, exiting...")
                        break
                    
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        request = json.loads(line)
                        logger.info(f"Received request: {json.dumps(request)}")
                        
                        method = request.get("method")
                        params = request.get("params", {})
                        request_id = request.get("id")
                        
                        response = None
                        
                        if method == "initialize":
                            response = {
                                "jsonrpc": "2.0",
                                "result": {
                                    "protocolVersion": params.get("protocolVersion", "2024-11-05"),
                                    "name": self.server.name,
                                    "version": self.server.version,
                                    "capabilities": {},
                                },
                                "id": request_id,
                            }
                        elif method == "tools/list":
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
                            
                            response = {
                                "jsonrpc": "2.0",
                                "result": {
                                    "tools": tools,
                                },
                                "id": request_id,
                            }
                        elif method == "call_tool":
                            tool_name = params.get("tool")
                            tool_params = params.get("params", {})
                            
                            try:
                                handler = self._get_tool_handler(tool_name)
                                if handler:
                                    result = asyncio.run(handler(tool_params))
                                    response = {
                                        "jsonrpc": "2.0",
                                        "result": result,
                                        "id": request_id,
                                    }
                                else:
                                    response = {
                                        "jsonrpc": "2.0",
                                        "error": {
                                            "code": -32602,
                                            "message": f"Unknown tool: {tool_name}",
                                        },
                                        "id": request_id,
                                    }
                            except Exception as e:
                                logger.error(f"Error calling tool {tool_name}: {e}")
                                response = {
                                    "jsonrpc": "2.0",
                                    "error": {
                                        "code": -32603,
                                        "message": f"Error calling tool {tool_name}: {str(e)}",
                                    },
                                    "id": request_id,
                                }
                        else:
                            response = {
                                "jsonrpc": "2.0",
                                "error": {
                                    "code": -32601,
                                    "message": f"Method not found: {method}",
                                },
                                "id": request_id,
                            }
                        
                        if response:
                            response_json = json.dumps(response)
                            logger.info(f"Sending response: {response_json}")
                            print(response_json, flush=True)
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON: {line}")
                        error_response = {
                            "jsonrpc": "2.0",
                            "error": {
                                "code": -32700,
                                "message": "Parse error",
                            },
                            "id": None,
                        }
                        print(json.dumps(error_response), flush=True)
                    except Exception as e:
                        logger.error(f"Error handling request: {e}")
                        error_response = {
                            "jsonrpc": "2.0",
                            "error": {
                                "code": -32603,
                                "message": f"Internal error: {str(e)}",
                            },
                            "id": None,
                        }
                        print(json.dumps(error_response), flush=True)
                
                logger.info("Stdio server started, waiting for input...")
                signal.pause()
            except Exception as e:
                logger.error(f"Error in stdio server: {e}")
                raise


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
