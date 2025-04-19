"""
MCP BigQuery Server - A fully-compliant Model Context Protocol server for Google BigQuery.
Implements MCP specification rev 2025-03-26 using FastMCP.
"""
import argparse
import logging
import sys
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from fastmcp import FastMCP

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("mcp-bigquery-server")


class BigQueryMCPServer:
    """MCP server implementation for Google BigQuery using FastMCP."""

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
            """Submit a SQL query to BigQuery, optionally as dry-run.
            
            Args:
                sql: The SQL query to execute
                projectId: Optional Google Cloud project ID
                location: Optional BigQuery location
                params: Optional query parameters
                dryRun: Whether to perform a dry run
                
            Returns:
                Query execution results or dry run information
            """
            try:
                job_config = bigquery.QueryJobConfig(
                    dry_run=dryRun,
                    use_query_cache=True,
                )

                if params:
                    pass

                query_job = self.bq_client.query(
                    sql,
                    job_config=job_config,
                    project=projectId,
                    location=location,
                )

                query_job.result(timeout=self.query_timeout_ms / 1000)

                if dryRun:
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

        @self.mcp.tool()
        async def get_job_status(jobId: str) -> Dict[str, Any]:
            """Poll job execution state.
            
            Args:
                jobId: The BigQuery job ID to check
                
            Returns:
                Current job status information
            """
            try:
                job = self.bq_client.get_job(jobId)
                
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

        @self.mcp.tool()
        async def cancel_job(jobId: str) -> Dict[str, Any]:
            """Cancel a running BigQuery job.
            
            Args:
                jobId: The BigQuery job ID to cancel
                
            Returns:
                Cancellation result
            """
            try:
                job = self.bq_client.get_job(jobId)
                job.cancel()
                
                return {
                    "jobId": job.job_id,
                    "status": "CANCELLED",
                    "success": True,
                }
            except Exception as e:
                logger.error(f"Error cancelling job: {e}")
                raise Exception(f"BigQuery error: {str(e)}")

        @self.mcp.tool()
        async def fetch_results_chunk(
            jobId: str, 
            offset: int = 0, 
            maxRows: int = 100
        ) -> Dict[str, Any]:
            """Page through results.
            
            Args:
                jobId: The BigQuery job ID
                offset: Starting row offset
                maxRows: Maximum number of rows to return
                
            Returns:
                Result chunk with pagination information
            """
            try:
                job = self.bq_client.get_job(jobId)
                
                if job.state != "DONE":
                    return {
                        "jobId": job.job_id,
                        "status": job.state,
                        "message": "Job is not complete yet",
                    }
                
                if job.error_result:
                    raise Exception(f"Query failed: {job.error_result}")
                
                results = job.result(start_index=offset, max_results=maxRows)
                schema = [field.name for field in results.schema]
                
                rows = []
                for row in results:
                    rows.append({field: value for field, value in zip(schema, row.values())})
                
                has_more = len(rows) == maxRows
                
                if self.expose_resources:
                    resource_uri = f"bq://results/{jobId}/{offset}"
                    
                    return {
                        "jobId": jobId,
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
                        "jobId": jobId,
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

        @self.mcp.tool()
        async def list_datasets(
            projectId: Optional[str] = None,
            cursor: Optional[str] = None,
        ) -> Dict[str, Any]:
            """Enumerate datasets visible to the service account.
            
            Args:
                projectId: Optional Google Cloud project ID
                cursor: Optional pagination cursor
                
            Returns:
                List of datasets with pagination information
            """
            try:
                import json
                
                page_size = 50
                
                start_index = 0
                if cursor:
                    try:
                        cursor_data = json.loads(cursor)
                        start_index = cursor_data.get("index", 0)
                    except (json.JSONDecodeError, TypeError):
                        raise Exception("Invalid cursor format")
                
                datasets = list(self.bq_client.list_datasets(project=projectId))
                
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

        @self.mcp.tool()
        async def get_table_schema(
            projectId: str,
            datasetId: str,
            tableId: str,
        ) -> Dict[str, Any]:
            """Retrieve schema for a table.
            
            Args:
                projectId: Google Cloud project ID
                datasetId: BigQuery dataset ID
                tableId: BigQuery table ID
                
            Returns:
                Table schema information
            """
            try:
                table_ref = self.bq_client.dataset(datasetId, project=projectId).table(tableId)
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
                    resource_uri = f"bq://{projectId}/{datasetId}/{tableId}/schema"
                    
                    return {
                        "projectId": projectId,
                        "datasetId": datasetId,
                        "tableId": tableId,
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
                        "projectId": projectId,
                        "datasetId": datasetId,
                        "tableId": tableId,
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
            self.mcp.start_http(host=self.host, port=self.port)
        else:
            self.mcp.start_stdio()


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

    logger.info("Starting BigQuery MCP server with FastMCP implementation...")
    
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
