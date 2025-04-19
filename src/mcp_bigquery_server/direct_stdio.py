"""
Direct stdio implementation for MCP server to fix Claude Desktop compatibility.
"""
import json
import logging
import sys
import asyncio
from typing import Any, Dict, Optional

from mcp.server import Server

logger = logging.getLogger("mcp-bigquery-server")

def direct_stdio_server(server: Server) -> None:
    """
    Run a direct stdio server implementation that immediately responds to requests.
    
    This implementation directly reads from stdin and writes to stdout without
    any threading or async complexity, ensuring immediate responses to Claude Desktop.
    """
    logger.info("Starting direct stdio server...")
    
    # Ensure stdout is unbuffered
    sys.stdout.reconfigure(write_through=True)
    
    try:
        while True:
            # Read a line from stdin
            line = sys.stdin.readline()
            if not line:
                logger.info("End of input stream, exiting...")
                break
            
            line = line.strip()
            if not line:
                continue
            
            # Parse the JSON-RPC request
            try:
                request = json.loads(line)
                logger.info(f"Received request: {json.dumps(request)}")
                
                # Handle the request
                response = handle_request(server, request)
                
                # Send the response
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
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, exiting...")
    except Exception as e:
        logger.error(f"Error in stdio server: {e}")

def handle_request(server: Server, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Handle a JSON-RPC request synchronously.
    
    This is a simplified implementation that handles the core requests needed by Claude Desktop:
    - initialize
    - tools/list
    - call_tool
    """
    method = request.get("method")
    params = request.get("params", {})
    request_id = request.get("id")
    
    if method == "initialize":
        # Handle initialize request
        return {
            "jsonrpc": "2.0",
            "result": {
                "protocolVersion": params.get("protocolVersion", "2024-11-05"),
                "name": server.name,
                "version": server.version,
                "capabilities": {},
            },
            "id": request_id,
        }
    elif method == "tools/list":
        # Handle tools/list request
        tools = []
        for tool in server.tools:
            tools.append({
                "name": tool.name,
                "description": tool.description,
                "inputSchema": tool.inputSchema,
            })
        
        return {
            "jsonrpc": "2.0",
            "result": {
                "tools": tools,
            },
            "id": request_id,
        }
    elif method == "call_tool":
        # Handle call_tool request
        tool_name = params.get("tool")
        tool_params = params.get("params", {})
        
        # Find the tool
        tool = None
        for t in server.tools:
            if t.name == tool_name:
                tool = t
                break
        
        if not tool:
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32602,
                    "message": f"Unknown tool: {tool_name}",
                },
                "id": request_id,
            }
        
        try:
            # Call the tool handler
            result = asyncio.run(server.call_tool(tool_name, tool_params))
            
            return {
                "jsonrpc": "2.0",
                "result": result,
                "id": request_id,
            }
        except Exception as e:
            logger.error(f"Error calling tool {tool_name}: {e}")
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": f"Error calling tool {tool_name}: {str(e)}",
                },
                "id": request_id,
            }
    else:
        # Unknown method
        return {
            "jsonrpc": "2.0",
            "error": {
                "code": -32601,
                "message": f"Method not found: {method}",
            },
            "id": request_id,
        }
