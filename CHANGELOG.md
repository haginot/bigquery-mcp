# Changelog

All notable changes to the BigQuery MCP Server project will be documented in this file.

## [Unreleased]

## [1.0.0] - 2025-04-20

### Added
- Initial implementation: Compliant with Model Context Protocol (MCP) specification rev 2025-03-26
- Support for stdio transport (default) and HTTP transport
- Implementation of MCP Tools providing access to BigQuery operations
- Pagination support for long result sets
- Implementation of logging utilities
- Error handling according to JSON-RPC standards
- Docker support for easy deployment
- Direct stdio server implementation for improved Claude Desktop compatibility
- `execute_query_with_results` tool: Execute SQL queries with immediate results

### Fixed
- Fixed Claude Desktop compatibility issues in Docker containers
- Fixed early termination issues with stdio transport in containers
- Improved INFORMATION_SCHEMA query handling
- Improved service account credential handling
- Fixed Claude Desktop response format
- Added support for configuration via environment variables (project ID and location)

### Changed
- Optimized implementation using FastMCP
- Organized test scripts into functional directories
- Removed and cleaned up redundant server implementations
