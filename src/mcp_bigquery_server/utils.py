"""
MCP BigQuery Server utility functions.
"""
import re
import logging

logger = logging.getLogger("mcp-bigquery-server")

def qualify_information_schema_query(sql: str, project_id: str) -> str:
    """
    Transform INFORMATION_SCHEMA queries by properly qualifying them with backticks and project ID.
    Different INFORMATION_SCHEMA tables require different access patterns.
    
    Args:
        sql: The SQL query to transform
        project_id: The Google Cloud project ID
        
    Returns:
        Transformed SQL query
    """
    if not project_id:
        logger.warning("No project ID provided for INFORMATION_SCHEMA query transformation")
        return sql
        
    pattern = r'FROM\s+(?:`?([^`\.]+(?:-[^`\.]+)?)`?\.)?INFORMATION_SCHEMA\.([A-Za-z_]+)'
    
    def replace(match):
        dataset = match.group(1)
        info_type = match.group(2)
        
        if info_type.upper() == "DATASETS":
            if dataset and dataset.startswith('region-'):
                logger.info(f"Converting DATASETS to SCHEMATA for region-specific query")
                return f'FROM `{project_id}`.INFORMATION_SCHEMA.SCHEMATA'
            else:
                return f'FROM `{project_id}`.INFORMATION_SCHEMA.SCHEMATA'
        
        if dataset and dataset.startswith('region-'):
            logger.info(f"Detected region-specific dataset: {dataset}")
            return f'FROM INFORMATION_SCHEMA.{info_type}'
        elif dataset:
            return f'FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.{info_type}`'
        else:
            return f'FROM `{project_id}.INFORMATION_SCHEMA.{info_type}`'
    
    return re.sub(pattern, replace, sql, flags=re.IGNORECASE)
