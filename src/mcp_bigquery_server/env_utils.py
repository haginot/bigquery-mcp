"""
Utility functions for environment variable handling in the BigQuery MCP server.
"""
import os
import logging

logger = logging.getLogger("mcp-bigquery-server")

def get_env_or_default(env_var: str, default=None):
    """
    Get an environment variable value or return a default.
    
    Args:
        env_var: Name of the environment variable
        default: Default value if environment variable is not set
        
    Returns:
        Value of the environment variable or default
    """
    value = os.environ.get(env_var)
    if value:
        logger.info(f"Using environment variable {env_var}={value}")
    return value or default

def get_project_id_from_env():
    """
    Get the Google Cloud project ID from environment variables.
    
    Returns:
        Project ID from environment variables or None
    """
    return get_env_or_default("PROJECT_ID")

def get_location_from_env():
    """
    Get the BigQuery location from environment variables.
    
    Returns:
        Location from environment variables or None
    """
    return get_env_or_default("LOCATION")

def get_credentials_path_from_env():
    """
    Get the Google Cloud credentials path from environment variables.
    
    Returns:
        Credentials path from environment variables or None
    """
    return get_env_or_default("GOOGLE_APPLICATION_CREDENTIALS")
