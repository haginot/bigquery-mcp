"""
Utility functions for environment variable handling in the BigQuery MCP server.
"""
import os
import logging
from google.oauth2 import service_account

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

def load_credentials_from_file(path: str):
    """
    Load service-account credentials or raise FileNotFoundError.
    
    Args:
        path: Path to the service account JSON file
        
    Returns:
        Service account credentials object
        
    Raises:
        FileNotFoundError: If the credentials file doesn't exist
    """
    if not path or not os.path.exists(path):
        raise FileNotFoundError(
            f"GOOGLE_APPLICATION_CREDENTIALS file not found: {path}"
        )
    
    creds = service_account.Credentials.from_service_account_file(
        path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    logger.info(f"Loaded credentials for service account: {creds.service_account_email}")
    return creds
