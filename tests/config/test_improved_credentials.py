"""
Test script to verify improved credential handling in the BigQuery MCP server.
"""
import os
import sys
import json
import logging
import argparse
from google.cloud import bigquery
from google.oauth2 import service_account

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

from mcp_bigquery_server.env_utils import (
    get_project_id_from_env,
    get_location_from_env,
    get_credentials_path_from_env,
    load_credentials_from_file,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("test-credentials")

def test_load_credentials():
    """Test loading credentials from file."""
    logger.info("Testing credential loading...")
    
    creds_path = get_credentials_path_from_env()
    logger.info(f"Credentials path from env: {creds_path}")
    
    if not creds_path:
        logger.error("No credentials path found in environment variables")
        return False
    
    try:
        credentials = load_credentials_from_file(creds_path)
        logger.info(f"Successfully loaded credentials for: {credentials.service_account_email}")
        logger.info(f"Project ID from credentials: {credentials.project_id}")
        
        client = bigquery.Client(
            project=credentials.project_id,
            credentials=credentials,
        )
        logger.info(f"Successfully created BigQuery client for project: {client.project}")
        
        query_job = client.query("SELECT 1 as test")
        results = list(query_job.result())
        logger.info(f"Query results: {results}")
        
        return True
    except Exception as e:
        logger.error(f"Error testing credentials: {e}")
        return False

def test_project_id_fallback():
    """Test project ID fallback from credentials when not in environment."""
    logger.info("Testing project ID fallback...")
    
    original_project_id = os.environ.get("PROJECT_ID")
    
    try:
        if "PROJECT_ID" in os.environ:
            del os.environ["PROJECT_ID"]
        
        creds_path = get_credentials_path_from_env()
        if not creds_path:
            logger.error("No credentials path found in environment variables")
            return False
        
        credentials = load_credentials_from_file(creds_path)
        
        client = bigquery.Client(credentials=credentials)
        
        logger.info(f"Project ID from credentials: {credentials.project_id}")
        logger.info(f"Project ID used by client: {client.project}")
        
        if credentials.project_id == client.project:
            logger.info("Project ID fallback successful!")
            return True
        else:
            logger.error(f"Project ID mismatch: {credentials.project_id} vs {client.project}")
            return False
    
    except Exception as e:
        logger.error(f"Error testing project ID fallback: {e}")
        return False
    
    finally:
        if original_project_id:
            os.environ["PROJECT_ID"] = original_project_id

def main():
    """Main test function."""
    parser = argparse.ArgumentParser(description="Test improved credential handling")
    args = parser.parse_args()
    
    logger.info("Starting credential handling tests...")
    
    creds_success = test_load_credentials()
    logger.info(f"Credential loading test: {'PASSED' if creds_success else 'FAILED'}")
    
    fallback_success = test_project_id_fallback()
    logger.info(f"Project ID fallback test: {'PASSED' if fallback_success else 'FAILED'}")
    
    if creds_success and fallback_success:
        logger.info("All tests PASSED!")
        return 0
    else:
        logger.error("Some tests FAILED!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
