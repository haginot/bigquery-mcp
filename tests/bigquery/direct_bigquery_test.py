"""
Direct test script for BigQuery operations.
This script tests BigQuery operations directly without using the MCP server.
"""
import json
import logging
import os
import sys

from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("direct-bigquery-test")

if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    logger.error("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
    logger.error("Please set this variable to your service account key file path.")
    sys.exit(1)

try:
    with open(os.environ["GOOGLE_APPLICATION_CREDENTIALS"], "r") as f:
        service_account = json.load(f)
        project_id = service_account.get("project_id")
        if not project_id:
            logger.error("Could not find project_id in service account key file.")
            sys.exit(1)
        logger.info(f"Using project ID: {project_id}")
except Exception as e:
    logger.error(f"Error reading service account key file: {e}")
    sys.exit(1)

def test_bigquery_operations():
    """Test BigQuery operations directly."""
    try:
        logger.info("Creating BigQuery client...")
        client = bigquery.Client()
        
        logger.info("Testing query execution...")
        query = "SELECT 1 as test"
        
        job_config = bigquery.QueryJobConfig(dry_run=True)
        query_job = client.query(query, job_config=job_config)
        logger.info(f"Dry run processed {query_job.total_bytes_processed} bytes")
        
        query_job = client.query(query)
        results = query_job.result()
        rows = list(results)
        logger.info(f"Query returned {len(rows)} rows")
        for row in rows:
            logger.info(f"Row: {row}")
        
        logger.info("Testing list_datasets...")
        datasets = list(client.list_datasets())
        logger.info(f"Found {len(datasets)} datasets:")
        for dataset in datasets[:5]:  # Show first 5 datasets
            logger.info(f"- {dataset.dataset_id}")
        
        if datasets:
            dataset_id = datasets[0].dataset_id
            tables = list(client.list_tables(dataset_id))
            
            if tables:
                table_id = tables[0].table_id
                logger.info(f"Testing get_table_schema for {dataset_id}.{table_id}...")
                
                table = client.get_table(f"{dataset_id}.{table_id}")
                logger.info(f"Table schema has {len(table.schema)} fields")
                for field in table.schema[:5]:  # Show first 5 fields
                    logger.info(f"- {field.name} ({field.field_type})")
        
        logger.info("Test completed successfully!")
        return True
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False

def main():
    """Main entry point for the test script."""
    success = test_bigquery_operations()
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
