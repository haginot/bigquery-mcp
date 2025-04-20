"""
Test script to try different approaches to accessing INFORMATION_SCHEMA tables in BigQuery.
Based on BigQuery documentation for INFORMATION_SCHEMA access patterns.
"""
import os
import sys
from google.cloud import bigquery

def main():
    key_file_path = os.path.expanduser("~/attachments/64ba55d6-602f-4772-9884-0619feefa042/amazon-study-db-c6ecbc10001a.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path
    
    project_id = "amazon-study-db"
    
    print(f"Testing INFORMATION_SCHEMA access patterns for project: {project_id}")
    print(f"Using service account key: {key_file_path}")
    
    try:
        client = bigquery.Client(project=project_id)
        
        print("\nListing datasets:")
        datasets = list(client.list_datasets())
        if datasets:
            print(f"Found {len(datasets)} datasets in project {project_id}:")
            for dataset in datasets:
                print(f"- {dataset.dataset_id}")
        else:
            print(f"No datasets found in project {project_id}")
        
        
        print("\nApproach 1: Project-level INFORMATION_SCHEMA")
        query1 = f"SELECT * FROM `{project_id}`.INFORMATION_SCHEMA.SCHEMATA LIMIT 5"
        print(f"Query: {query1}")
        try:
            query_job = client.query(query1)
            results = list(query_job.result())
            print(f"Success! Found {len(results)} results")
            if results:
                print("First result:", results[0])
        except Exception as e:
            print(f"Error: {e}")
        
        print("\nApproach 2: Region-level INFORMATION_SCHEMA")
        query2 = f"SELECT * FROM `{project_id}`.`region-us`.INFORMATION_SCHEMA.SCHEMATA LIMIT 5"
        print(f"Query: {query2}")
        try:
            query_job = client.query(query2)
            results = list(query_job.result())
            print(f"Success! Found {len(results)} results")
            if results:
                print("First result:", results[0])
        except Exception as e:
            print(f"Error: {e}")
        
        print("\nApproach 3: Using location parameter")
        query3 = "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA LIMIT 5"
        print(f"Query: {query3} (with location='us')")
        try:
            query_job = client.query(query3, location="us")
            results = list(query_job.result())
            print(f"Success! Found {len(results)} results")
            if results:
                print("First result:", results[0])
        except Exception as e:
            print(f"Error: {e}")
        
        print("\nApproach 4: Using DATASETS with location parameter")
        query4 = "SELECT * FROM INFORMATION_SCHEMA.DATASETS LIMIT 5"
        print(f"Query: {query4} (with location='us')")
        try:
            query_job = client.query(query4, location="us")
            results = list(query_job.result())
            print(f"Success! Found {len(results)} results")
            if results:
                print("First result:", results[0])
        except Exception as e:
            print(f"Error: {e}")
        
        if datasets:
            dataset_id = datasets[0].dataset_id
            print(f"\nApproach 5: Dataset-specific INFORMATION_SCHEMA for {dataset_id}")
            query5 = f"SELECT * FROM `{project_id}.{dataset_id}`.INFORMATION_SCHEMA.TABLES LIMIT 5"
            print(f"Query: {query5}")
            try:
                query_job = client.query(query5)
                results = list(query_job.result())
                print(f"Success! Found {len(results)} results")
                if results:
                    print("First result:", results[0])
            except Exception as e:
                print(f"Error: {e}")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
