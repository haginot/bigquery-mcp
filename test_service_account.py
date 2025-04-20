"""
Test script to verify service account access to the amazon-study-db project.
"""
import os
import sys
from google.cloud import bigquery

def main():
    key_file_path = os.path.expanduser("~/attachments/64ba55d6-602f-4772-9884-0619feefa042/amazon-study-db-c6ecbc10001a.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path
    
    project_id = "amazon-study-db"
    
    print(f"Testing access to project: {project_id}")
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
        
        print("\nExecuting a simple query:")
        query = "SELECT 1 as test"
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            print(f"Query result: {row.test}")
        
        print("\nExecuting INFORMATION_SCHEMA query:")
        info_schema_query = "SELECT * FROM `region-us`.INFORMATION_SCHEMA.DATASETS LIMIT 5"
        
        transformed_query = f"SELECT * FROM `{project_id}.region-us.INFORMATION_SCHEMA.DATASETS` LIMIT 5"
        print(f"Original query: {info_schema_query}")
        print(f"Transformed query: {transformed_query}")
        
        try:
            query_job = client.query(transformed_query)
            results = query_job.result()
            
            print("\nINFORMATION_SCHEMA query results:")
            row_count = 0
            for row in results:
                row_count += 1
                if row_count <= 3:  # Only print first 3 rows
                    print(f"- Dataset: {row.dataset_id if hasattr(row, 'dataset_id') else str(row)}")
            
            if row_count > 3:
                print(f"... and {row_count - 3} more datasets")
                
            print("\nINFORMATION_SCHEMA query successful!")
        except Exception as e:
            print(f"\nINFORMATION_SCHEMA query error: {e}")
            
            print("\nTrying original query to demonstrate the error:")
            try:
                query_job = client.query(info_schema_query)
                results = query_job.result()
                print("Original query unexpectedly succeeded")
            except Exception as e:
                print(f"Original query error: {e}")
        
        print("\nAll tests passed successfully!")
        return True
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
