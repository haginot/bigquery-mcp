"""
Test script to try different formats for INFORMATION_SCHEMA queries in BigQuery.
"""
import os
import sys
from google.cloud import bigquery

def main():
    key_file_path = os.path.expanduser("~/attachments/64ba55d6-602f-4772-9884-0619feefa042/amazon-study-db-c6ecbc10001a.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path
    
    project_id = "amazon-study-db"
    
    print(f"Testing INFORMATION_SCHEMA query formats for project: {project_id}")
    print(f"Using service account key: {key_file_path}")
    
    try:
        client = bigquery.Client(project=project_id)
        
        query1 = f"SELECT * FROM `{project_id}`.INFORMATION_SCHEMA.DATASETS LIMIT 5"
        print(f"\nFormat 1: {query1}")
        try:
            query_job = client.query(query1)
            results = query_job.result()
            print("Format 1 succeeded!")
            row_count = 0
            for row in results:
                row_count += 1
                if row_count <= 3:
                    print(f"- Dataset: {row.dataset_id if hasattr(row, 'dataset_id') else str(row)}")
        except Exception as e:
            print(f"Format 1 error: {e}")
            
        query2 = f"SELECT * FROM `{project_id}.region-us`.INFORMATION_SCHEMA.DATASETS LIMIT 5"
        print(f"\nFormat 2: {query2}")
        try:
            query_job = client.query(query2)
            results = query_job.result()
            print("Format 2 succeeded!")
            row_count = 0
            for row in results:
                row_count += 1
                if row_count <= 3:
                    print(f"- Dataset: {row.dataset_id if hasattr(row, 'dataset_id') else str(row)}")
        except Exception as e:
            print(f"Format 2 error: {e}")
            
        print(f"\nFormat 3: Using location parameter")
        try:
            query_job = client.query(
                "SELECT * FROM INFORMATION_SCHEMA.DATASETS LIMIT 5",
                location="us"  # Explicitly set location
            )
            results = query_job.result()
            print("Format 3 succeeded!")
            row_count = 0
            for row in results:
                row_count += 1
                if row_count <= 3:
                    print(f"- Dataset: {row.dataset_id if hasattr(row, 'dataset_id') else str(row)}")
        except Exception as e:
            print(f"Format 3 error: {e}")
            
        query4 = f"SELECT * FROM `{project_id}`.`region-us.INFORMATION_SCHEMA.DATASETS` LIMIT 5"
        print(f"\nFormat 4: {query4}")
        try:
            query_job = client.query(query4)
            results = query_job.result()
            print("Format 4 succeeded!")
            row_count = 0
            for row in results:
                row_count += 1
                if row_count <= 3:
                    print(f"- Dataset: {row.dataset_id if hasattr(row, 'dataset_id') else str(row)}")
        except Exception as e:
            print(f"Format 4 error: {e}")
            
        query5 = f"SELECT * FROM `{project_id}.region-us.INFORMATION_SCHEMA.DATASETS` LIMIT 5"
        print(f"\nFormat 5: {query5}")
        try:
            query_job = client.query(query5)
            results = query_job.result()
            print("Format 5 succeeded!")
            row_count = 0
            for row in results:
                row_count += 1
                if row_count <= 3:
                    print(f"- Dataset: {row.dataset_id if hasattr(row, 'dataset_id') else str(row)}")
        except Exception as e:
            print(f"Format 5 error: {e}")
            
        return True
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
