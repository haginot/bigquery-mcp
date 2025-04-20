set -e

echo "Building Docker image..."
docker build -t mcp-bigquery-server-test:latest .

echo "Running container to check environment variables (overriding entrypoint)..."
docker run -i --rm \
  --entrypoint /bin/bash \
  -v ~/attachments/64ba55d6-602f-4772-9884-0619feefa042/amazon-study-db-c6ecbc10001a.json:/app/key.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/key.json \
  -e PROJECT_ID=amazon-study-db \
  -e LOCATION=US \
  mcp-bigquery-server-test:latest \
  -c "echo 'Environment variables:' && \
  echo 'GOOGLE_APPLICATION_CREDENTIALS: '\$GOOGLE_APPLICATION_CREDENTIALS && \
  echo 'PROJECT_ID: '\$PROJECT_ID && \
  echo 'LOCATION: '\$LOCATION && \
  echo '' && \
  echo 'Testing BigQuery access:' && \
  python -c \"
import os
from google.cloud import bigquery
print('Credentials file exists:', os.path.exists(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '')))
print('Project ID:', os.environ.get('PROJECT_ID', 'Not set'))
print('Location:', os.environ.get('LOCATION', 'Not set'))
try:
    client = bigquery.Client(project=os.environ.get('PROJECT_ID'))
    print('BigQuery client created successfully')
    query = 'SELECT 1 as test'
    query_job = client.query(query)
    results = list(query_job.result())
    print('Query executed successfully, result:', results[0].test)
except Exception as e:
    print('Error:', str(e))
\""

echo "Test completed"
