#!/bin/bash

BUCKET_NAME=taxi-data

echo "=== Setting up LocalStack S3 Test ==="

# 1. Set environment variables
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_DEFAULT_REGION="us-east-1"
export FS_S3A_ENDPOINT="http://localhost:4566"

# 2. Check LocalStack is running
echo "Checking LocalStack..."
if ! curl -s http://localhost:4566/_localstack/health >/dev/null; then
  echo "ERROR: LocalStack not running!"
  echo "Start it with: docker run --rm -it -p 4566:4566 localstack/localstack"
  exit 1
fi
echo "✓ LocalStack is running"

# 3. Create bucket and upload data
echo "Creating bucket and uploading data..."
awslocal s4 mb s3://$BUCKET_NAME 2>/dev/null || echo "Bucket already exists"

if [ "$(awslocal s3 ls s3://$BUCKET_NAME/trips/ | wc -l)" -gt 0 ]; then
  echo "✓ Data uploaded"
else
  for file in sample_data/*.parquet; do
    echo "Processing $file"
    awslocal s3 cp $file s3://$BUCKET_NAME/trips/
  done
  awslocal s3 cp sample_data/taxi_zone_lookup.csv s3://$BUCKET_NAME/zones/
fi

awslocal s3 ls s3://$BUCKET_NAME/trips/
awslocal s3 ls s3://$BUCKET_NAME/zones/
echo "✓ Data uploaded"

# 4. Run the application
echo ""
echo "=== Running Application with S3 ==="
spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=http://localhost:4566 \
  --conf spark.hadoop.fs.s3a.access.key=test \
  --conf spark.hadoop.fs.s3a.secret.key=test \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  src/application.py \
  --trips "s3a://$BUCKET_NAME/trips/*.parquet" \
  --zones "s3a://$BUCKET_NAME/zones/taxi_zone_lookup.csv" \
  --output "s3a://$BUCKET_NAME/output/results" \
  --job both

# 5. Check results
echo ""
echo "=== Results ==="
awslocal s3 ls s3://$BUCKET_NAME/output/

# 6. Download results locally
echo ""
echo "Downloading results..."
awslocal s3 cp s3://$BUCKET_NAME/output/results_optimized.json ./output_s3_optimized.json
awslocal s3 cp s3://$BUCKET_NAME/output/results_non_optimized.json ./output_s3_non_optimized.json

echo ""
echo "✓ Done!"

