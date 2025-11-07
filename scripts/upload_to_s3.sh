#!/bin/bash

echo "=== Uploading Data to AWS S3 ==="

BUCKET_NAME="unibo-31-10-fgurioli"

# Check if AWS CLI is configured
aws sts get-caller-identity >/dev/null 2>&1
if [ $? -ne 0 ]; then
  echo "Error: AWS CLI not configured. Run 'aws configure' first."
  exit 1
fi

# Create bucket (if it doesn't exist)
echo "Creating bucket s3://$BUCKET_NAME"
aws s3 mb s3://$BUCKET_NAME 2>/dev/null || echo "Bucket already exists or creation failed"

# Upload data
echo "Uploading trip data..."
for file in sample_data/*.parquet; do
  echo "Processing $file"
  aws s3 cp $file s3://$BUCKET_NAME/trips/
done
aws s3 cp sample_data/taxi_zone_lookup.csv s3://$BUCKET_NAME/zones/

echo ""
echo "=== Verification ==="
aws s3 ls s3://$BUCKET_NAME/trips/
aws s3 ls s3://$BUCKET_NAME/zones/

echo ""
echo "✓ Data uploaded successfully to s3://$BUCKET_NAME"
echo "Now run: ./run_aws.sh"
