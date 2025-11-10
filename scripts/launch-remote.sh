#!/bin/bash

CLUSTER_ID=$(aws emr list-clusters | jq -r ".Clusters[0].Id")

aws emr ssh \
  --cluster-id $CLUSTER_ID \
  --key-pair-file keys/31-10-fgurioli.pem \
  --command "spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --class project.Application \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=s3://unibo-31-10-fgurioli/history/ \
    --conf spark.history.fs.logDirectory=s3://unibo-31-10-fgurioli/history/ \
    --conf spark.executor.memory=6G \
    --conf spark.executor.memoryOverhead=2G \
    --conf spark.sql.shuffle.partitions=64 \
    --conf spark.default.parallelism=24 \
    s3://unibo-31-10-fgurioli/jars/nyc-taxi-tip-analysis_2.12-1.0.jar remote 1"
