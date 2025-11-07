#!/bin/bash

CLUSTER_ID=$(aws emr list-clusters | jq ".Clusters[0].Id")

aws emr ssh \
  --cluster-id $CLUSTER_ID \
  --key-pair-file keys/31-10-fgurioli.pem \
  --command "spark-submit --master yarn --deploy-mode cluster --class project.Application s3://unibo-31-10-fgurioli/jars/nyc-taxi-tip-analysis_2.12-1.0.jar remote 3"
