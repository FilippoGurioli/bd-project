#!/bin/bash

if [ $# -eq 0 ]; then
  echo "Pass cluster id as first parameter"
  exit
fi

aws emr ssh \
  --cluster-id $1 \
  --key-pair-file keys/31-10-fgurioli.pem \
  --command "spark-submit --master yarn --deploy-mode cluster --class project.Application s3://unibo-31-10-fgurioli/jars/nyc-taxi-tip-analysis_2.12-1.0.jar remote 3"
