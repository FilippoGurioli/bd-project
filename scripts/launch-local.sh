#!/bin/bash

export SPARK_HOME="/home/filippo/spark"
export PATH="$SPARK_HOME/bin:$PATH"

echo "BUILDING AND PUSHING TO AWS S3"
source ./build-and-push-aws.sh

if ! curl -s --head http://localhost:18080 | grep -q "200 OK"; then
  echo "History server not running. Starting..."
  "$SPARK_HOME"/sbin/start-history-server.sh
else
  echo "History Server already running"
fi

echo "SUBMITTING TO SPARK LOCALLY"
spark-submit \
  --master "local[8]" \
  --class project.Application \
  target/scala-2.12/nyc-taxi-tip-analysis_2.12-1.0.jar \
  local 3
