#!/bin/bash

if [[ -n "${JAVA_HOME:-}" ]]; then
  echo "Setting java-home to openjdk-17"
  export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
  export PATH=$JAVA_HOME/bin:$PATH
fi

sbt clean compile
sbt package
aws s3 cp target/scala-2.12/nyc-taxi-tip-analysis_2.12-1.0.jar s3://unibo-31-10-fgurioli/jars/
