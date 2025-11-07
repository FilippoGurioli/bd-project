#!/bin/bash

if [ $# -eq 0 ]; then
  echo "Pass cluster name as first parameter"
  return 0
fi

aws emr create-cluster \
  --name "Cluster $1" \
  --release-label "emr-7.3.0" \
  --applications Name=Hadoop Name=Spark \
  --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge InstanceGroupType=CORE,InstanceCount=2,InstanceType=m5.xlarge \
  --service-role EMR_DefaultRole \
  --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,KeyName=31-10-fgurioli \
  --region "us-east-1" \
  --log-uri s3://unibo-31-10-fgurioli/logs/

watch -n 20 "aws emr list-clusters | jq'.Clusters[0].Status.State'"
