#!/bin/bash

CURRENT_NUMBER=$(aws emr list-clusters | jq -r ".Clusters[0].Name" | cut -d' ' -f2)
NEW_NUMBER=$((CURRENT_NUMBER + 1))

aws emr create-cluster \
  --name "Cluster $NEW_NUMBER" \
  --release-label "emr-7.3.0" \
  --applications Name=Hadoop Name=Spark \
  --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge InstanceGroupType=CORE,InstanceCount=3,InstanceType=m5.xlarge \
  --service-role EMR_DefaultRole \
  --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,KeyName=31-10-fgurioli \
  --region "us-east-1"

watch -n 20 "aws emr list-clusters | jq '.Clusters[0].Status.State'"
