# Useful commands

## Inside the cluster (with ssh)

```bash
yarn logs -applicationId application_1762199471914_0001
yarn application -status application_1762199471914_0001 | grep -i "State :"
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class project.Application \
  s3://unibo-31-10-fgurioli/jars/nyc-taxi-tip-analysis_2.12-1.0.jar \
  remote 3
```

## Outside the cluster

```bash
aws emr create-cluster \
    --name "Cluster X" \
    --release-label "emr-7.3.0" \
    --applications Name=Hadoop Name=Spark \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge InstanceGroupType=CORE,InstanceCount=2,InstanceType=m5.xlarge \
    --service-role EMR_DefaultRole \
    --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,KeyName=31-10-fgurioli \
    --region "us-east-1" \
    --log-uri s3://unibo-31-10-fgurioli/logs/
```
