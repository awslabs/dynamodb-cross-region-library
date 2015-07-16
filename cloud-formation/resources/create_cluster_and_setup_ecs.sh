#!/bin/bash

ECS_CLUSTER="DynamoDBCrossRegionReplication"

# Create ECS cluster

yum update -y 

aws --region @{ref("AWS::Region")} ecs create-cluster --cluster $ECS_CLUSTER

yum install docker -y

/sbin/service docker start

# Start ECS agent
# See: http://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-agent-install.html

docker run --name ecs-agent -d \
-v /var/run/docker.sock:/var/run/docker.sock \
-v /var/log/ecs/:/log -p 127.0.0.1:51678:51678 \
-v /var/lib/ecs/data:/data \
-e ECS_LOGFILE=/log/ecs-agent.log \
-e ECS_LOGLEVEL=info \
-e ECS_DATADIR=/data \
-e ECS_CLUSTER=$ECS_CLUSTER \
amazon/amazon-ecs-agent:latest

echo "*/5 * * * * root docker rm \$(docker ps -a -f status=exited -q)" >> /etc/crontab
