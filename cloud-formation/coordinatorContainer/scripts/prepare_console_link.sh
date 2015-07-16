#!/bin/bash

if ! [ -f '/etc/aws-sqsd.d/default.yaml' ]; then
    echo "sqsd config file is not found. Cannot proceed."
    exit 1
fi

# Determin queue URLs
SQS_WORKER_QUEUE_URL=`grep queue_url /etc/aws-sqsd.d/default.yaml | cut -f2 -d ' '`
EB_STACK_ID=`echo -n $SQS_WORKER_QUEUE_URL | cut -f5 -d'/' | cut -f3 -d'-'`
SQS_WORKER_DEAD_LETTER_QUEUE_URL=`aws --output text --region $CURRENT_REGION sqs list-queues | cut -f2 | grep $EB_STACK_ID | grep AWSEBWorkerDeadLetterQueue`
SQS_WORKER_QUEUE_NAME=`echo $SQS_WORKER_QUEUE_URL | cut -f5 -d\/`
SQS_WORKER_DEAD_LETTER_QUEUE_NAME=`echo $SQS_WORKER_DEAD_LETTER_QUEUE_URL | cut -f5 -d\/`
echo "SQS Worker Queue Name is $SQS_WORKER_QUEUE_NAME"
echo "SQS Worker Dead Letter Queue Name is $SQS_WORKER_DEAD_LETTER_QUEUE_NAME"

# Update stack parameters
RETRY_INTERVAL=30

while [ true ]
do 
        aws --region $CURRENT_REGION cloudformation\
                update-stack --stack-name $CLOUDFORMATION_STACK_NAME \
                --use-previous-template \
                --capabilities CAPABILITY_IAM \
                --parameters \
                        ParameterKey=CoordinatorInstanceType,UsePreviousValue=true \
                        ParameterKey=MetadataTableName,UsePreviousValue=true \
                        ParameterKey=MetadataTableRegion,UsePreviousValue=true \
                        ParameterKey=EcsInstanceType,UsePreviousValue=true \
                        ParameterKey=EcsClusterSize,UsePreviousValue=true \
                        ParameterKey=SQSUrl,ParameterValue=$SQS_WORKER_QUEUE_URL \
                        ParameterKey=SQSDeadLetterUrl,ParameterValue=$SQS_WORKER_DEAD_LETTER_QUEUE_URL \

        rc=$?
        if [ "$rc" != "0" ]; then 
                sleep $RETRY_INTERVAL
        else 
                break
        fi
done

