#!/bin/bash
cd /opt

LOG_FILE=./log/coordinator-container-stdout-stderr.log

while ! [ -f /etc/aws-sqsd.d/default.yaml ]
do
	echo "Waiting for sqsd configuration directory is mounted..." >> $LOG_FILE
	sleep 5
done

# Download DynamoDB Replication Coordinator API server
aws s3 cp s3://$SOURCE_BUCKET/$COORDINATOR_SERVER_JAR DynamoDBReplicationServer.jar

# Download DynamoDB Replication Coordinator worker
aws s3 cp s3://$SOURCE_BUCKET/$COORDINATOR_JAR DynamoDBReplicationCoordinator.jar

# Create supervisord.conf
sed \
	-e "s/\${AWS_ACCOUNT_ID}/$AWS_ACCOUNT_ID/" \
	-e "s/\${METADATA_TABLE_ENDPOINT}/$METADATA_TABLE_ENDPOINT/" \
	-e "s/\${METADATA_TABLE_NAME}/$METADATA_TABLE_NAME/" \
	./scripts/supervisord.conf.template > ./supervisord.conf

# Launch DynamoDB Replication Coordinator daemon via supervisord
supervisord -c ./supervisord.conf 

# Launch DynamoDB Replication Coordinator API server
java -jar DynamoDBReplicationServer.jar --accountId $AWS_ACCOUNT_ID --metadataTableEndpoint $METADATA_TABLE_ENDPOINT --metadataTableName $METADATA_TABLE_NAME
