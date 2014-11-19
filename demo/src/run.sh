#!/bin/sh

DASHBOARD_DIR=dashboard
DYNAMODB_LOCAL_DIR=DynamoDBLocal
PORT1=8000
PORT2=8001

## Start DynamoDB Local
pushd $DYNAMODB_LOCAL_DIR

echo "### Starting DynamoDB Local on port $PORT1\n"
mkdir -p port_$PORT1
java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -port $PORT1 -sharedDb -dbPath port_$PORT1 >& dynamodb_$PORT1.log &
echo $! > dynamodb_local_$PORT1.pid

echo "### Starting DynamoDB Local on port $PORT2\n"
mkdir -p port_$PORT2
java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -port $PORT2 -sharedDb -dbPath port_$PORT2 >& dynamodb_$PORT2.log &
echo $! > dynamodb_local_$PORT2.pid

sleep 3

popd

## Start Dashboard app
pushd $DASHBOARD_DIR

echo "### Starting Dashboard app for us-east-1 on port 10000\n"
PORT_OFFSET=0 REGION=us-east-1 node server.js &
echo $! > dashboard_port_offset0.pid

echo "### Starting Dashboard app for ap-northeast-1 on port 10001\n"
PORT_OFFSET=1 REGION=ap-northeast-1 node server.js &
echo $! > dashboard_port_offset1.pid

## Start Dummy Data Generator
echo "Starting sensor data generator\n"
AWS_ACCESS_KEY=DummyAccessKey AWS_SECRET_ACCESS_KEY=DummySecretKey DYNAMODB_ENDPOINT=http://localhost:8000 DYNAMODB_REGION=us-east-1 TABLE_COMMON=master node lib/generate_dummy_data.js >& /dev/null &
echo $! > data_generator.pid

popd

## Start Cross Region Replication Manager
echo "Starting DynamoDB Cross Region Replication Manager on port 8080\n"
AWS_ACCESS_KEY=DummyAccessKey AWS_SECRET_ACCESS_KEY=DummySecretKey java -jar lib/jetty-runner-8.1.2.v20120308.jar lib/dynamodb-cross-region-replication-manager.war 

