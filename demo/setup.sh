#!/bin/sh

DYNAMODB_LOCAL_DIR=DynamoDBLocal
DASHBOARD_DIR=dashboard
CRR_MANAGER_WAR=dynamodb-cross-region-replication-manager.war
mkdir -p lib

## Copy DynamoDB Cross Region Replication Manager
echo "## Setting up DynamoDB Cross Region Replication Manager\n"
if [ ! -f lib/$CRR_MANAGER_WAR ]; then
  cp -v ../replication-manager/target/$CRR_MANAGER_WAR ./lib/
  if [ "$?" != "0" ]; then
     echo "ERROR: Failed to copy the Cross Region Replication Manager war file. Please make sure if it was built correctly beforehand\n"
     exit 1
  fi
fi

## Download dependencies
pushd ./lib

echo "## Download DynamoDB Local\n"
wget -N http://dynamodb-preview.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest_preview.tar.gz

echo "## Download jetty runner\n"
wget -N http://repo2.maven.org/maven2/org/mortbay/jetty/jetty-runner/8.1.2.v20120308/jetty-runner-8.1.2.v20120308.jar

popd

## Setup DynamoDB Local
echo "### Unpacking DynamoDB Local\n"
mkdir -p $DYNAMODB_LOCAL_DIR
pushd $DYNAMODB_LOCAL_DIR

tar xvzf ../lib/dynamodb_local_latest_preview.tar.gz

popd

## Setup Dashboard App
cd $DASHBOARD_DIR

echo "### Setting up tools required to run dashboard app\n"
npm install

