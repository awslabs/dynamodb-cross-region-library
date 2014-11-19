#!/bin/sh

DYNAMODB_LOCAL_DIR=DynamoDBLocal
DASHBOARD_DIR=dashboard

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

