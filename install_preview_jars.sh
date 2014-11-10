#!/bin/sh

mkdir -p .tmp
cd .tmp

echo "Downloading DynamoDB Kinesis Adapter preview version\n"
wget -O dynamodb-streams-kinesis-adapter-latest-preview.jar -N http://dynamodb-preview.s3-website-us-west-2.amazonaws.com/dynamodb-streams-kinesis-adapter-latest-preview.jar

echo "Done."

echo "Installing DynamoDB Kinesis Adapter preview version\n"
mvn install:install-file \
 -Dfile=dynamodb-streams-kinesis-adapter-latest-preview.jar \
 -DgroupId=com.amazonaws.services.dynamodbv2 \
 -DartifactId=dynamodb-streams-kinesis-adapter -Dversion=0.1.0-preview -Dpackaging=jar

echo "Done."

echo "Downloading AWS SDK 1.9.4a preview version\n"
wget -O aws-java-sdk-latest-preview.zip -N http://dynamodb-preview.s3-website-us-west-2.amazonaws.com/aws-java-sdk-latest-preview.zip
unzip -o aws-java-sdk-latest-preview.zip

echo "Done."

echo "Installing AWS SDK 1.9.4a preview version\n"
mvn install:install-file \
 -Dfile=aws-java-sdk-1.9.4a-preview/lib/aws-java-sdk-1.9.4a-preview.jar \
 -DgroupId=com.amazonaws \
 -DartifactId=aws-java-sdk -Dversion=1.9.4a-preview -Dpackaging=jar
