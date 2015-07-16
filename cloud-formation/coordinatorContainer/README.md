# Amazon DynamoDB Replication Coordinator container
An ElasticBeanstalk application that launches DynamoDB Replication Coordinator and its API server. It is for worker tier environment. The API server receives commands via SQS queue for the environment.

The application is also responsible for:
- generating output to the cross region replication console UI hosted on the DynamoDB Ecosystem S3 bucket

## Structure
```
	Dockerfile  # Docker container definition to build container image
	Dockerrun.aws.json	# Condfiguration for running Docker container
	scripts/	# Scripts and templates for configuration files
```

## To build
Simply create a zip archieve and deploy to ElasticBeanstalk worker tier environment.
```
	$ zip -r ../DynamoDBReplicationCoordinatorApplicationBundle.zip .
	$ aws s3 cp --acl public-read ../DynamoDBReplicationCoordinatorApplicationBundle.zip s3://<bucket_name>/
```
