# CloudFormation template to launch Amazon DynamoDB Replication Coordinator
Coffee script used to generate cloud formation template that launches the DynamoDB Cross Region Replication Coordinator. 
It launches ElasticBeanstalk worker environment that runs a docker container for the actual Replication coordinator and replication server.

## Structure
```
	dynamodb-replication-coordinator.coffee  # Source coffeeformation script to build CloudFormation template in JSON. 
	include/ # Utility coffeescripts used in to build CloudFormation template
	coordinatorContainer/ # ElasticBeanstalk worker tier definition
```

## To Build (TODO: Automate the following steps)
0. Install coffeeformation to build CloudFormation template from the source coffeeformation template
```
	# npm install -g csfn
```

1. Create ElasticBeanstalk application source bundle and upload to an S3 bucket 
```
	$ cd coordinator-container
	$ zip -r ../DynamoDBReplicationCoordinatorApplicationBundle.zip *
	$ aws s3 cp --acl public-read ../DynamoDBReplicationCoordinatorApplicationBundle.zip s3://<bucket name>/
```
(The bucket name and the object key name need to be given when launching CloudFormation stack)

2. Generate the cloud formation template by using the following command:
```
   csfn -I include dynamodb-replication-coordinator.coffee > dynamodb-replication-coordinator.template
```

3. Generate the cloud formation template to launch ECS clusters by using the following command:
```
   csfn -I include dynamodb-replication-ecs-cluster.coffee > dynamodb-replication-ecs-cluster.template
```
