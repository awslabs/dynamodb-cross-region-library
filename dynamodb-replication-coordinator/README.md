# DynamoDB Cross-region Replication library coordinator

This is a library that creates and uses a DynamoDB table to keep track of metadata for replication groups that are executing cross-region replication.

The main class creates metadata table needed if it does not already exist, and launches a Kinesis Client Library worker that processes DynamoDB 
Streams records from the stream of the metadata table. That is, the coordinator is event-driven by changes made to the metadata table.

Entry point for the class:
    
```
    DynamoDBReplicationCoordinator
```

To build the target jar file:
```
    mvn install
```

To launch the coordinator after mvn install, note the coordinator uses AWS [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) to pick the AWS credential to use:

```
    cd target
    java -jar dynamodb-replication-coordinator-1.0.0.jar --accountId <AWS_ACCOUNT_ID> --metadataTableEndpoint <METADATA_TABLE_ENDPOINT> --metadataTableName <METADATA_TABLE_NAME> 
```
