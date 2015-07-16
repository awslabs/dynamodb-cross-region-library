# DynamoDB Cross-region Replication Library

Amazon DynamoDB Cross-region Replication Library is composed of different components that together allows automatic replication of data across DynamoDB tables of different AWS regions. 

In the root directory, build all the modules:

```
    mvn install
```

IMPORTANT: all target jar files must be executed on an EC2 instance with either environment credential variables or instance profile enabled. This is due to the presence of CloudWatch logs publishing to AWS. If you wish to execute this locally, please remove the src/main/resources/log4j.properties and use your own custom log4j.properties.

This will create a target directory with a sub-directory dependencies containing all the executable jars of the Cross-region Library. Instructions for executing these can be found in the README.md of each module's directory. Here is an aggregate version:
```
    java -jar dynamodb-replication-server-1.0.0.jar --accountId <AWS_ACCOUNT_ID> --metadataTableEndpoint <METADATA_TABLE_ENDPOINT> --metadataTableName <METADATA_TABLE_NAME> 
    
    java -jar dynamodb-replication-coordinator-1.0.0.jar --accountId <AWS_ACCOUNT_ID> --metadataTableEndpoint <METADATA_TABLE_ENDPOINT> --metadataTableName <METADATA_TABLE_NAME> 
    
    java -jar dynamodb-connectors-1.0.0.jar --pipeline SINGLE_MASTER_TO_READ_REPLICA --sourceEndpoint <source_endpoint> --sourceTable <source_table_name> --destinationEndpoint <destination_endpoint> --destinationTable <destination_table_name>
```
