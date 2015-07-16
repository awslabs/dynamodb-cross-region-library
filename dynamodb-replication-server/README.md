# DynamoDB Cross-region Replication library server 

This is a library that communicates with the cross-region replication coordinator by receive HTTP requests at default port 7000. 

The main class launches a Jetty server that listens on port 7000 (by default) for replication group requests, and modifies the given metadata table according to the request. The replication coordinator is expected to be active and monitoring the DynamoDB Stream of the metadata table to process requests that this server inputs to the table.

Entry point for the class:
    
```
    DynamoDBReplicationServer
```

To build the target jar file:
```
    mvn install
```

To launch the server after mvn install:
```
    cd target
    java -jar dynamodb-replication-server-1.0.0.jar --accountId <AWS_ACCOUNT_ID> --metadataTableEndpoint <METADATA_TABLE_ENDPOINT> --metadataTableName <METADATA_TABLE_NAME> 
```
