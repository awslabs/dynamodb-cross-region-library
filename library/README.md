## Overview

The Amazon DynamoDB Cross Region Replication Library (CRR Library) is a preview version of a client-side tool to propagate writes among a replication group of DynamoDB tables with the same schema. [DynamoDB Streams](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html) provide a key-level ordered log of writes to a DynamoDB table. By using the [Amazon DynamoDB Streams Kinesis Adapter](https://github.com/awslabs/dynamodb-streams-kinesis-adapter), the library leverages the power of the [Amazon Kinesis Client Library (KCL)](http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-record-processor-app.html) to process records from DynamoDB Streams and replicates the writes to other tables in its replication group.

Each table in a replication group is classified as a master or read-replica table. The distinction is that writes are allowed only to master tables. This property cannot be enforced by the CRR Library, so it is important to classify any table that may receive writes as a master table. Both master and read-replica tables will maintain an eventually consistent view of the replication group. The latest write for a key on any master table will be reflected in all tables within the replication group. 

The CRR Library provides a way to maintain eventually consistent views of the same data in multiple regions. This is useful for regionalization of data. Additionally, the CRR Library provides a way to maintain a backup copy of data in the case a region is unavailable. More detailed information on the CRR library can be found in the [Design Document](https://github.com/awslabs/dynamodb-cross-region-library/blob/master/library/DESIGN.md).

## Features

- Core Replication Library
    - Maintains the replication group configuration
    - Provides a framework to manage instances of the KCL for each master table
    - Implements the KCL to propagate writes to other tables in the replication group
    - Defines interfaces for customizing replication behavior
    - Publishes metrics to Amazon CloudWatch for monitoring replication

## Getting Started 
  
To build the CRR library:
  
```
mvn install
```

This should create a jar file containing the Cross Region Replication library, which can be used in your own applications. A typical application follows the steps below to create a replication group:

1. Specify a replication policy (BasicTimestampReplicationPolicy)
2. Create all DynamoDB tables to be included in the replication group (using AWS SDK)
3. Define a replication configuration (ReplicationConfigurationImpl)
4. Create a replication coordinator (LocalRegionReplicationCoordinator)
5. Start replication (LocalRegionReplicationCoordinator.startReplicationWorkers())

### Replication Configuration
The ReplicationConfiguration contains information about tables in the replication group including region, table name, CloudWatch credentials, DynamoDB credentials, and DynamoDB Streams credentials. When setting up a replication group, first create an empty ReplicationConfiguration, and then add all the tables in the replication group using the AddTable or AddRegionConfiguration actions. AddRegionConfiguration allows regions to be defined in more granular containers called RegionConfiguration (RegionConfigurationImpl).

### CRR Java SDK Client
Once you have the replication group setup you can perform DynamoDB table operations according to the data model rules mentioned below (using AmazonDynamoDBTimestampReplicationClient). We highly recommend using this Java SDK wrapper to perform put/update/delete operations to the          replication tables. If your application does not use the provided Java SDK, make sure you adhere to the CRR data model when performing DynamoDB table operations. Primarily the following rules apply:
  - All PutItem and UpdateItem requests must contain a monotonically increasing parameter in order to achieve eventual consistency. The current                implementation uses an ISO-8601 UTC timestamp as the monotonically increasing value. This value is stored under key `TIMESTAMP___________KEY`.
  - All user writes (PutItem and UpdateItem) must contain a flag to indicate it is a user write, not a replication library write. The current flag has key     `USER___________KEY`, the value can be anything.
  - DeleteItem and BatchWriteItem are disallowed.
  
For more information on details of the CRR data model, see the DESIGN.md document.

## CloudWatch Metrics
The CRR library emits the following Amazon CloudWatch metrics for each table in a replication group:
- NumberUserWrites: number of user write requests sent to a table and then appeared in the DynamoDB Streams. 
- NumberReplicationWrites: number of write requests a table gets from other replicas (not from users). 
- NumberCheckpointedRecords: the shard subscriber propagates each user write to other tables for replicating and then receives acks from other tables after the Stream record is successfully applied to those tables. After obtaining acks from all other tables (meaning the update is successfully replicated in the replication group), the Stream record is ready to checkpoint to the DynamoDB Streams. This metric counts the number of checkpointed Stream records. 
- AccumulatedRecordLatency: the accumulated end-to-end latency (the duration from the time a user makes a write request to the time the write is successfully replicated in all other tables) of user write requests. Dividing this metric by NumberCheckpointedRecords gives the average end-to-end latency of each request. 


## Release Notes
1.  This is a preview version of the library, containing some experimental features:
    - Bootstrapping tables and adding them to an active replication group
    - Rolling back invalid user writes (ReplicationPolicy.ROLLBACK)
2. The CRR library uses the new expressions API for updates and conditional writes. Using the legacy expected API will result in a ValidationException.
