## Overview

The Amazon DynamoDB Cross Region Replication (CRR) Library is a preview snapshot of a library that uses DynamoDB Streams to keep DynamoDB Tables in sync across multiple regions in near real time.  When you write to your DynamoDB Table in one region, those changes are automatically propagated by the Cross Region Replication Library to your tables in other regions. 

## Package Contents 

- [Demo](https://github.com/awslabs/dynamodb-cross-region-library/tree/master/demo)
    - Provides an example of Cross Region Replication with local DynamoDB instances
- [Core Replication Library](https://github.com/awslabs/dynamodb-cross-region-library/tree/master/library)
    - Maintains the replication group configuration
    - Defines interfaces for customizing replication behavior
    - Publishes metrics to Amazon CloudWatch for monitoring replication
- [Replication Manager](https://github.com/awslabs/dynamodb-cross-region-library/tree/master/replication-manager)
    - Provides HTTP hooks to:
        - Start and stop replication
        - Get the replication group status
        - Get a description of the tables in the replication group
    - Future features include hooks to add and remove tables from the replication group
    - Replication Console
        - Shows the status of a replication group
        - Displays replication metrics in graphs
        - Provides an interface to the API Server through a console.

## Minimum Requirements 
- Java 1.7+
- wget
- NodeJS
  - npm
  - coffee-script
  - bower
  - grunt
- Ruby
  - compass
- Maven

## Getting Started 

The demo directory provides an implementation of the CRR library, with a frontend interface, it should be run independently from the library and the replication manager. Please see the [README](https://github.com/awslabs/dynamodb-cross-region-library/blob/master/demo/README.md) in the demo directory for instructions on how to launch it. 

To build the library and the replication manager, use the following commands:

```
./install_preview_jars.sh
mvn install
```

This should generate a jar in the library directory containing the CRR library, which can be used in your own applications. A war file should be generated in the replication-manager directory which can be used to launch a HTTP server that can communicate with the library, as well as a frontend console to access the HTTP server. More detailed instructions can be found in their respective READMEs for the [library](https://github.com/awslabs/dynamodb-cross-region-library/blob/master/library/README.md) and [replication-manager](https://github.com/awslabs/dynamodb-cross-region-library/blob/master/replication-manager/README.md).

### How to Use the Library
A typical application follows the steps below to create a replication group:

1. Specify a replication policy (BasicTimestampReplicationPolicy)
2. Create all DynamoDB tables to be included in the replication group (using AWS SDK)
3. Define a replication configuration (ReplicationConfigurationImpl)
4. Create a replication coordinator (LocalRegionReplicationCoordinator)
5. Start replication (LocalRegionReplicationCoordinator.startReplicationWorkers())

Once you have the replication group setup you can perform DynamoDB table operations according to the data model rules (using AmazonDynamoDBTimestampReplicationClient). For a detailed example, refer to the [demo application](https://github.com/awslabs/dynamodb-cross-region-library/tree/master/demo)

## Release Notes
1. This is a preview version of the library, containing some experimental features:
    - Bootstrapping tables and adding them to an active replication group
    - Rolling back invalid user writes (ReplicationPolicy.ROLLBACK)
2. The CRR library uses the new expressions API for updates and conditional writes. Using the legacy expected API will result in a ValidationException
