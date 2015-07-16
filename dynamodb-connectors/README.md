# DynamoDB Connectors

This is an implementation of the Amazon Kinesis Connectors specifically for
replicating DynamoDB changes between DynamoDB tables. It consumes DynamoDB
Streams using the following application stack:
1. DynamoDB Streams
2. DynamoDB Streams Kinesis Adapter
3. Amazon Kinesis Client Library 1.3
4. Amazon Kinesis Connectors

The components function as follows:

* DynamoDBStreamsRecordTransformer - Converts a DynamoDB Streams Record wrapped as
a Kinesis Record into a DynamoDB Streams Record. For Amazon Kinesis Client
Library 1.3 and before this is done through subclassing the Kinesis Record
class and casting down to the adapter object. For Amazon Kinesis Client Library
1.4 and after, the DynamoDB Record is serialized using Jackson ObjectMapper to
bytes.

* DynamoDBReplicationBuffer - Buffers records, but squashes changes to the same
primary key so there is at most one change per item in the buffer at any time.
The buffer is configured to flush anytime there is at least 1 record in it.

* DynamoDBReplicationEmitter - Writes the new image from the DynamoDB Streams
record to the destination DynamoDB table using PutItem (replaces existing item)

To build the library:

```
    mvn install
```

The target directory contains the target jar, to run:

```
    java -jar dynamodb-connectors-1.0.0.jar --pipeline SINGLE_MASTER_TO_READ_REPLICA --sourceEndpoint <source_endpoint> --sourceTable <source_table_name> --destinationEndpoint <destination_endpoint> --destinationTable <destination_table_name>
```
