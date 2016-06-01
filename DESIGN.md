# DynamoDB Cross-region Replication 

This library is designed to perform real-time replication between DynamoDB tables. It consumes DynamoDB Streams using the following Amazon components:

1. DynamoDB Streams Service
2. DynamoDB Streams Kinesis Adapter
3. Kinesis Client Library (KCL)
4. Kinesis Connectors Library

Important classes:

* CommandLineInterface - Main entry point for the cross-region replication tool, this class accepts command line arguments and kicks off the appropriate processes to start real-time replication:
  * Sets up connection to DynamoDB stream of the source table, performs sanity check to ensure stream records are ready to be consumed.
  * Initializes KCL configurations with default values and generated `taskName` which determines the DynamoDB checkpoint table name.
  * Kicks off the KCL worker process which immediately starts to consume from DynamoDB Streams and replicate writes to the destination table.

* DynamoDBStreamsRecordTransformer - Converts a Kinesis Record wrapper into a DynamoDB Streams Record. Currently, this is done through subclassing the Kinesis Record class and casting down to the DynamoDB Streams adapter object.

* DynamoDBReplicationBuffer - Buffers DynamoDB Streams records like a write cache, where only the latest change to each primary key is stored and the rest are squashed, so there is at most one change per item in the buffer at any time. The buffer is configured to flush as long as there is at least 1 record in it.

* DynamoDBReplicationEmitter - Writes the new image from the DynamoDB Streams record to the destination DynamoDB table using PutItem (replaces existing item). All records in the buffer are flushed to the destination DynamoDB table in this manner, using asynchronous, parallel writes.

* DynamoDBConnectorUtilities - Utility class with various methods that convert from region name to endpoints and vice versa. Note the `getTaskName()` method is used to generate a default taskName when the user does not provide one.
  * Default `taskName` = MD5 hash of (sourceTableRegion + sourceTableName + destinationTableRegion + destinationTableName)

    > **NOTE**: Each replication process requires a different `taskName`. Overlapping names will result in strange, unpredictable behavior. Please also delete this DynamoDB checkpoint table if you wish to completely restart replication. When running the same replication over multiple machines (same source & destination tables), then you must use the same `taskName` for each process becaues they use the DynamoDB checkpoint to coordinate and distribute work among them.

The cross-region replication library relies heavily on the Kinesis Client Library (KCL) for stream consumption including shard leasing, work distribution and checkpointing logic. Please use the [official KCL documentation](http://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html) for more information on troubleshooting and monitoring. KCL is open-sourced on [Github](https://github.com/awslabs/amazon-kinesis-client) as well.
