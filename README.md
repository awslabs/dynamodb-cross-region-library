**_[IMPORTANT]_ Dynamodb now provides server-side support for cross-region replication using Global Tables. Please use that instead of this client-side library. For more details about Global Tables, please see https://aws.amazon.com/dynamodb/global-tables/**

# DynamoDB Cross-Region Replication

The DynamoDB cross-region replication process consists of 2 distinct steps:

* Step 1: Table copy (bootstrap) - copying existing data from source table to destination table
* Step 2: Real-time updates (this component) - applying live DynamoDB stream records from the source table to the destination table

## Requirements ##
* Maven
* JRE 1.7+
* Pre-existing source and destination DynamoDB tables

## Step 1 (Optional): Table copy (bootstrapping existing data)

This step is necessary if your source table contains existing data, and you would like to sync the data first. Please use the following steps to complete the table copy:

1. (Optional) If your source table is not receiving live traffic, you may skip this step. Otherwise, if your source table is being continuously updated, you must enable DynamoDB Streams to record these live writes while table copy is ongoing. Enable DynamoDB Streams on your source table with StreamViewType set to "New and old images". For more information on how to do this, please refer to our [offical DynamoDB Streams documentation.](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html)
2. Check the read provisioned throughput (RCU) on your source table, and the write provisioned throughput (WCU) on your destination table. Ensure they are set high enough to allow table copy to complete well within 24 hours.
   * Rough calculation: table copy completion time ~= # of items in source table * ceiling(average item size / 1KB) / WCU of destination table.
3. Start the table copy process, there are a few options:
   * Use the Import/Export option available via the official AWS DynamoDB Console, which exports data to S3 then imports it back to a different DynamoDB table. For more information, please refer to our [official Import/Export documentation](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBPipeline.html)
   * Use a custom Java tool on awslabs that performs a parallel table scan then writes scanned items to the destination table, also [available on Github.](https://github.com/awslabs/dynamodb-import-export-tool)
   * Write your own tool to perform the table copy, essentially scanning items in the source table and using parallel PutItem calls to write items into the destination table.

> **WARNING**: If your source table has live writes, make sure the table copy process completes well within 24 hours, because DynamoDB Streams records are only available for 24 hours. If your table copy process takes more than 24 hours, you can potentially end up with inconsistent data across your tables!

## Step 2: Real-time updates (applying live stream records)

This step sets up a replication process that continuously consumes DynamoDB stream records from the source table and applies them to the destination table in real-time.

0. Enable DynamoDB Streams on your source table with StreamViewType set to "New and old images". For more information on how to do this, please refer to our [offical DynamoDB Streams documentation.](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html)

1. Build the library:

```
    mvn install
```

2. This produces the target jar in the target/ directory, to start the replication process:

```
    java -jar target/dynamodb-cross-region-replication-1.2.1.jar --sourceRegion <source_region> --sourceTable <source_table_name> --destinationRegion <destination_region> --destinationTable <destination_table_name>
```

Use the `--help` option to view all available arguments to the connector executable jar. The connector process accomplishes a few things:
* Sets up a [Kinesis Client Library (KCL)](https://github.com/awslabs/amazon-kinesis-client) worker to consume the DynamoDB Stream of the source table
* Uses a custom implementation of the [Kinesis Connector Library](https://github.com/awslabs/amazon-kinesis-connectors) to apply incoming stream records to the destination table in real-time
* Creates a DynamoDB checkpoint table using the given or default `taskName`, used when restoring from crashes.
  * **WARNING**: Each replication process requires a different `taskName`. Overlapping names will result in strange, unpredictable behavior. Please also delete this DynamoDB checkpoint table if you wish to completely restart replication. See how a default `taskName` is calculated below in section "Advanced: running replication process across multiple machines".
* Publishes default KCL CloudWatch metrics to report number of records and bytes processed. For more information please refer to the [official KCL documentation.](http://docs.aws.amazon.com/streams/latest/dev/monitoring-with-kcl.html). CloudWatch metric publishing can be disabled with the `--dontPublishCloudwatch` flag.
* Produces logs locally according to the default log4j configuration file, which produces 2 separate log files: one for the KCL process and one for the rest of the connector application. You may use your own log4j.properties file to override these defaults. In addition, AWS CloudWatch offers a [monitoring agent](http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/WhatIsCloudWatchLogs.html) to automatically push local logs to your AWS CloudWatch account, if needed.
* You can override the source, KCL and destination DynamoDB endpoints with `--sourceEndpoint`, and `--destinationEndpoint` command line arguments. You can override the DynamoDB Streams source endpoint with the `--sourceStreamsEndpoint` command line argument. The main use case for overriding any endpoint is to use DynamoDB Local on one end or both ends of the replication pipeline, or for KCL leases and checkpoints.

> **NOTE**: More information on the design and internal structure of the connector library can be found in the [design doc.](./DESIGN.md) Please note it is your responsibility to ensure the connector process is up and running at all times - replication stops as soon as the process is killed, though upon resuming the process automatically uses the checkpoint table in DynamoDB to restore progress.

## Advanced: running replication process across multiple machines

With extremely large tables or tables with high throughput, it might be necessary to split the replication process across multiple machines. In this case, simply kick off the target executable jar with the same command on each machine (i.e. one KCL worker per machine). The processes use the DynamoDB checkpoint table to coordinate and distribute work among them, as a result, it is *essential* that you use the same `taskName` for each process, or if you did not specify a `taskName`, a default one is computed.
* Default `taskName` = MD5 hash of (sourceTableRegion + sourceTableName + destinationTableRegion + destinationTableName) 

## Advanced: replicating multiple tables

Each instantiation of the jar executable is for a single replication path only (i.e. one source DynamoDB table to one destination DynamoDB table). To enable replication for multiple tables or create multiple replicas of the same table, a separate instantiation of the cross-region replication library is required. Some examples of replication setup:

**Replication Scenario 1**: One source table in us-east-1, one replica in each of us-west-2, us-west-1, and eu-west-1 
* Number of Processes Required: 3 cross-region replication processes required: one from us-east-1 to us-west-2, one from us-east-1 to us-west-1, and one from us-east-1 to eu-west-1

**Replication Scenario 2**: Two source tables (table1 & table2) in us-east-1, both replicated separately to us-west-2 
* Number of Processes Required: 2 cross-region replication processes required: one for table1 from us-east-1 to us-west-2, and one for table2 from us-east-1 to us-west-2 

**Can multiple cross-region replication processes run on the same machine?**
* Yes, feel free to launch multiple processes on the same machine to optimize resource usage. However, it is highly recommended that you monitor one process first to understand its CPU, memory, network and other resource footprint. In general, bigger tables require more resources and high-throughput tables require more resources.

**How can I ensure the process is always up and running?**
* Use your own software tools to keep the process long-running. For instance, many people use [supervisord]( http://supervisord.org/). Others make use of other AWS services such as [EC2 Autoscaling](https://aws.amazon.com/autoscaling/) and [EC2 Container Service](https://aws.amazon.com/ecs/) to achieve the same purpose.

**How can I build the library and run tests?**
Execute `mvn clean verify -Pintegration-tests` on the command line. This will download DynamoDB Local and run an integration test against the local instance with CloudWatch metrics disabled.
