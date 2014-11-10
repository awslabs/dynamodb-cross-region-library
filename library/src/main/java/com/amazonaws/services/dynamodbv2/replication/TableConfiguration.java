/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.replication;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

/**
 * Configuration for a table (a table replica) in a region.
 */
public interface TableConfiguration {
    /**
     * The replication status of a table.
     */
    public enum ReplicationTableStatus {
        /**
         * The table does not exist.
         */
        DOES_NOT_EXIST,
        /**
         * The table exists, but the replication process is not started yet.
         */
        NOT_STARTED,
        /**
         * The table is under bootstrapping state.
         */
        BOOTSTRAPPING,
        /**
         * The table is replicating update from/to other tables.
         */
        REPLICATING,
        /**
         * The replication process is stopped.
         */
        STOPPED
    }

    /**
     * Gets the CloudWatch client for the table.
     *
     * @return The cloud watch client
     */
    AmazonCloudWatchClient getCloudWatchClient();

    /**
     * Gets the cloud watch credentials for the table.
     *
     * @return The DynamodDB credentials
     */
    AWSCredentialsProvider getCloudWatchCredentialsProvider();

    /**
     * Gets the endpoint of the CloudWatch in the region that the table resides.
     *
     * @return The end point
     */
    String getCloudWatchEndpoint();

    /**
     * Gets the DynamoDB client for the table.
     *
     * @return The DynamoDB client
     */
    AmazonDynamoDB getDynamoDBClient();

    /**
     * Gets the DynamodDB credentials for the table.
     *
     * @return The DynamodDB credentials
     */
    AWSCredentialsProvider getDynamoDBCredentialsProvider();

    /**
     * Gets the endpoint of DynamoDB in the region that the table resides.
     *
     * @return The end point
     */
    String getDynamoDBEndpoint();

    /**
     * Gets the Kinesis application name.
     * @return The Kinesis application name
     */
    String getKinesisApplicationName();

    /**
     * Gets the Kinesis client for the DynamoDB Streams.
     *
     * @return The KinesisClient
     */
    AmazonKinesis getKinesisClient();

    /**
     * Gets the Kinesis client library configuration for the table.
     *
     * @return The Kinesis client library configuration
     */
    KinesisClientLibConfiguration getKinesisClientLibConfiguration();

    /**
     * Gets the region that the table resides.
     *
     * @return The region name
     */
    String getRegion();

    /**
     * Gets number of update records (from other tables) successfully replicated to a given shard of this table.
     *
     * @param shardId
     *            The shardId to get
     * @return The number of update records (from other tables) successfully replicated to shardId of this table
     */
    long getShardAppliedRecordCount(String shardId);

    /**
     * Gets the status of the table.
     *
     * @return The status of the table
     */
    ReplicationTableStatus getStatus();

    /**
     * Gets the Streams credentials for the table.
     *
     * @return The Streams credentials
     */
    AWSCredentialsProvider getStreamsCredentialsProvider();

    /**
     * Gets the endpoint of DynamoDB Streams in the region that the table resides.
     *
     * @return The end point of DynamoDB Streams
     */
    String getStreamsEndpoint();

    /**
     * Gets the table name.
     *
     * @return The table name
     */
    String getTable();

    /**
     * Gets number of update records (from other tables) successfully replicated to this table.
     *
     * @return The number of update records (from other tables) successfully replicated to this table
     */
    long getTableAppliedRecordCount();

    /**
     * Whether the table has DynamoDB Streams.
     *
     * @return True iff the table has Streams
     */
    boolean hasStreams();

    /**
     * Increases the number of update records (from other tables) successfully replicated to a given shard of this
     * table. Called after each time we finish an apply task for the table.
     *
     * @param shardId
     *            The identifier of the target shard
     */
    void incAppliedRecordCount(String shardId);

    /**
     * Sets new status for the table.
     *
     * @param status
     *            The new status
     */
    void setStatus(ReplicationTableStatus status);
}
