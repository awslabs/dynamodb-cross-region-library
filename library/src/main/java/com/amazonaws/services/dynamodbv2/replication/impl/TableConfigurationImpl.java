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
package com.amazonaws.services.dynamodbv2.replication.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.replication.TableConfiguration;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

/**
 * Implementation of {@link TableConfiguration}.
 *
 */
public class TableConfigurationImpl implements TableConfiguration {
    /**
     * DynamoDB Streams record batch limit 
     */
    private static final int DYNAMODB_STREAMS_RECORD_LIMIT = 1000;

    /**
     * The default application name.
     */
    public static final String APPLICATION_PREFIX = "CrossRegionReplicationApplication";
    /**
     * The stream service name.
     */
    public static final String STREAMS_SERVICE_NAME = "dynamodb";

    /**
     * Creates a temporary table Configuration.
     *
     * @param region
     *            The region name
     * @param table
     *            The table name
     * @return The temporary table configuration
     */
    public static TableConfigurationImpl buildNonexistent(final String region, final String table) {
        final TableConfigurationImpl ret = new TableConfigurationImpl(region, table);
        ret.status = ReplicationTableStatus.DOES_NOT_EXIST;
        return ret;
    }

    /**
     * The application name.
     */
    private final String kinesisApplicationName;
    /**
     * The cloud watch client for the table.
     */
    private final AmazonCloudWatchClient cloudWatchClient;
    /**
     * The cloud watch credentials for the region that the table resides.
     */
    private final AWSCredentialsProvider cloudWatchCredentialsProvider;
    /**
     * The cloud watch endpoint of the region that the table resides.
     */
    private final String cloudWatchEndpoint;
    /**
     * The DynamoDB client for the table.
     */
    private final AmazonDynamoDBClient dynamoDBClient;
    /**
     * The DynamoDB credentials for the region that the table resides.
     */
    private final AWSCredentialsProvider dynamoDBCredentialsProvider;
    /**
     * The DynamoDB endpoint of the region that the table resides.
     */
    private final String dynamoDBEndpoint;
    /**
     * True iff the table has Streams.
     */
    private final boolean hasStreams;
    /**
     * The Kinesis client for the Streams.
     */
    private final AmazonDynamoDBStreamsAdapterClient kinesisClient;
    /**
     * The Kinesis client library configuration for the table.
     */
    private KinesisClientLibConfiguration kinesisClientLibConfiguration;
    /**
     * The name of the region that the table resides.
     */
    private final String region;
    /**
     * The status of the table.
     */
    private ReplicationTableStatus status;
    /**
     * The table name.
     */
    private final String table;
    /**
     * The Streams client for the table.
     */

    private final AmazonDynamoDBStreamsClient streamsClient;
    /**
     * The Streams credentials for the region that the table resides.
     */
    private final AWSCredentialsProvider streamsCredentialsProvider;

    /**
     * The DynamoDB Streams endpoint of the region that the table resides.
     */
    private final String streamsEndpoint;

    /**
     * The number of update records successfully applied on this table, used for CloudWatch.
     */
    private AtomicLong tableAppliedRecordCount;

    /**
     * The number of update records successfully applied on each shard of this table, used for CloudWatch.
     */
    private HashMap<String, AtomicLong> shardAppliedRecordCounts;

    /**
     * Constructs a temporary Table Configuration.
     *
     * @param region
     *            The region name
     * @param table
     *            The table name
     */
    private TableConfigurationImpl(final String region, final String table) {
        streamsEndpoint = null;
        streamsCredentialsProvider = null;
        streamsClient = null;
        dynamoDBClient = null;
        dynamoDBCredentialsProvider = null;
        dynamoDBEndpoint = null;
        this.table = table;
        this.region = region;
        kinesisClient = null;
        kinesisApplicationName = null;
        hasStreams = false;
        cloudWatchClient = null;
        cloudWatchCredentialsProvider = null;
        cloudWatchEndpoint = null;
    }

    /**
     *
     * @param region
     *            The region name
     * @param table
     *            The table name
     * @param cloudWatchEndpoint
     *            The endpoint of CloudWatch for the table
     * @param dynamoDBEndpoint
     *            The region DynamoDB endpoint
     * @param streamsEndpoint
     *            The region DynamoDB Streams endpoint
     * @param cloudWatchCredentialsProvider
     *            The Cloudwatch credentials for the table
     * @param dynamoDBCredentialsProvider
     *            The DynamoDB credentials
     * @param streamsCredentialsProvider
     *            The DynamoDB Streams credentials
     */
    public TableConfigurationImpl(final String region, final String table, final String cloudWatchEndpoint,
        final String dynamoDBEndpoint, final String streamsEndpoint,
        final AWSCredentialsProvider cloudWatchCredentialsProvider,
        final AWSCredentialsProvider dynamoDBCredentialsProvider,
        final AWSCredentialsProvider streamsCredentialsProvider) {
        this(APPLICATION_PREFIX + "-" + region + "-" + table, region, table, cloudWatchEndpoint, dynamoDBEndpoint,
            streamsEndpoint, cloudWatchCredentialsProvider, dynamoDBCredentialsProvider, streamsCredentialsProvider);
    }

    /**
     *
     * @param applicationName
     *            The application name
     * @param region
     *            The region name
     * @param table
     *            The table name
     * @param cloudWatchEndpoint
     *            The endpoint of CloudWatch for the table
     * @param dynamoDBEndpoint
     *            The region DynamoDB endpoint
     * @param streamsEndpoint
     *            The region DynamoDB Streams endpoint
     * @param cloudWatchCredentialsProvider
     *            The Cloudwatch credentials for the table
     * @param dynamoDBCredentialsProvider
     *            The DynamoDB credentials
     * @param streamsCredentialsProvider
     *            The DynamoDB Streams credentials
     */
    public TableConfigurationImpl(final String applicationName, final String region, final String table,
        final String cloudWatchEndpoint, final String dynamoDBEndpoint, final String streamsEndpoint,
        final AWSCredentialsProvider cloudWatchCredentialsProvider,
        final AWSCredentialsProvider dynamoDBCredentialsProvider,
        final AWSCredentialsProvider streamsCredentialsProvider) {
        kinesisApplicationName = applicationName;
        this.region = region;
        this.table = table;
        this.cloudWatchCredentialsProvider = cloudWatchCredentialsProvider;
        this.cloudWatchEndpoint = cloudWatchEndpoint;
        this.dynamoDBCredentialsProvider = dynamoDBCredentialsProvider;
        this.streamsCredentialsProvider = streamsCredentialsProvider;
        this.dynamoDBEndpoint = dynamoDBEndpoint;
        this.streamsEndpoint = streamsEndpoint;
        status = ReplicationTableStatus.NOT_STARTED;

        streamsClient = new AmazonDynamoDBStreamsClient(streamsCredentialsProvider);
        streamsClient.setServiceNameIntern(STREAMS_SERVICE_NAME);
        streamsClient.setEndpoint(streamsEndpoint, STREAMS_SERVICE_NAME, region);

        dynamoDBClient = new AmazonDynamoDBClient(dynamoDBCredentialsProvider);
        dynamoDBClient.setEndpoint(dynamoDBEndpoint, STREAMS_SERVICE_NAME, region);

        if (cloudWatchEndpoint != null && cloudWatchCredentialsProvider != null) {
            cloudWatchClient = new AmazonCloudWatchClient(cloudWatchCredentialsProvider);
            cloudWatchClient.setEndpoint(cloudWatchEndpoint);
        } else {
            cloudWatchClient = null;
        }

        final DescribeTableResult tableResult = dynamoDBClient.describeTable(table);

        final TableDescription tableDescription = tableResult.getTable();
        hasStreams = tableDescription.getStreamSpecification().getStreamEnabled();
        if (hasStreams) {
            kinesisClientLibConfiguration = new KinesisClientLibConfiguration(applicationName, getStreamId(),
                streamsCredentialsProvider, getWorkerId()).withInitialPositionInStream(
                InitialPositionInStream.TRIM_HORIZON).withMaxRecords(DYNAMODB_STREAMS_RECORD_LIMIT);
            kinesisClient = new AmazonDynamoDBStreamsAdapterClient(streamsCredentialsProvider);
            kinesisClient.setEndpoint(streamsEndpoint, STREAMS_SERVICE_NAME, region);
        } else {
            kinesisClientLibConfiguration = null;
            kinesisClient = null;
        }
        tableAppliedRecordCount = new AtomicLong(0);
        shardAppliedRecordCounts = new HashMap<String, AtomicLong>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AmazonCloudWatchClient getCloudWatchClient() {
        return cloudWatchClient;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AWSCredentialsProvider getCloudWatchCredentialsProvider() {
        return cloudWatchCredentialsProvider;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCloudWatchEndpoint() {
        return cloudWatchEndpoint;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AmazonDynamoDBClient getDynamoDBClient() {
        return dynamoDBClient;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AWSCredentialsProvider getDynamoDBCredentialsProvider() {
        return dynamoDBCredentialsProvider;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDynamoDBEndpoint() {
        return dynamoDBEndpoint;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getKinesisApplicationName() {
        return kinesisApplicationName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AmazonKinesis getKinesisClient() {
        return kinesisClient;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KinesisClientLibConfiguration getKinesisClientLibConfiguration() {
        return kinesisClientLibConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getRegion() {
        return region;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getShardAppliedRecordCount(final String shardId) {
        if (shardAppliedRecordCounts.containsKey(shardId)) {
            return shardAppliedRecordCounts.get(shardId).get();
        }
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReplicationTableStatus getStatus() {
        return status;
    }

    /**
     * Retrieves the stream id.
     *
     * @return The stream id
     */
    private String getStreamId() {
        return dynamoDBClient.describeTable(getTable()).getTable().getLatestStreamId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AWSCredentialsProvider getStreamsCredentialsProvider() {
        return streamsCredentialsProvider;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getStreamsEndpoint() {
        return streamsEndpoint;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTable() {
        return table;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized long getTableAppliedRecordCount() {
        return tableAppliedRecordCount.get();
    }

    /**
     * Generates id for the StreamsWorker.
     *
     * @return The worker id.
     */
    private String getWorkerId() {
        String workerId;
        try {
            workerId = region + ":" + InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        } catch (final UnknownHostException e) {
            workerId = region + ":" + UUID.randomUUID();
        }
        return workerId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasStreams() {
        return hasStreams;
    }

    @Override
    public void incAppliedRecordCount(final String shardId) {
        synchronized (shardAppliedRecordCounts) {
            if (!shardAppliedRecordCounts.containsKey(shardId)) {
                shardAppliedRecordCounts.put(shardId, new AtomicLong(0));
            }
        }
        shardAppliedRecordCounts.get(shardId).incrementAndGet();
        tableAppliedRecordCount.incrementAndGet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStatus(final ReplicationTableStatus tableStatus) {
        status = tableStatus;
    }
}
