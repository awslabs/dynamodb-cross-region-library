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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.dynamodbv2.replication.MultiRegionCheckpoint;
import com.amazonaws.services.dynamodbv2.replication.MultiRegionCheckpointFactory;
import com.amazonaws.services.dynamodbv2.replication.RegionReplicationWorker;
import com.amazonaws.services.dynamodbv2.replication.ShardSubscriber;
import com.amazonaws.services.dynamodbv2.replication.TableCloudWatchMetric;
import com.amazonaws.services.dynamodbv2.replication.impl.ShardSubscriberImplFactory.SequenceNumberComparator;
import com.amazonaws.services.dynamodbv2.replication.sdk.AmazonDynamoDBTimestampReplicationClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordObjectMapper;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Implementation of ShardSubscriber.
 */
public class ShardSubscriberImpl implements ShardSubscriber {
    /**
     * LOGGER.
     */
    protected static final Logger LOGGER = LoggerFactory.getLogger(ShardSubscriberImpl.class);

    /**
     * ObjectMapper for converting Kinesis {@link Record}s to DynamoDB Stream
     * {@link com.amazonaws.services.dynamodbv2.model.Record}s.
     */
    private static final ObjectMapper MAPPER = new RecordObjectMapper();
    /**
     * The checkpointer to checkpoint the progress of the shard subscriber.
     */
    private IRecordProcessorCheckpointer checkpointer;
    /**
     * The map for buffering {@link MultiRegionCheckpoint}. Key is the sequence number.
     */
    private final ConcurrentNavigableMap<String, MultiRegionCheckpoint> checkpoints;

    /**
     * Factory for producing {@link MultiRegionCheckpoint}s.
     */
    private final MultiRegionCheckpointFactory multiRegionCheckpointFactory;
    /**
     * The {@link RegionReplicationWorkers} that manages this subscriber.
     */
    private final RegionReplicationWorker replicationWorker;
    /**
     * Unique identifier for the subscriber, used for logging purpose.
     */
    private String shardSubscriberId;
    /**
     * The id of the shard that the this subscriber manages.
     */
    private String orginalShardId;
    /**
     * Sweeper that should be run on this Subscriber.
     */
    private MultiRegionCheckpointSweeper sweeper;
    /**
     * The sweeper thread that maintain checkpoints.
     */
    private ExecutorService sweeperService;

    /**
     * The table name.
     */
    private final String tableName;
    /**
     * The backoff time in millisecond before retrying a checkpoint.
     */
    private final long checkpointBackoffTime;
    /**
     * The time in millisecond between scans for successfully replicated updates.
     */
    private final long timeBetweenSweeps;

    /**
     * The cloud watch client used to publish the number of user writes.
     */
    private final AmazonCloudWatchClient cloudWatchClient;

    /**
     * The number of updates users make on the table, used for CloudWatch.
     */
    private final AtomicLong userWriteCount;

    /**
     * Constructs a Subscriber that creates Checkpoints based on the provided factory.
     *
     * @param tableName
     *            The table name
     * @param multiRegionCheckpointFactory
     *            The factory for producing {@link MultiRegionCheckpoint}s
     * @param replicationWorker
     *            The {@link RegionReplicationWorkers} that manages this subscriber
     * @param timeBetweenSweeps
     *            The time in millisecond between scans for successfully replicated updates.
     * @param checkpointBackoffTime
     *            The backoff time in millisecond before retrying a checkpoint
     */
    public ShardSubscriberImpl(final String tableName, final MultiRegionCheckpointFactory multiRegionCheckpointFactory,
        final RegionReplicationWorker replicationWorker, final long timeBetweenSweeps, final long checkpointBackoffTime) {
        this.tableName = tableName;
        this.multiRegionCheckpointFactory = multiRegionCheckpointFactory;
        this.replicationWorker = replicationWorker;
        this.timeBetweenSweeps = timeBetweenSweeps;
        this.checkpointBackoffTime = checkpointBackoffTime;
        checkpointer = null;

        checkpoints = new ConcurrentSkipListMap<String, MultiRegionCheckpoint>(new SequenceNumberComparator());
        sweeper = null;
        cloudWatchClient = replicationWorker.getReplicationConfiguration().getCloudWatchClient(
            replicationWorker.getRegionName(), tableName);
        userWriteCount = new AtomicLong(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ack(final String sequenceNumber, final String regionApplied, final String tableApplied) {
        getCheckpoints().get(sequenceNumber).ack(regionApplied, tableApplied);
    }

    /**
     * Converts Kinesis record to DynamoDB Streams record.
     *
     * @param record
     *            The Kinesis record
     * @return The DynamoDB Streams record
     * @throws IOException
     *             Exception in conversion
     */
    protected com.amazonaws.services.dynamodbv2.model.Record convertToStreamRecord(final Record record)
        throws IOException {
        final ByteBuffer data = record.getData();
        LOGGER.debug("deserializing record data: " + (new String(data.array())));
        return MAPPER.readValue(data.array(), com.amazonaws.services.dynamodbv2.model.Record.class);
    }

    /**
     * Gets the checkpointer.
     *
     * @return The IRecordProcessorCheckpointer
     */
    public IRecordProcessorCheckpointer getCheckpointer() {
        return checkpointer;
    }

    /**
     * Gets the MultiRegionCheckpoint map.
     *
     * @return The MultiRegionCheckpoint map.
     */
    public ConcurrentNavigableMap<String, MultiRegionCheckpoint> getCheckpointMap() {
        return getCheckpoints();
    }

    /**
     * @return the checkpoints
     */
    public ConcurrentNavigableMap<String, MultiRegionCheckpoint> getCheckpoints() {
        return checkpoints;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getLatestCheckpoint() {
        if (sweeper != null) {
            return sweeper.getLastCheckpoint();
        }
        return null;
    }

    /**
     * @return the multiRegionCheckpointFactory
     */
    public MultiRegionCheckpointFactory getMultiRegionCheckpointFactory() {
        return multiRegionCheckpointFactory;
    }

    /**
     * Gets the sequence number of the update that is ready to checkpoint.
     *
     * @return The sequence number of the next full acked update
     */
    private String getNextCheckpoint() {
        String nextCheckpoint = null;
        for (final Entry<String, MultiRegionCheckpoint> entry : checkpoints.entrySet()) {
            if (entry.getValue().isReadyToCheckpoint()) {
                nextCheckpoint = entry.getKey();
            } else {
                break;
            }
        }
        return nextCheckpoint;
    }

    /**
     * @return the replicationWorker
     */
    public RegionReplicationWorker getReplicationWorker() {
        return replicationWorker;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getShardId() {
        if (orginalShardId == null) {
            throw new IllegalStateException("Subscriber not yet initialized");
        }
        return orginalShardId;
    }

    /**
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(final String shardId) {
        try {
            shardSubscriberId = "{" + InetAddress.getLocalHost().getCanonicalHostName() + ", " + shardId + ", "
                + UUID.randomUUID() + "}";
        } catch (final UnknownHostException e) {
            shardSubscriberId = "{" + shardId + ", " + UUID.randomUUID() + "}";
        }
        orginalShardId = shardId;
        // Registration will throw a NullPointerException if subscriberId is not set.
        getReplicationWorker().registerShardSubscriber(this);
        LOGGER.debug("registered shardSubscriber: " + shardSubscriberId);

        // Create and start sweeper
        sweeper = new MultiRegionCheckpointSweeper(replicationWorker, tableName, shardId, checkpoints,
            timeBetweenSweeps, checkpointBackoffTime);

        sweeperService = Executors.newSingleThreadExecutor();
        sweeperService.submit(sweeper);
        sweeperService.shutdown();
        LOGGER.debug("Sweepers initialized for shard subscriber: " + shardSubscriberId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(final List<Record> records, final IRecordProcessorCheckpointer recordCheckpointer) {
        try {
            setCheckpointer(recordCheckpointer);
            LOGGER.debug("Table " + tableName + ", Shardsubscriber " + getShardId() + "is processing " + records.size()
                + " records");
            for (final Record record : records) {
                processSingleRecord(record);
            }
            LOGGER.debug("DONE PROCESSING RECORDS!!");

            final String namespace = replicationWorker.getReplicationConfiguration().getKinesisApplicationName(
                replicationWorker.getRegionName(), tableName);

            final ArrayList<MetricDatum> metricData = new ArrayList<MetricDatum>();

            synchronized (this) {
                if (cloudWatchClient != null && userWriteCount.get() > 0) {
                    LOGGER.debug("Table " + replicationWorker.getRegionName() + ":" + tableName + ", Shardsubscriber "
                        + getShardId() + " publishes " + userWriteCount + " user writes to cloudwatch");
                    metricData.add(new MetricDatum().withMetricName(TableCloudWatchMetric.NUMBER_USER_WRITES)
                        .withUnit(StandardUnit.Count).withValue((double) userWriteCount.get()));
                    final PutMetricDataRequest putMetricDataRequest = new PutMetricDataRequest().withNamespace(
                        namespace).withMetricData(metricData);
                    cloudWatchClient.putMetricData(putMetricDataRequest);
                    userWriteCount.set(0);
                }
            }

        } catch (final Exception e) {
            LOGGER.error("Shard Subscriber: " + shardSubscriberId + " encountered an exception in process records", e);
            throw e;
        }
    }

    /**
     * Helper method for processRecord(). Processes a single record.
     *
     * @param record
     *            The record to process
     */
    protected void processSingleRecord(final Record record) {
        com.amazonaws.services.dynamodbv2.model.Record streamRecord;
        try {
            LOGGER.debug("converting record: " + record);
            streamRecord = convertToStreamRecord(record);
            LOGGER.debug("got stream record " + streamRecord);
        } catch (final IOException e) {
            LOGGER.error("Could not deserialize DynamoDB Stream Record from Kinesis Record: " + record, e);
            return;
        }

        // if it is a user update, delete the user update flag and apply to other regions
        if (streamRecord.getDynamodb().getNewImage() != null) {
            if (streamRecord.getDynamodb().getNewImage()
                .containsKey(AmazonDynamoDBTimestampReplicationClient.USER_UPDATE_KEY)) {
                String createdTime = null;
                if (replicationWorker.getReplicationPolicy() instanceof BasicTimestampReplicationPolicy) {
                    createdTime = streamRecord.getDynamodb().getNewImage()
                        .get(BasicTimestampReplicationPolicy.TIMESTAMP_KEY).getS();
                }
                final MultiRegionCheckpoint checkpoint = multiRegionCheckpointFactory.createCheckpoint(
                    record.getSequenceNumber(), createdTime);
                getCheckpoints().put(record.getSequenceNumber(), checkpoint);
                streamRecord.getDynamodb().getNewImage()
                    .remove(AmazonDynamoDBTimestampReplicationClient.USER_UPDATE_KEY);
                replicationWorker.getApplierProxy().apply(streamRecord, replicationWorker.getRegionName(),
                    tableName, this.getShardId());
                if (cloudWatchClient != null) {
                    userWriteCount.incrementAndGet();
                }
                LOGGER.debug("applied streamRecord from shard: " + this.getShardId() + " --> "
                    + streamRecord);
            } else {
                LOGGER.debug("Not replicating non-user write: " + streamRecord);
            }
        } else {
            LOGGER.warn("Detected REMOVE record even though only TOMBSTONE delete is supported!");
        }
    }

    /**
     * Sets the checkpointer.
     *
     * @param recordCheckpointer
     *            The checkpointer to checkpoint the progress of the shard subscriber
     */
    protected void setCheckpointer(final IRecordProcessorCheckpointer recordCheckpointer) {
        if (checkpointer == null) {
            checkpointer = recordCheckpointer;
            sweeper.setCheckpointer(checkpointer);
            LOGGER.debug("set checkpointer for sweeper " + sweeper);

        } else {
            // Make sure we get the same checkpointer
            assert checkpointer == recordCheckpointer;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(final IRecordProcessorCheckpointer recordCheckpointer, final ShutdownReason reason) {
        switch (reason) {
            case TERMINATE:
                // Wait for remaining records to process and checkpoint
                while (getCheckpoints().size() > 0) {
                    try {
                        Thread.sleep(checkpointBackoffTime);
                    } catch (final InterruptedException e2) {
                        LOGGER.warn("ShardSubscriber " + shardSubscriberId + " has been interrupted");
                    }
                    assert true; // Placeholder for actual logic
                }
                sweeper.shutdown();
                boolean retry;
                do {
                    retry = false;
                    try {
                        assert checkpointer != null;
                        final String nextCheckpoint = getNextCheckpoint();
                        if (nextCheckpoint != null) {
                            checkpointer.checkpoint(nextCheckpoint);
                        }
                    } catch (final ThrottlingException e) {
                        // Retry
                        // ThrottlingException: Checkpointing too frequently
                        retry = true;
                        try {
                            Thread.sleep(checkpointBackoffTime);
                        } catch (final InterruptedException e2) {
                            LOGGER.warn("ShardSubscriber " + shardSubscriberId + " has been interrupted", e);
                        }
                    } catch (final KinesisClientLibDependencyException e) {
                        // Retry
                        // KinesisClientLibDependencyException: Encountered an issue when storing the checkpoint
                        retry = true;
                        try {
                            Thread.sleep(checkpointBackoffTime);
                        } catch (final InterruptedException e2) {
                            LOGGER.warn("ShardSubscriber " + shardSubscriberId + " has been interrupted", e);
                        }
                    } catch (final ShutdownException e) {
                        // No retry
                        // ShutdownException: Another instance may have started processing these records. Log and quit
                        LOGGER.warn("ShardSubscriber " + shardSubscriberId + " could not checkpoint", e);
                    } catch (final InvalidStateException e) {
                        // No retry
                        // InvalidStateException: Problem with application (i.e. DynamoDB table has been deleted)
                        LOGGER.warn("ShardSubscriber " + shardSubscriberId + " could not checkpoint", e);
                    }
                } while (retry);
                break;
            case ZOMBIE:
                // Do nothing. This Subscriber was not responding
                break;
            default:
                throw new UnsupportedOperationException("Unsupported shutdown reason: " + reason);
        }
        getReplicationWorker().unregisterShardSubscriber(this);
    }

    /**
     * Shuts down using the local checkpointer.
     *
     * @param reason
     *            The shutdown reason
     */
    public void shutdown(final ShutdownReason reason) {
        if (checkpointer != null) {
            shutdown(checkpointer, reason);
        }
    }
}
