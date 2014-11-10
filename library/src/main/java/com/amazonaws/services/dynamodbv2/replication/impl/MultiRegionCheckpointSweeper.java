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

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.dynamodbv2.replication.MultiRegionCheckpoint;
import com.amazonaws.services.dynamodbv2.replication.RegionReplicationWorker;
import com.amazonaws.services.dynamodbv2.replication.TableCloudWatchMetric;
import com.amazonaws.services.dynamodbv2.replication.TableConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;

/**
 * A Runnable that can run concurrently with a Subscriber to process {@link MultiRegionCheckpoint}s. Can provide many
 * useful functions like coalescing updates to the same item or reporting statistics to the nanny.
 */
public class MultiRegionCheckpointSweeper implements Runnable {
    /**
     * Logger for {@link MultiRegionCheckpointSweeper}.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiRegionCheckpointSweeper.class.getName());
    /**
     * The map of sequence number to {@link MultiRegionCheckpoint} the {@link MultiRegionCheckpointSweeper} will process
     * periodically.
     */
    private final ConcurrentNavigableMap<String, MultiRegionCheckpoint> checkpoints;
    /**
     * Time to wait between sweeps of the checkpoints in milliseconds.
     */
    private final long timeBetweenSweepsMillis;
    /**
     * The unique identifier for the sweeper. Used for logging.
     */
    private final String sweeperId;
    /**
     * Flag for shutting down the {@link MultiRegionCheckpointSweeper}.
     */
    private boolean isShutdown;
    /**
     * The checkpointer to checkpoint the progress of the shard subscriber.
     */
    private IRecordProcessorCheckpointer checkpointer;
    /**
     * The last checkpoint the sweeper sets.
     */
    private String lastCheckpoint;
    /**
     * Backoff time in millisecond before retrying a checkpoint.
     */
    private final long checkpointBackoffTime;
    /**
     * The replication worker.
     */
    private final RegionReplicationWorker replicationWorker;
    /**
     * The number of update records successfully replicated to destination tables, used for CloudWatch.
     */
    private long checkpointedRecordCount;
    /**
     * The number of update other tables propagates to the table, used for CloudWatch.
     */
    private long appliedRecordCount;
    /**
     * The table that the this sweeper works on.
     */
    private final String table;
    /**
     * The accumulated end-to-end latency of update records successfully replicated to destination tables, used for
     * CloudWatch.
     */
    private long accumulatedLatency;
    /**
     * The identifier of the shard that the this sweeper works on.
     */
    private final String shardId;
    /**
     * The configuration of the table that the this sweeper works on.
     */
    private final TableConfiguration tableConfig;

    /**
     * Constructs a CheckpointSweeper that will sweep the provided checkpoints map. It will wait the specified amount of
     * time between iterations.
     *
     * @param replicationWorker
     *            The replication worker
     * @param table
     *            The table that the this sweeper works on
     * @param shardId
     *            The identifier of the shard that the this sweeper works on
     * @param checkpoints
     *            The map of checkpoints the sweeper will process
     * @param timeBetweenSweepsMillis
     *            The amount of time to wait between sweep operations
     * @param checkpointBackoffTime
     *            Backoff time in millisecond before retrying a checkpoint
     */
    public MultiRegionCheckpointSweeper(final RegionReplicationWorker replicationWorker, final String table,
        final String shardId, final ConcurrentNavigableMap<String, MultiRegionCheckpoint> checkpoints,
        final long timeBetweenSweepsMillis, final long checkpointBackoffTime) {
        this.replicationWorker = replicationWorker;
        this.checkpoints = checkpoints;
        this.timeBetweenSweepsMillis = timeBetweenSweepsMillis;
        this.shardId = shardId;
        this.checkpointBackoffTime = checkpointBackoffTime;
        this.table = table;
        sweeperId = this.getClass().getName() + UUID.randomUUID();
        isShutdown = false;
        checkpointer = null;
        lastCheckpoint = null;
        tableConfig = replicationWorker.getReplicationConfiguration()
            .getRegionConfiguration(replicationWorker.getRegionName()).getTableConfiguration(table);
        checkpointedRecordCount = 0;
        accumulatedLatency = 0;
        appliedRecordCount = 0;
    }

    /**
     * @return the checkpointer
     */
    public IRecordProcessorCheckpointer getCheckpointer() {
        return checkpointer;
    }

    /**
     * Gets the map of sequence number to {@link MultiRegionCheckpoint} that this sweeper processes.
     *
     * @return The map of sequence number to {@link MultiRegionCheckpoint} that this sweeper processes
     */
    public ConcurrentNavigableMap<String, MultiRegionCheckpoint> getCheckpoints() {
        return checkpoints;
    }

    /**
     *
     * @return The last checkpoint
     */
    public String getLastCheckpoint() {
        return lastCheckpoint;
    }

    /**
     * @return the replicationWorker
     */
    public RegionReplicationWorker getReplicationWorker() {
        return replicationWorker;
    }

    /**
     * @return the sweeperId
     */
    public String getSweeperId() {
        return sweeperId;
    }

    /**
     * @return the isShutdown
     */
    public boolean isShutdown() {
        return isShutdown;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        LOGGER.debug("Started sweeper " + sweeperId);
        while (!isShutdown()) {
            LOGGER.debug("SWEEEP: " + sweeperId + " I has checkpointer: " + checkpointer + " and checkpoints "
                + checkpoints);
            try {
                sweep();
                try {
                    Thread.sleep(timeBetweenSweepsMillis);
                } catch (final InterruptedException e) {
                    LOGGER.warn(sweeperId + " was interrupted");
                }
            } catch (final Exception e) {
                LOGGER.error("Exception in sweeper!", e);
            }

            if (tableConfig != null && tableConfig.getCloudWatchClient() != null) {
                final long newNoAppliedRecords = tableConfig.getTableAppliedRecordCount();
                if (checkpointedRecordCount != 0 || newNoAppliedRecords != appliedRecordCount) {
                    final AmazonCloudWatchClient cloudWatchClient = tableConfig.getCloudWatchClient();
                    final String namespace = tableConfig.getKinesisApplicationName();

                    final ArrayList<MetricDatum> metricData = new ArrayList<MetricDatum>();
                    LOGGER.debug("Sweeper for table " + replicationWorker.getRegionName() + ":" + table + " publishes "
                        + (newNoAppliedRecords - appliedRecordCount) + " applied records " + checkpointedRecordCount
                        + " checkpointed records with total time " + accumulatedLatency + "ms to cloudwatch");

                    if (checkpointedRecordCount != 0) {
                        metricData.add(new MetricDatum()
                            .withMetricName(TableCloudWatchMetric.NUMBER_CHECKPOINTED_RECORDS)
                            .withUnit(StandardUnit.Count).withValue((double) checkpointedRecordCount));
                        metricData.add(new MetricDatum()
                            .withMetricName(TableCloudWatchMetric.ACCUMULATED_RECORD_LATENCY)
                            .withUnit(StandardUnit.Milliseconds).withValue(0.0 + accumulatedLatency));
                        checkpointedRecordCount = 0;
                        accumulatedLatency = 0;
                    }

                    if (newNoAppliedRecords != appliedRecordCount) {
                        metricData.add(new MetricDatum()
                            .withMetricName(TableCloudWatchMetric.NUMBER_REPLICATION_WRITES)
                            .withUnit(StandardUnit.Count).withValue(0.0 + newNoAppliedRecords - appliedRecordCount));
                        appliedRecordCount = newNoAppliedRecords;
                    }

                    final PutMetricDataRequest putMetricDataRequest = new PutMetricDataRequest().withNamespace(
                        namespace).withMetricData(metricData);
                    cloudWatchClient.putMetricData(putMetricDataRequest);

                }

            }

        }
    }

    /**
     * Sets the checkpointer.
     *
     * @param checkpointer
     *            The checkpointer to checkpoint the progress of the shard subscriber
     */
    public void setCheckpointer(final IRecordProcessorCheckpointer checkpointer) {
        this.checkpointer = checkpointer;
    }

    /**
     * Shuts down the {@link MultiRegionCheckpointSweeper} instance. The sweeper will finish the current sweep.
     */
    public void shutdown() {
        LOGGER.debug("Shutting down sweeper " + sweeperId);
        isShutdown = true;
    }

    /**
     * Performs a sweep of the checkpoints map. May perform read or write operations on the checkpoints. Please note the
     * map may be modified by other sweepers while processing.
     */
    private void sweep() {
        String nextCheckpoint = lastCheckpoint;
        long readyCount = 0;
        while (!checkpoints.isEmpty()) {
            final String sequenceNumber = checkpoints.firstKey();
            if (checkpoints.get(sequenceNumber).isReadyToCheckpoint()) {
                nextCheckpoint = sequenceNumber;
                final MultiRegionCheckpoint checkpoint = checkpoints.remove(sequenceNumber);
                readyCount++;
                accumulatedLatency += checkpoint.getLatencyMillis();
                LOGGER.debug("Replication completed for record: " + checkpoint.getSequenceNumber());
                LOGGER.info("Latency per region for record " + checkpoint.getSequenceNumber() + " "
                    + checkpoint.getLatencyPerTableMillis().toString());
            } else {
                break;
            }
        }
        if (nextCheckpoint != null && !nextCheckpoint.equals(lastCheckpoint)) {
            LOGGER.debug("TIME TO CHECKPOINT");
            boolean retry;
            do {
                retry = false;
                try {
                    assert checkpointer != null;
                    checkpointer.checkpoint(nextCheckpoint);
                } catch (final ThrottlingException e) {
                    // Retry
                    // ThrottlingException: Checkpointing too frequently
                    retry = true;
                    try {
                        Thread.sleep(checkpointBackoffTime);
                    } catch (final InterruptedException e2) {
                        LOGGER.warn("Sweeper " + sweeperId + " has been interrupted", e);
                    }
                } catch (final KinesisClientLibDependencyException e) {
                    // Retry
                    // KinesisClientLibDependencyException: Encountered an issue when storing the checkpoint
                    retry = true;
                    try {
                        Thread.sleep(checkpointBackoffTime);
                    } catch (final InterruptedException e2) {
                        LOGGER.warn("Sweeper " + sweeperId + " has been interrupted", e);
                    }
                } catch (final ShutdownException e) {
                    // No retry
                    // ShutdownException: Another instance may have started processing these records. Log and quit
                    LOGGER.warn("Sweeper " + sweeperId + " could not checkpoint", e);
                } catch (final InvalidStateException e) {
                    // No retry
                    // InvalidStateException: Problem with application (i.e. DynamoDB table has been deleted)
                    LOGGER.warn("Sweeper " + sweeperId + " could not checkpoint", e);
                }
            } while (retry);
            LOGGER.info("Checkpoint record:" + nextCheckpoint);
            lastCheckpoint = nextCheckpoint;
            checkpointedRecordCount += readyCount;
        }

    }
}
