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

import java.util.Comparator;

import com.amazonaws.services.dynamodbv2.replication.MultiRegionCheckpointFactory;
import com.amazonaws.services.dynamodbv2.replication.RegionReplicationWorker;
import com.amazonaws.services.dynamodbv2.replication.ReplicationCheckpointUtil;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

/**
 * The factory to produce ShardSubscribers.
 */
public class ShardSubscriberImplFactory implements IRecordProcessorFactory {
    /**
     * Sequence number comparator.
     *
     */
    public static class SequenceNumberComparator implements Comparator<String> {

        @Override
        public int compare(final String sequenceNumber1, final String sequenceNumber2) {
            return ReplicationCheckpointUtil.compareSequenceNumbers(sequenceNumber1, sequenceNumber2);
        }
    }

    /**
     * Default checkpoint backoff time.
     */
    public static final long DEFAULT_CHECKPOINT_BACKOFF_TIME_IN_MILLIS = 3000L;

    /**
     * Default interval between sweeper scans.
     */
    public static final long DEFAULT_TIME_BETWEEN_SWEEPER_SCAN_IN_MILLIS = 6000L;

    /**
     * The {@link RegionReplicationWorkers} that manages all subscribers created by this factory.
     */
    private final RegionReplicationWorker replicationWorker;

    /**
     * The table name.
     */
    private final String table;
    /**
     * The backoff time in millisecond before retrying a checkpoint.
     */
    private final long checkpointBackoffTime;
    /**
     * The time in millisecond between scans for successfully replicated updates.
     */
    private final long timeBetweenSweeps;

    /**
     * Constructor.
     *
     * @param tableName
     *            The table name
     * @param regionReplicationWorker
     *            The replication worker
     */
    public ShardSubscriberImplFactory(final String tableName, final RegionReplicationWorker regionReplicationWorker) {
        table = tableName;
        replicationWorker = regionReplicationWorker;
        checkpointBackoffTime = DEFAULT_CHECKPOINT_BACKOFF_TIME_IN_MILLIS;
        timeBetweenSweeps = DEFAULT_TIME_BETWEEN_SWEEPER_SCAN_IN_MILLIS;
    }

    /**
     * Constructor.
     *
     * @param tableName
     *            The table name
     * @param regionReplicationWorker
     *            The replication worker
     * @param checkpointBackoffTime
     *            The backoff time in millisecond before retrying a checkpoint
     * @param timeBetweenSweeps
     *            The time in millisecond between scans for successfully replicated updates.
     */
    public ShardSubscriberImplFactory(final String tableName, final RegionReplicationWorker regionReplicationWorker,
        final long checkpointBackoffTime, final long timeBetweenSweeps) {
        table = tableName;
        replicationWorker = regionReplicationWorker;
        this.checkpointBackoffTime = checkpointBackoffTime;
        this.timeBetweenSweeps = timeBetweenSweeps;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IRecordProcessor createProcessor() {
        final MultiRegionCheckpointFactory multiRegionCheckpointFactory = new BitSetMultiRegionCheckpointFactory(
            replicationWorker.getReplicationConfiguration());
        return new ShardSubscriberImpl(table, multiRegionCheckpointFactory, replicationWorker, timeBetweenSweeps,
            checkpointBackoffTime);
    }

    /**
     * @return the checkpointBackoffTime
     */
    public long getCheckpointBackoffTime() {
        return checkpointBackoffTime;
    }

    /**
     * @return the replicationWorker
     */
    public RegionReplicationWorker getReplicationWorker() {
        return replicationWorker;
    }

    /**
     * @return the table
     */
    public String getTable() {
        return table;
    }

    /**
     * @return the timeBetweenSweeps
     */
    public long getTimeBetweenSweeps() {
        return timeBetweenSweeps;
    }

}
