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
package com.amazonaws.services.dynamodbv2.replication.catchup;

import com.amazonaws.services.dynamodbv2.replication.MultiRegionCheckpointFactory;
import com.amazonaws.services.dynamodbv2.replication.RegionReplicationWorker;
import com.amazonaws.services.dynamodbv2.replication.ReplicationCheckpoint;
import com.amazonaws.services.dynamodbv2.replication.impl.BitSetMultiRegionCheckpointFactory;
import com.amazonaws.services.dynamodbv2.replication.impl.ShardSubscriberImplFactory;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;

/**
 * The factory to produce CatchupShardSubscribers.
 */
public class CatchupShardSubscriberFactory extends ShardSubscriberImplFactory {
    /**
     * The {@link ReplicationCheckpoint} to start catching up.
     */
    private final ReplicationCheckpoint fromCheckpoint;
    /**
     * The {@link ReplicationCheckpoint} to stop catching up.
     */
    private final ReplicationCheckpoint toCheckpoint;

    /**
     * @param tableName
     *            The table name
     * @param regionReplicationWorker
     *            The {@link RegionReplicationWorkers} that manages this subscriber
     * @param checkpointBackoffTime
     *            The backoff time in millisecond before retrying a checkpoint
     * @param timeBetweenSweeps
     *            The time in millisecond between scans for successfully replicated updates.
     * @param fromCheckpoint
     *            The {@link ReplicationCheckpoint} to start catching up
     * @param toCheckpoint
     *            The {@link ReplicationCheckpoint} to stop catching up
     */
    public CatchupShardSubscriberFactory(final String tableName, final RegionReplicationWorker regionReplicationWorker,
        final long checkpointBackoffTime, final long timeBetweenSweeps, final ReplicationCheckpoint fromCheckpoint,
        final ReplicationCheckpoint toCheckpoint) {
        super(tableName, regionReplicationWorker, checkpointBackoffTime, timeBetweenSweeps);
        this.fromCheckpoint = fromCheckpoint;
        this.toCheckpoint = toCheckpoint;
    }

    /**
     * @param tableName
     *            The table name
     * @param regionReplicationWorker
     *            The {@link RegionReplicationWorkers} that manages this subscriber
     * @param fromCheckpoint
     *            The {@link ReplicationCheckpoint} to start catching up
     * @param toCheckpoint
     *            The {@link ReplicationCheckpoint} to stop catching up
     */
    public CatchupShardSubscriberFactory(final String tableName, final RegionReplicationWorker regionReplicationWorker,
        final ReplicationCheckpoint fromCheckpoint, final ReplicationCheckpoint toCheckpoint) {
        super(tableName, regionReplicationWorker);
        this.fromCheckpoint = fromCheckpoint;
        this.toCheckpoint = toCheckpoint;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IRecordProcessor createProcessor() {
        assert getReplicationWorker() != null && getTable() != null;
        final MultiRegionCheckpointFactory multiRegionCheckpointFactory = new BitSetMultiRegionCheckpointFactory(
            getReplicationWorker().getReplicationConfiguration());

        if (fromCheckpoint.getRegion() != getReplicationWorker().getRegionName()
            || toCheckpoint.getRegion() != getReplicationWorker().getRegionName()
            || fromCheckpoint.getTable() != getTable() || toCheckpoint.getTable() != getTable()) {
            throw new IllegalArgumentException("Imcompatible checkpoints");
        }
        return new CatchupShardSubscriber(getTable(), multiRegionCheckpointFactory, getReplicationWorker(),
            getTimeBetweenSweeps(), getCheckpointBackoffTime(), fromCheckpoint.getSequenceNumberMap(),
            toCheckpoint.getSequenceNumberMap());

    }

    /**
     * @return the fromCheckpoint
     */
    public ReplicationCheckpoint getFromCheckpoint() {
        return fromCheckpoint;
    }

    /**
     * @return the toCheckpoint
     */
    public ReplicationCheckpoint getToCheckpoint() {
        return toCheckpoint;
    }
}
