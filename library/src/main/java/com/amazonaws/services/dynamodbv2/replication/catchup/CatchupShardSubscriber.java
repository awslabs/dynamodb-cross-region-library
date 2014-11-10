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

import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.replication.MultiRegionCheckpoint;
import com.amazonaws.services.dynamodbv2.replication.MultiRegionCheckpointFactory;
import com.amazonaws.services.dynamodbv2.replication.RegionReplicationWorker;
import com.amazonaws.services.dynamodbv2.replication.ReplicationCheckpointUtil;
import com.amazonaws.services.dynamodbv2.replication.ShardSubscriber;
import com.amazonaws.services.dynamodbv2.replication.impl.ShardSubscriberImpl;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.model.Record;

/**
 * {@link ShardSubscriber} used for region catchup. The CatchupShardSubscriber gets changes from a shard in a given
 * SequenceNumber range and apply these changes to the destination region.
 */
public class CatchupShardSubscriber extends ShardSubscriberImpl {
    /**
     * The map of shardId to the right end of the applied range.
     */
    private final Map<String, String> endSenquenceNumbers;
    /**
     * True iff this ShardSubscriber finished all records in the applied range for this shard.
     */
    private boolean isCompleted;
    /**
     * The map of shardId to the left end of the applied range.
     */
    private final Map<String, String> startSequenceNumbers;

    /**
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
     * @param startSequenceNumbers
     *            The shard sequence number map from which to start catching up
     * @param endSequenceNumbers
     *            The shard sequence number map until which to stop catching up
     */
    public CatchupShardSubscriber(final String tableName,
        final MultiRegionCheckpointFactory multiRegionCheckpointFactory,
        final RegionReplicationWorker replicationWorker, final long timeBetweenSweeps,
        final long checkpointBackoffTime, final Map<String, String> startSequenceNumbers,
        final Map<String, String> endSequenceNumbers) {
        super(tableName, multiRegionCheckpointFactory, replicationWorker, timeBetweenSweeps, checkpointBackoffTime);
        this.startSequenceNumbers = startSequenceNumbers;
        endSenquenceNumbers = endSequenceNumbers;
        setCompleted(false);
    }

    /**
     * @return True iff this ShardSubscriber finished all records in the applied range for this shard
     */
    public boolean isCompleted() {
        return isCompleted;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(final List<Record> records, final IRecordProcessorCheckpointer checkpointer) {
        setCheckpointer(checkpointer);
        for (final Record record : records) {
            if (ReplicationCheckpointUtil.compareSequenceNumbers(record.getSequenceNumber(),
                startSequenceNumbers.get(getShardId())) > 0) {
                if (ReplicationCheckpointUtil.compareSequenceNumbers(record.getSequenceNumber(),
                    endSenquenceNumbers.get(getShardId())) <= 0) {
                    processSingleRecord(record);
                } else {
                    ((LocalReplicationCatchupWorker) getReplicationWorker()).markCompletedShard(getTableName(),
                        getShardId());
                }
            }
        }

    }

    /**
     * @param completed
     *            True iff this ShardSubscriber finished all records in the applied range for this shard
     */
    public void setCompleted(final boolean completed) {
        isCompleted = completed;
    }
}
