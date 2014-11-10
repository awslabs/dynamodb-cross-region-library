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

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import com.amazonaws.services.dynamodbv2.replication.ApplierProxy;
import com.amazonaws.services.dynamodbv2.replication.RegionConfiguration;
import com.amazonaws.services.dynamodbv2.replication.ReplicationCheckpoint;
import com.amazonaws.services.dynamodbv2.replication.ReplicationConfiguration;
import com.amazonaws.services.dynamodbv2.replication.ReplicationPolicy;
import com.amazonaws.services.dynamodbv2.replication.ShardSubscriber;
import com.amazonaws.services.dynamodbv2.replication.ShardSubscriberProxy;
import com.amazonaws.services.dynamodbv2.replication.TableConfiguration;
import com.amazonaws.services.dynamodbv2.replication.impl.local.LocalRegionReplicationWorker;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

/**
 * A sub class of {@link LocalRegionReplicationWorker} to handle region catchup.
 *
 */
public class LocalReplicationCatchupWorker extends LocalRegionReplicationWorker {
    /**
     * Count of complete shards for each stream (associated with a table).
     */
    private final HashMap<String, Integer> completeShardCounts;
    /**
     * Count of completed streams.
     */
    private int completedStreamCount;
    /**
     * The {@link ReplicationCheckpoint} to start catching up.
     */
    private final ReplicationCheckpoint fromCheckpoint;
    /**
     * The {@link ReplicationCheckpoint} to stop catching up.
     */
    private final ReplicationCheckpoint toCheckpoint;

    /**
     * Constructor.
     *
     * @param region
     *            The region that this replication worker manages.
     * @param configuration
     *            The configuration for the replication group.
     * @param replicationPolicy
     *            The policy for resolving conflict updates.
     * @param shardSubscriberProxy
     *            The subscriber proxy for appliers to send ack for updates to subscribers.
     * @param applierProxy
     *            The applier proxy for subscribers to send apply requests to appliers. * @param fromCheckpoint The
     *            {@link ReplicationCheckpoint} to start catching up
     * @param toCheckpoint
     *            The {@link ReplicationCheckpoint} to stop catching up
     */
    public LocalReplicationCatchupWorker(final String region, final ReplicationConfiguration configuration,
        final ReplicationPolicy replicationPolicy, final ShardSubscriberProxy shardSubscriberProxy,
        final ApplierProxy applierProxy, final ReplicationCheckpoint fromCheckpoint,
        final ReplicationCheckpoint toCheckpoint) {
        super(region, configuration, replicationPolicy, shardSubscriberProxy, applierProxy);
        this.fromCheckpoint = fromCheckpoint;
        this.toCheckpoint = toCheckpoint;
        completeShardCounts = new HashMap<String, Integer>();
        for (final String table : configuration.getTables(region)) {
            completeShardCounts.put(table, 0);
        }
        completedStreamCount = 0;
    }

    /**
     * Initializes the worker.
     */
    private void initializeStreams() {
        resetAppliers();

        final RegionConfiguration regionConfig = getReplicationConfiguration().getRegionConfiguration(getRegionName());
        getSubscribers().clear();
        for (final String table : getReplicationConfiguration().getTables(getRegionName())) {
            if (regionConfig.getTableConfiguration(table).hasStreams()) {
                completeShardCounts.put(table, 0);
                getSubscribers().put(table, new ConcurrentHashMap<String, ShardSubscriber>());
                final TableConfiguration tableConfiguration = regionConfig.getTableConfiguration(table);
                assert tableConfiguration != null;
                getStreamsWorkers().put(
                    table,
                    new Worker(new CatchupShardSubscriberFactory(table, this, fromCheckpoint, toCheckpoint),
                        tableConfiguration.getKinesisClientLibConfiguration(), tableConfiguration.getKinesisClient(),
                        tableConfiguration.getDynamoDBClient(), tableConfiguration.getCloudWatchClient()));
            }
        }
        if (isMasterRegion()) {
            getShardSubscriberProxy().register(this);
        }
    }

    /**
     * @return True iff this worker finished all work.
     */
    public synchronized boolean isDone() {
        if (completedStreamCount == completeShardCounts.size()) {
            return true;
        }
        return false;
    }

    /**
     * Marks a shard of a stream as completed.
     *
     * @param table
     *            The table associate with the stream
     * @param shardId
     *            The shard id
     */
    public synchronized void markCompletedShard(final String table, final String shardId) {
        if (getSubscribers().containsKey(table) && getSubscribers().get(table).containsKey(shardId)) {
            final CatchupShardSubscriber sharSubscriber = (CatchupShardSubscriber) getSubscribers().get(table).get(
                shardId);
            if (!sharSubscriber.isCompleted()) {
                sharSubscriber.setCompleted(true);
                int completeShardCount = completeShardCounts.get(table);
                completeShardCounts.put(table, ++completeShardCount);
                if (completeShardCount == getSubscribers().get(table).size()) {
                    shutdownSingleStream(table);
                    completedStreamCount++;
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void startAllStreams() {
        initializeStreams();
        for (final Worker worker : getStreamsWorkers().values()) {
            Executors.newSingleThreadExecutor().submit(worker);
        }
    }

}
