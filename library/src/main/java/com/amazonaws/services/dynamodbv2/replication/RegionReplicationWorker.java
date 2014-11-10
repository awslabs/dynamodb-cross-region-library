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

import com.amazonaws.services.dynamodbv2.model.Record;

/**
 * The interface for managing replication on a single region.
 */
public interface RegionReplicationWorker {
    /**
     * Calls ack on the {@link ShardSubscriber} identified by sourceTable and subscriberID with the parameters 
     * sequenceNumber, appliedRegion, and appliedRegion. 
     *
     * @param sequenceNumber
     *            The unique identifier for the update within a shard
     * @param appliedRegion
     *            The region where the update has been applied
     * @param appliedRegion
     *            The table where the update has been applied
     * @param sourceTable
     *            The table so send the ack
     * @param subscriberID
     *            The unique identifier for the Subscriber
     */
    void ack(String sequenceNumber, String appliedRegion, String appliedTable, String sourceTable, String subscriberID);

    /**
     * Calls the applier to apply the update on the region.
     *
     * @param updateRecord
     *            The update to distribute
     * @param sourceRegion
     *            The region to receive an ack upon successful processing of the update
     * @param sourceTable
     *            The table to receive an ack upon successful processing of the update
     * @param shardSubscriberID
     *            The subscriber to receive an ack upon successful processing of the update
     */
    void applyUpdateRecord(Record updateRecord, String sourceRegion, String sourceTable, String shardSubscriberID);

    /**
     * Gets the {@link ApplierProxy}.
     *
     * @return The {@link ApplierProxy}
     */
    ApplierProxy getApplierProxy();

    /**
     * Gets latest checkpoints from all shards.
     *
     * @param table
     *            The table name
     * @return The replication checkpoint
     */
    ReplicationCheckpoint getLatestCheckpoint(String table);

    /**
     * Gets the region name.
     *
     * @return The region name
     */
    String getRegionName();

    /**
     * Gets the replication configuration.
     *
     * @return The replication configuration
     */
    ReplicationConfiguration getReplicationConfiguration();

    /** Gets the replicationPolicy.
     * @return the replicationPolicy
     */
    ReplicationPolicy getReplicationPolicy();

    /**
     * Gets the {@link ShardSubscriberProxy}.
     *
     * @return The {@link ShardSubscriberProxy}
     */
    ShardSubscriberProxy getShardSubscriberProxy();

    /**
     * @return True if the region is a master region
     */
    boolean isMasterRegion();

    /**
     * Registers a {@link ShardSubscriber} with the RegionReplicationWorker so an {@link Applier} can ack to it.
     *
     * @param subscriber
     *            The {@link ShardSubscriber} to register
     */
    void registerShardSubscriber(ShardSubscriber subscriber);

    /**
     * Shuts down all streams.
     */
    void shutdownAllStreams();

    /**
     * Shuts down a stream for a given table.
     *
     * @param table
     *            The table associated with the stream to be shutdown
     */
    void shutdownSingleStream(String table);

    /**
     * Starts all Streams to listen for updates on the region.
     */
    void startAllStreams();

    /**
     * Starts a stream to listen for updates on a given table.
     *
     * @param table
     *            The table associated with the stream to be started
     */
    void startSingleStream(String table);

    /**
     * Unregisters a {@link ShardSubscriber} with the RegionReplicationWorker. {@link Applier}s will no longer be able
     * to ack to it.
     *
     * @param subscriber
     *            The {@link ShardSubscriber} to unregister
     */
    void unregisterShardSubscriber(ShardSubscriber subscriber);
}
