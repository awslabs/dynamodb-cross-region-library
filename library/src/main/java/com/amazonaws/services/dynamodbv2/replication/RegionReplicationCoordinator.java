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

import java.util.Map;
import java.util.Set;

import com.amazonaws.auth.AWSCredentialsProvider;

/**
 * The interface for managing replication on a group of regions. It is responsible for adding/removing a region
 * into/from the group and starting/stopping replication on the group.
 */
public interface RegionReplicationCoordinator {
    /**
     * Status of the coordinator.
     */
    public enum ReplicationCoordinatorStatus {
        /**
         * The coordinator is not running.
         */
        STOPPED,
        /**
         * The coordinator is running.
         */
        RUNNING
    }

    /**
     * Adds a new region to the replication group managed by the coordinator.
     *
     * @param newRegion
     *            The name of the new region
     * @param regionTables
     *            List of tables (table replicas) in the region
     * @param cloudWatchEndpoint
     *            The endpoint of CloudWatch in the new region
     * @param dynamoDBEndpoint
     *            The endpoint of DynamoDB tables in the new region
     * @param streamsEndpoint
     *            The endpoint of DynamoDB Streams in the new region
     * @param cloudWatchCredentialsProvider
     *            The CloudWatch credentials for the new region
     * @param dynamoDBCredentialsProvider
     *            The DynamoDB credentials for the new region
     * @param streamsCredentialsProvider
     *            The Streams credentials for the new region
     * @return True iff the region is successfully added
     */
    boolean addRegion(String newRegion, final Set<String> regionTables, final String cloudWatchEndpoint,
        final String dynamoDBEndpoint, final String streamsEndpoint,
        final AWSCredentialsProvider cloudWatchCredentialsProvider,
        final AWSCredentialsProvider dynamoDBCredentialsProvider,
        final AWSCredentialsProvider streamsCredentialsProvider);

    /**
     * Adds a new region to the replication group managed by the coordinator.
     *
     * @param newRegion
     *            The name of the new region
     * @param regionTables
     *            List of tables (table replicas) in the region
     * @param cloudWatchEndpoint
     *            The endpoint of CloudWatch in the new region
     * @param dynamoDBEndpoint
     *            The endpoint of DynamoDB tables in the new region
     * @param streamsEndpoint
     *            The endpoint of DynamoDB Streams in the new region
     * @param cloudWatchCredentialsProvider
     *            The CloudWatch credentials for the new region
     * @param dynamoDBCredentialsProvider
     *            The DynamoDB credentials for the region
     * @param streamsCredentialsProvider
     *            The Streams credentials for the region
     * @param startingSequenceNumber
     *            The checkpoint from which the region gets the new updates from the replication group. If NULL, the
     *            whole table is copied into the new region
     * @return True iff the region is successfully added
     */
    boolean addRegion(String newRegion, final Set<String> regionTables, final String cloudWatchEndpoint,
        final String dynamoDBEndpoint, final String streamsEndpoint,
        final AWSCredentialsProvider cloudWatchCredentialsProvider,
        final AWSCredentialsProvider dynamoDBCredentialsProvider,
        final AWSCredentialsProvider streamsCredentialsProvider, final String startingSequenceNumber);

    /**
     * Gets the applier proxy managed by the coordinator.
     *
     * @return The applier proxy {@link ApplierProxy}
     */
    ApplierProxy getApplierProxy();

    /**
     * Gets the replication configuration.
     *
     * @return The replication configuration {@link ReplicationConfiguration}
     */
    ReplicationConfiguration getReplicationConfiguration();

    /**
     * Gets the replication policy.
     *
     * @return The replication policy {@link ReplicationPolicy}
     */
    ReplicationPolicy getReplicationPolicy();

    /**
     * Gets the status of replication workers.
     *
     * @return The status of replication workers
     */
    ReplicationCoordinatorStatus getReplicationCoordinatorStatus();

    /**
     * Gets the shard subscriber proxy managed by the coordinator.
     *
     * @return The shard subscriber proxy {@link ShardSubscriberProxy}
     */
    ShardSubscriberProxy getShardSubsriberProxy();

    /**
     * Gets the configuration of a given table in a given region.
     *
     * @param region
     *            The target region
     * @param table
     *            The table to check
     * @return The configuration of the table
     */
    TableConfiguration getTableConfiguration(final String region, final String table);

    /**
     * Removes a region from the replication group managed by the coordinator.
     *
     * @param regionName
     *            Name of the region to remove
     * @return Checkpoints for each table of the region before removing
     */
    Map<String, ReplicationCheckpoint> removeRegion(String regionName);

    /**
     * Removes a table of a given region from the replication group managed by the coordinator.
     *
     * @param regionName
     *            Name of the region where the table resides
     * @param tableName
     *            Name of the table to remove
     * @return Checkpoint for the table of the region before removing
     */
    ReplicationCheckpoint removeTable(String regionName, String tableName);

    /**
     * Starts replication on all regions in the replication group managed by the coordinator.
     */
    void startRegionReplicationWorkers();

    /**
     * Stops replication on all regions in the replication group managed by the coordinator.
     */
    void stopRegionReplicationWorkers();
}
