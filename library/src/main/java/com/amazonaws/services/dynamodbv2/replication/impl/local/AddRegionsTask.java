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
package com.amazonaws.services.dynamodbv2.replication.impl.local;

import com.amazonaws.services.dynamodbv2.replication.RegionConfiguration;
import com.amazonaws.services.dynamodbv2.replication.RegionReplicationWorker;
import com.amazonaws.services.dynamodbv2.replication.ReplicationCheckpoint;
import com.amazonaws.services.dynamodbv2.replication.ReplicationCheckpointUtil;
import com.amazonaws.services.dynamodbv2.replication.ReplicationConfiguration;
import com.amazonaws.services.dynamodbv2.replication.catchup.LocalReplicationCatchupCoordinator;

/**
 * The task for bootstrapping when adding a new table or region.
 */
public class AddRegionsTask implements Runnable {
    /**
     * The replication coordinator.
     */
    private final LocalRegionReplicationCoordinator replicationCoordinator;
    /**
     * The catchup coordinator.
     */
    private final LocalReplicationCatchupCoordinator catchupCoordinator;
    /**
     * The SequenceNumber to start doing catchup.
     */
    private final String fromSequenceNumber;

    /**
     *
     * @param replicationCoordinator
     *            The replication coordinator
     * @param catchupCoordinator
     *            The catchup coordinator
     * @param fromSequenceNumber
     *            The SequenceNumber to start doing catchup, if null start catching up from scratch.
     */
    public AddRegionsTask(final LocalRegionReplicationCoordinator replicationCoordinator,
        final LocalReplicationCatchupCoordinator catchupCoordinator, final String fromSequenceNumber) {
        this.replicationCoordinator = replicationCoordinator;
        this.catchupCoordinator = catchupCoordinator;
        this.fromSequenceNumber = fromSequenceNumber;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        final String sourceRegion = catchupCoordinator.getSourceRegion();
        final String sourceTable = catchupCoordinator.getSourceTable();
        final ReplicationConfiguration replicationConfiguration = replicationCoordinator.getReplicationConfiguration();
        final ReplicationConfiguration catchupConfiguration = catchupCoordinator.getReplicationConfiguration();
        final RegionReplicationWorker sourceRegionWorker = replicationCoordinator.getReplicationWorker(sourceRegion);
        // Add catchup regions into the pending region configurations so that we can check for status of their tables.
        for (final String region : catchupConfiguration.getRegions()) {
            replicationCoordinator.getPendingRegions().put(region, catchupConfiguration.getRegionConfiguration(region));

        }
        ReplicationCheckpoint checkpoint1 = sourceRegionWorker.getLatestCheckpoint(sourceTable);
        if (fromSequenceNumber == null) {
            // Copy table from scatch.
            catchupCoordinator.copyTable(checkpoint1);
        } else {
            checkpoint1 = ReplicationCheckpointUtil.sequenceNumberToReplicationCheckpoint(fromSequenceNumber,
                checkpoint1);
        }

        final ReplicationCheckpoint checkpoint2 = sourceRegionWorker.getLatestCheckpoint(sourceTable);
        // Long catchup.
        if (!checkpoint2.equals(checkpoint1)) {
            catchupCoordinator.catchUp(checkpoint1, checkpoint2);
        }
        synchronized (replicationConfiguration) {
            replicationCoordinator.stopRegionReplicationWorkers();
            final ReplicationCheckpoint checkpoint3 = sourceRegionWorker.getLatestCheckpoint(sourceTable);
            // Short catchup.
            if (!checkpoint3.equals(checkpoint2)) {
                catchupCoordinator.catchUp(checkpoint2, checkpoint3);
            }
            // Update replication configuration and new replication workers.
            for (final String region : catchupConfiguration.getRegions()) {
                final RegionConfiguration regionConfig = replicationConfiguration.getRegionConfiguration(region);
                if (!region.equals(sourceRegion)) {
                    if (regionConfig == null) {
                        replicationConfiguration.addRegionConfiguration(catchupConfiguration
                            .getRegionConfiguration(region));
                    } else {
                        for (final String table : catchupConfiguration.getTables(region)) {
                            regionConfig.addTable(table);
                        }
                    }
                } else if (regionConfig != null) {
                    for (final String table : catchupConfiguration.getTables(region)) {
                        if (!table.equals(sourceTable)) {
                            regionConfig.addTable(table);
                        }
                    }
                }
                // Create new workers for new regions.
                if (regionConfig == null) {
                    final RegionReplicationWorker newWorker = new LocalRegionReplicationWorker(region,
                        replicationConfiguration, replicationCoordinator.getReplicationPolicy(),
                        replicationCoordinator.getShardSubsriberProxy(), replicationCoordinator.getApplierProxy());
                    replicationCoordinator.getReplicationWorkers().put(region, newWorker);
                }
                // Remove catchup regions from pending region configuration.
                replicationCoordinator.getPendingRegions().remove(region);
            }

            for (final String region : replicationCoordinator.getReplicationWorkers().keySet()) {
                ((LocalRegionReplicationWorker) replicationCoordinator.getReplicationWorker(region))
                    .resetConfiguration(replicationConfiguration);
            }
            replicationCoordinator.startRegionReplicationWorkers();
        }

    }
}
