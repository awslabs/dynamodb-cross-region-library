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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.replication.ApplierProxy;
import com.amazonaws.services.dynamodbv2.replication.ReplicationCheckpoint;
import com.amazonaws.services.dynamodbv2.replication.ReplicationConfiguration;
import com.amazonaws.services.dynamodbv2.replication.ReplicationPolicy;
import com.amazonaws.services.dynamodbv2.replication.ShardSubscriberProxy;
import com.amazonaws.services.dynamodbv2.replication.impl.local.LocalShardSubscriberProxy;
import com.amazonaws.services.dynamodbv2.replication.impl.local.LocalTableApplierProxy;

/**
 * Local implementation of {@link ReplicationCatchupCoordinator}.
 */
public class LocalReplicationCatchupCoordinator implements ReplicationCatchupCoordinator {
    /**
     * Default number of threads for parallel scans.
     */
    private static final int DEFAULT_NUMBER_OF_SCAN_THREADS = 10;
    /**
     * Time in second for an executor service to wait for its termination.
     */
    private static final int TERMINATION_WAIT_TIME_IN_SECONDS = 7200;
    /**
     * Time in millisecond for the coordinator to wait for catchup workers to finish.
     */
    private static final long CATCHUP_WAIT_TIME_IN_MILLIS = 10000L;
    /**
     * Logger for {@link LocalReplicationCatchupCoordinator}.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalReplicationCatchupCoordinator.class.getName());

    /**
     * Number of threads (= total number of segments) for parallel scans.
     */
    private final int numberOfScanThreads;
    /**
     * The configuration for the catchup region group.
     */
    private final ReplicationConfiguration replicationConfiguration;

    /**
     * The policy for resolving conflict updates.
     */
    private final ReplicationPolicy replicationPolicy;

    /**
     * The source region to get the changes.
     */
    private final String sourceRegion;

    /**
     * The source table to get the changes.
     */
    private final String sourceTable;

    /**
     * Constructor.
     *
     * @param sourceRegion
     *            The source region to get the changes
     * @param sourceTable
     *            The source table to get the changes
     * @param replicationConfiguration
     *            The configuration for the catchup region group
     * @param replicationPolicy
     *            The policy for resolving conflict updates
     */
    public LocalReplicationCatchupCoordinator(final String sourceRegion, final String sourceTable,
        final ReplicationConfiguration replicationConfiguration, final ReplicationPolicy replicationPolicy) {
        this(sourceRegion, sourceTable, replicationConfiguration, replicationPolicy, DEFAULT_NUMBER_OF_SCAN_THREADS);
    }
    /**
     * Constructor.
     *
     * @param sourceRegion
     *            The source region to get the changes
     * @param sourceTable
     *            The source table to get the changes
     * @param replicationConfiguration
     *            The configuration for the catchup region group
     * @param replicationPolicy
     *            The policy for resolving conflict updates
     * @param numberOfScanThreads
     *            Number of threads (= total number of segments) for parallel scans
     */
    public LocalReplicationCatchupCoordinator(final String sourceRegion, final String sourceTable,
        final ReplicationConfiguration replicationConfiguration, final ReplicationPolicy replicationPolicy,
        final int numberOfScanThreads) {
        this.sourceRegion = sourceRegion;
        this.sourceTable = sourceTable;
        this.replicationConfiguration = replicationConfiguration;
        this.replicationPolicy = replicationPolicy;
        this.numberOfScanThreads = numberOfScanThreads;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void catchUp(final ReplicationCheckpoint fromCheckpoint, final ReplicationCheckpoint toCheckpoint) {
        final ShardSubscriberProxy subscriberProxy = new LocalShardSubscriberProxy();
        final ApplierProxy applierProxy = new LocalTableApplierProxy();
        final LocalReplicationCatchupWorker sourceRegionWorker = new LocalReplicationCatchupWorker(sourceRegion,
            replicationConfiguration, replicationPolicy, subscriberProxy, applierProxy, fromCheckpoint, toCheckpoint);
        for (final String destRegion : replicationConfiguration.getRegions()) {
            if (!destRegion.equals(sourceRegion)) {
                final LocalReplicationCatchupWorker destRegionWorker = new LocalReplicationCatchupWorker(destRegion,
                    replicationConfiguration, replicationPolicy, subscriberProxy, applierProxy, fromCheckpoint,
                    toCheckpoint);
                assert !destRegionWorker.isMasterRegion();
            }
        }

        sourceRegionWorker.startAllStreams();
        // wait for completion
        while (!sourceRegionWorker.isDone()) {
            try {
                Thread.sleep(CATCHUP_WAIT_TIME_IN_MILLIS);
            } catch (final InterruptedException e) {
                LOGGER.warn("LocalReplicationCatchupCoordinator has been interrupted", e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyTable(final ReplicationCheckpoint replicationCheckpoint) {
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfScanThreads);
        final int totalSegments = numberOfScanThreads;

        for (int segment = 0; segment < totalSegments; segment++) {
            final ReplicationScanSegmentTask task = new ReplicationScanSegmentTask(sourceRegion, sourceTable,
                replicationConfiguration, totalSegments, segment);
            executor.execute(task);
        }
        shutDownExecutorService(executor);
    }

    /**
     * @return the replicationConfiguration
     */
    public ReplicationConfiguration getReplicationConfiguration() {
        return replicationConfiguration;
    }

    /**
     * @return the sourceRegion
     */
    public String getSourceRegion() {
        return sourceRegion;
    }

    /**
     * @return the sourceTable
     */
    public String getSourceTable() {
        return sourceTable;
    }

    /**
     * Shutdowns the executor service used for parallel scans.
     *
     * @param executor
     *            The executor service
     */
    private void shutDownExecutorService(final ExecutorService executor) {
        executor.shutdown();
        try {
            executor.awaitTermination(TERMINATION_WAIT_TIME_IN_SECONDS, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            executor.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}
