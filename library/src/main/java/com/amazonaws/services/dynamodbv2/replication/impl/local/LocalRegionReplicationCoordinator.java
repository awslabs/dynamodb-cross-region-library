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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.replication.ApplierProxy;
import com.amazonaws.services.dynamodbv2.replication.RegionConfiguration;
import com.amazonaws.services.dynamodbv2.replication.RegionReplicationCoordinator;
import com.amazonaws.services.dynamodbv2.replication.RegionReplicationWorker;
import com.amazonaws.services.dynamodbv2.replication.ReplicationCheckpoint;
import com.amazonaws.services.dynamodbv2.replication.ReplicationConfiguration;
import com.amazonaws.services.dynamodbv2.replication.ReplicationPolicy;
import com.amazonaws.services.dynamodbv2.replication.ShardSubscriberProxy;
import com.amazonaws.services.dynamodbv2.replication.TableConfiguration;
import com.amazonaws.services.dynamodbv2.replication.TableConfiguration.ReplicationTableStatus;
import com.amazonaws.services.dynamodbv2.replication.catchup.CatchupRegionConfiguration;
import com.amazonaws.services.dynamodbv2.replication.catchup.LocalReplicationCatchupCoordinator;
import com.amazonaws.services.dynamodbv2.replication.impl.ReplicationConfigurationImpl;
import com.amazonaws.services.dynamodbv2.replication.impl.TableConfigurationImpl;

/**
 * {@link RegionReplicationCoordinator} running on local JVM.
 */
public class LocalRegionReplicationCoordinator implements RegionReplicationCoordinator {
    /**
     * Logger for LocalRegionReplicationWorker.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalRegionReplicationCoordinator.class);

    /**
     * The number of threads for doing parallel scan in bootstraping.
     */
    private static final int NUMBER_THREADS = 4;
    /**
     * The applier proxy for subscribers to send apply requests to appliers.
     */
    private final ApplierProxy applierProxy;
    /**
     * The configuration for the replication group managed by the coordinator.
     */
    private final ReplicationConfiguration configuration;
    /**
     * The policy for resolving conflict updates.
     */
    private final ReplicationPolicy replicationPolicy;
    /**
     * Mapping each region to a {@link RegionReplicationWorker}.
     */
    private final HashMap<String, RegionReplicationWorker> replicationWorkers;
    /**
     * The subscriber proxy for appliers to send ack for updates to subscribers.
     */
    private final ShardSubscriberProxy subscriberProxy;
    /**
     * The executor service for starting and stopping replication.
     */
    private final ExecutorService executor;

    /**
     * The configurations of regions including tables in bootstraping state.
     */
    private final HashMap<String, RegionConfiguration> pendingRegions;

    /**
     * The status of replication coordinator.
     */
    private ReplicationCoordinatorStatus replicationCoordinatorStatus;

    /**
     * The random generator.
     */
    private static Random randomGenerator = new Random();

    /**
     * Constructs a local implementation of {@link RegionReplicationCoordinator}. Also creates
     * {@link RegionReplicationWorker}s it managed from a given {@link ReplicationConfiguration}.
     *
     * @param configuration
     *            The replication configuration for the replication group managed by the coordinator
     * @param replicationPolicy
     *            The policy for resolving conflict updates
     * @param subscriberProxy
     *            The subscriber proxy for appliers to send ack for updates to subscribers
     * @param applierProxy
     *            The applier proxy for subscribers to send apply requests to appliers
     */
    public LocalRegionReplicationCoordinator(final ReplicationConfiguration configuration,
        final ReplicationPolicy replicationPolicy, final ShardSubscriberProxy subscriberProxy,
        final ApplierProxy applierProxy) {
        this.configuration = configuration;
        this.replicationPolicy = replicationPolicy;
        this.applierProxy = applierProxy;
        this.subscriberProxy = subscriberProxy;
        replicationWorkers = new HashMap<String, RegionReplicationWorker>();
        pendingRegions = new HashMap<String, RegionConfiguration>();
        for (final String region : configuration.getRegions()) {
            replicationWorkers.put(region, new LocalRegionReplicationWorker(region, configuration, replicationPolicy,
                subscriberProxy, applierProxy));
        }
        executor = Executors.newFixedThreadPool(NUMBER_THREADS);
        replicationCoordinatorStatus = ReplicationCoordinatorStatus.STOPPED;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addRegion(final String newRegion, final Set<String> regionTables, final String cloudWatchEndpoint,
        final String dynamoDBEndpoint, final String streamsEndpoint,
        final AWSCredentialsProvider cloudWatchCredentialsProvider,
        final AWSCredentialsProvider dynamoDBCredentialsProvider,
        final AWSCredentialsProvider streamsCredentialsProvider) {
        return addRegion(newRegion, regionTables, cloudWatchEndpoint, dynamoDBEndpoint, streamsEndpoint,
            cloudWatchCredentialsProvider, dynamoDBCredentialsProvider, streamsCredentialsProvider, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addRegion(final String newRegion, final Set<String> regionTables, final String cloudWatchEndpoint,
        final String dynamoDBEndpoint, final String streamsEndpoint,
        final AWSCredentialsProvider cloudWatchCredentialsProvider,
        final AWSCredentialsProvider dynamoDBCredentialsProvider,
        final AWSCredentialsProvider streamsCredentialsProvider, final String checkpoint) {
        if (regionTables.size() == 0) {
            return false;
        }

        final RegionConfiguration regionConfig = configuration.getRegionConfiguration(newRegion);
        if (regionConfig != null) {
            for (final String table : configuration.getTables(newRegion)) {
                if (regionTables.contains(table)) {
                    LOGGER.warn("Region " + newRegion + " and table " + table + " was already in the configuration");
                    return false;
                }
            }
        }

        // If the first region, no bootstrapping needed.
        if (replicationCoordinatorStatus == ReplicationCoordinatorStatus.STOPPED) {
            boolean retVal = false;
            if (regionConfig == null) {
                retVal = configuration.addRegion(newRegion, regionTables, cloudWatchEndpoint, dynamoDBEndpoint,
                    streamsEndpoint, cloudWatchCredentialsProvider, dynamoDBCredentialsProvider,
                    streamsCredentialsProvider);
            } else {
                for (final String table : regionTables) {
                    if (regionConfig.addTable(table)) {
                        retVal = true;
                    }
                }
            }
            if (retVal) {
                replicationWorkers.put(newRegion, new LocalRegionReplicationWorker(newRegion, configuration,
                    replicationPolicy, subscriberProxy, applierProxy));
                return retVal;

            }
        }

        // Select source region and source table.
        String sourceRegion = null;
        String sourceTable = null;
        synchronized (configuration) {
            while (sourceTable == null) {
                sourceRegion = selectSourceRegion();
                sourceTable = selectSourceTable(sourceRegion);
            }
        }

        // Set up replication configuration for adding new region.
        final ReplicationConfiguration catchupReplicationConfiguration = new ReplicationConfigurationImpl();
        final RegionConfiguration sourceRegionConfig = new CatchupRegionConfiguration(
            configuration.getRegionConfiguration(sourceRegion));
        sourceRegionConfig.addTable(sourceTable);
        catchupReplicationConfiguration.addRegionConfiguration(sourceRegionConfig);
        final RegionConfiguration destRegionConfig;
        if (sourceRegion.equals(newRegion)) {
            destRegionConfig = sourceRegionConfig;
        } else {
            destRegionConfig = new CatchupRegionConfiguration(newRegion, cloudWatchEndpoint, dynamoDBEndpoint,
                streamsEndpoint, cloudWatchCredentialsProvider, dynamoDBCredentialsProvider,
                streamsCredentialsProvider);
        }

        for (final String table : regionTables) {
            destRegionConfig.addTable(table);
            destRegionConfig.setTableStatus(table, ReplicationTableStatus.BOOTSTRAPPING);
        }
        catchupReplicationConfiguration.addRegionConfiguration(destRegionConfig);

        // Start the boostraping & catchup process
        final LocalReplicationCatchupCoordinator catchupCoordinator = new LocalReplicationCatchupCoordinator(
            sourceRegion, sourceTable, catchupReplicationConfiguration, replicationPolicy);
        final AddRegionsTask addRegionsTask = new AddRegionsTask(this, catchupCoordinator, checkpoint);
        executor.execute(addRegionsTask);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ApplierProxy getApplierProxy() {
        return applierProxy;
    }

    /**
     * @return the pendingRegions
     */
    public HashMap<String, RegionConfiguration> getPendingRegions() {
        return pendingRegions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReplicationConfiguration getReplicationConfiguration() {
        return configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReplicationPolicy getReplicationPolicy() {
        return replicationPolicy;
    }

    /**
     * @param region
     *            The target region
     * @return the RegionReplicationWorker for a given region
     */
    public RegionReplicationWorker getReplicationWorker(final String region) {
        return replicationWorkers.get(region);
    }

    /**
     * @return the replicationWorkers
     */
    public HashMap<String, RegionReplicationWorker> getReplicationWorkers() {
        return replicationWorkers;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized ReplicationCoordinatorStatus getReplicationCoordinatorStatus() {
        return replicationCoordinatorStatus;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ShardSubscriberProxy getShardSubsriberProxy() {
        return subscriberProxy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableConfiguration getTableConfiguration(final String region, final String table) {
        // Find the table config the replication configuration first.
        RegionConfiguration regionConfig = configuration.getRegionConfiguration(region);
        if (regionConfig != null) {
            final TableConfiguration tableConfig = regionConfig.getTableConfiguration(table);
            if (tableConfig != null) {
                return tableConfig;
            }
        }
        // Then find in pending region config
        regionConfig = pendingRegions.get(region);
        if (regionConfig != null) {
            final TableConfiguration tableConfig = regionConfig.getTableConfiguration(table);
            if (tableConfig != null) {
                return tableConfig;
            }
        }
        return TableConfigurationImpl.buildNonexistent(region, table);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, ReplicationCheckpoint> removeRegion(final String regionName) {
        Map<String, ReplicationCheckpoint> retVal = null;
        if (configuration.getRegionConfiguration(regionName) == null) {
            return null;
        }

        synchronized (configuration) {
            if (replicationCoordinatorStatus == ReplicationCoordinatorStatus.STOPPED) {
                return removeRegionHelper(regionName);
            }
            stopRegionReplicationWorkers();
            retVal = removeRegionHelper(regionName);
            startRegionReplicationWorkers();
        }
        return retVal;
    }

    /**
     * Helper methods for removeRegion.
     *
     * @param regionName
     *            The region to remove
     * @return The {@link ReplicationCheckpoint}s of all tables in the region
     */
    private Map<String, ReplicationCheckpoint> removeRegionHelper(final String regionName) {
        final HashMap<String, ReplicationCheckpoint> retVal = new HashMap<String, ReplicationCheckpoint>();
        for (final String table : configuration.getTables(regionName)) {
            retVal.put(table, replicationWorkers.get(regionName).getLatestCheckpoint(table));
        }
        configuration.removeRegion(regionName);
        applierProxy.unregister(regionName);
        subscriberProxy.unregister(regionName);
        replicationWorkers.remove(regionName);
        for (final String region : replicationWorkers.keySet()) {
            ((LocalRegionReplicationWorker) replicationWorkers.get(region)).resetConfiguration(configuration);
        }
        return retVal;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReplicationCheckpoint removeTable(final String regionName, final String tableName) {
        ReplicationCheckpoint retVal = null;
        if (configuration.getRegionConfiguration(regionName) == null) {
            return null;
        }

        synchronized (configuration) {
            if (replicationCoordinatorStatus == ReplicationCoordinatorStatus.STOPPED) {
                return removeTableHelper(regionName, tableName);
            }
            stopRegionReplicationWorkers();
            retVal = removeTableHelper(regionName, tableName);
            startRegionReplicationWorkers();
        }
        return retVal;
    }

    /**
     * Helper methods for removeTable.
     *
     * @param regionName
     *            The region containing the table to remove
     * @param tableName
     *            The table to remove
     * @return The {@link ReplicationCheckpoint} of the table
     */
    private ReplicationCheckpoint removeTableHelper(final String regionName, final String tableName) {
        final ReplicationCheckpoint retVal;
        retVal = replicationWorkers.get(regionName).getLatestCheckpoint(tableName);
        configuration.getRegionConfiguration(regionName).removeTable(tableName);
        for (final String region : replicationWorkers.keySet()) {
            ((LocalRegionReplicationWorker) replicationWorkers.get(region)).resetConfiguration(configuration);
        }
        return retVal;
    }

    /**
     * Helper method for addRegion(). Selects a region to copy data/updates.
     *
     * @return The region to copy
     */
    private String selectSourceRegion() {
        // Simple implementation, randomly pick up a region among all regions in the configuration.
        final int position = randomGenerator.nextInt(configuration.getRegions().size());
        int i = 0;
        for (final String region : configuration.getRegions()) {
            if (i == position) {
                return region;
            }
            i++;
        }
        return null;
    }

    /**
     * Helper method for addRegion(). Selects source a table from a given region to copy data/updates.
     *
     * @param sourceRegion
     *            The source region
     * @return The table to copy
     */
    private String selectSourceTable(final String sourceRegion) {
        // Simple implementation, randomly pick up a table among all tables in the sourceRegion in the configuration.
        final int position = randomGenerator.nextInt(configuration.getTables(sourceRegion).size());
        int i = 0;
        for (final String table : configuration.getTables(sourceRegion)) {
            if (i == position) {
                return table;
            }
            i++;
        }
        return null;
    }

    /**
     * For testing.
     *
     * @param status
     *            The status to set
     */
    protected void setReplicationCoordinatorStatus(final ReplicationCoordinatorStatus status) {
        replicationCoordinatorStatus = status;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void startRegionReplicationWorkers() {
        for (final String region : replicationWorkers.keySet()) {
            replicationWorkers.get(region).startAllStreams();
            LOGGER.debug("Started replication in region: " + region);
        }
        replicationCoordinatorStatus = ReplicationCoordinatorStatus.RUNNING;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void stopRegionReplicationWorkers() {
        for (final String region : replicationWorkers.keySet()) {
            replicationWorkers.get(region).shutdownAllStreams();
        }
        if (!replicationWorkers.isEmpty()) {
            replicationCoordinatorStatus = ReplicationCoordinatorStatus.STOPPED;
        }
    }

}
