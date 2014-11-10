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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.replication.Applier;
import com.amazonaws.services.dynamodbv2.replication.ApplierProxy;
import com.amazonaws.services.dynamodbv2.replication.RegionConfiguration;
import com.amazonaws.services.dynamodbv2.replication.RegionReplicationWorker;
import com.amazonaws.services.dynamodbv2.replication.ReplicationCheckpoint;
import com.amazonaws.services.dynamodbv2.replication.ReplicationConfiguration;
import com.amazonaws.services.dynamodbv2.replication.ReplicationPolicy;
import com.amazonaws.services.dynamodbv2.replication.ShardSubscriber;
import com.amazonaws.services.dynamodbv2.replication.ShardSubscriberProxy;
import com.amazonaws.services.dynamodbv2.replication.TableConfiguration;
import com.amazonaws.services.dynamodbv2.replication.TableConfiguration.ReplicationTableStatus;
import com.amazonaws.services.dynamodbv2.replication.impl.ApplierSingleUseRunner;
import com.amazonaws.services.dynamodbv2.replication.impl.ReplicationCheckpointImpl;
import com.amazonaws.services.dynamodbv2.replication.impl.ShardSubscriberImpl;
import com.amazonaws.services.dynamodbv2.replication.impl.ShardSubscriberImplFactory;
import com.amazonaws.services.dynamodbv2.replication.impl.TableApplierFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

/**
 * {@link RegionReplicationWorker} running on local JVM.
 */
public class LocalRegionReplicationWorker implements RegionReplicationWorker {
    /**
     * The status of the replication worker.
     */
    private enum LocalReplicationWorkerStatus {
        /**
         * The replication worker is neither initialized or running.
         */
        STOPPED,
        /**
         * The replication worker is initialized but not running.
         */
        INITIALIZED,
        /**
         * The replication worker is running.
         */
        RUNNING
    }

    /**
     * The time in millisecond to wait for all shard subscribers to shutdown.
     */
    private static final long SUBSCRIBER_SHUTDOWN_WAIT_TIME_MILLIS = 1000L;

    /**
     * Logger for LocalRegionReplicationWorker.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalRegionReplicationWorker.class);
    /**
     * The applier factory.
     */
    private final TableApplierFactory applierFactory;
    /**
     * The applier proxy for subscribers to send apply requests to appliers.
     */
    private final ApplierProxy applierProxy;
    /**
     * Lookup map for appliers. Key is the table name (a replica of a table in the region).
     */
    private final HashMap<String, Applier> appliers;
    /**
     * Thread for running applier tasks, one per table.
     */
    private final HashMap<String, ExecutorService> applierThreads;
    /**
     * The region that this replication worker manages.
     */
    private final String region;

    /**
     * The region configuration.
     */
    private RegionConfiguration regionConfiguration;
    /**
     * The configuration for the replication group.
     */
    private ReplicationConfiguration replicationConfiguration;
    /**
     * The subscriber proxy for appliers to send ack for updates to subscribers.
     */
    private final ShardSubscriberProxy subscriberProxy;

    /**
     * A lookup map for subscribers: (table -> (shardId -> ShardSubscriber)).
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ShardSubscriber>> subscribers;

    /**
     * The map from table name to StreamWorker.
     */
    private final HashMap<String, Worker> streamsWorkers;

    /**
     * The status of the replication worker.
     */
    private LocalReplicationWorkerStatus workerStatus;

    /**
     * The replication policy.
     */
    private final ReplicationPolicy replicationPolicy;

    /**
     * Constructs a local implementation of {@link RegionReplicationWorker}. Also sets up subscribers and the applier
     * for the region.
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
     *            The applier proxy for subscribers to send apply requests to appliers.
     */
    public LocalRegionReplicationWorker(final String region, final ReplicationConfiguration configuration,
        final ReplicationPolicy replicationPolicy, final ShardSubscriberProxy shardSubscriberProxy,
        final ApplierProxy applierProxy) {
        this.region = region;
        subscriberProxy = shardSubscriberProxy;
        this.applierProxy = applierProxy;
        replicationConfiguration = configuration;
        this.replicationPolicy = replicationPolicy;
        applierFactory = new TableApplierFactory(configuration, replicationPolicy, shardSubscriberProxy);
        subscribers = new ConcurrentHashMap<String, ConcurrentHashMap<String, ShardSubscriber>>();
        appliers = new HashMap<String, Applier>();
        streamsWorkers = new HashMap<String, Worker>();
        applierThreads = new HashMap<String, ExecutorService>();
        workerStatus = LocalReplicationWorkerStatus.STOPPED;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ack(final String sequenceNumber, final String appliedRegion, final String appliedTable,
        final String sourceTable, final String subscriberID) {
        if (!subscribers.containsKey(sourceTable)) {
            LOGGER.warn("Table: " + sourceTable + " is not existed in " + region);
            return;
        }
        final ShardSubscriber shardSubscriber = subscribers.get(sourceTable).get(subscriberID);
        if (shardSubscriber != null) {
            shardSubscriber.ack(sequenceNumber, appliedRegion, appliedTable);
        } else {
            LOGGER.warn("Subscriber is not registered: " + subscriberID + ". Could not ack for sequence number: "
                + sequenceNumber + " in " + region + " -> " + sourceTable);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void applyUpdateRecord(final Record updateRecord, final String sourceRegion, final String sourceTable,
        final String shardSubscriberID) {
        final Map<String, AttributeValue> key = updateRecord.getDynamodb().getKeys();
        final Map<String, AttributeValue> oldItem = updateRecord.getDynamodb().getOldImage();
        final Map<String, AttributeValue> newItem = updateRecord.getDynamodb().getNewImage();
        final String sequenceNumber = updateRecord.getDynamodb().getSequenceNumber();
        for (final String table : replicationConfiguration.getTables(region)) {
            if (!(sourceRegion.equals(region) && sourceTable.equals(table))) {
                final Applier applier = appliers.get(table);
                applierThreads.get(table).submit(
                    new ApplierSingleUseRunner(applier, key, oldItem, newItem, sequenceNumber, sourceRegion,
                        sourceTable, shardSubscriberID));
            } else {
                subscriberProxy.ack(sequenceNumber, region, table, sourceRegion, sourceTable, shardSubscriberID);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ApplierProxy getApplierProxy() {
        return applierProxy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReplicationCheckpoint getLatestCheckpoint(final String table) {
        if (!subscribers.containsKey(table)) {
            return null;
        }
        final HashMap<String, String> latestCheckpoints = new HashMap<String, String>();
        for (final ShardSubscriber subscriber : subscribers.get(table).values()) {
            latestCheckpoints.put(subscriber.getShardId(), subscriber.getLatestCheckpoint());
        }
        return new ReplicationCheckpointImpl(table, region, latestCheckpoints);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public String getRegionName() {
        return region;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReplicationConfiguration getReplicationConfiguration() {
        return replicationConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReplicationPolicy getReplicationPolicy() {
        return replicationPolicy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ShardSubscriberProxy getShardSubscriberProxy() {
        return subscriberProxy;
    }

    /**
     * @return the subscribers
     */
    public ConcurrentHashMap<String, ConcurrentHashMap<String, ShardSubscriber>> getSubscribers() {
        return subscribers;
    }

    /**
     * @return the streamsWorkers
     */
    public HashMap<String, Worker> getStreamsWorkers() {
        return streamsWorkers;
    }

    /**
     * Initializes the replication worker.
     */
    protected void initialize() {
        assert (workerStatus == LocalReplicationWorkerStatus.STOPPED);
        resetSubscribers();
        resetAppliers();
        workerStatus = LocalReplicationWorkerStatus.INITIALIZED;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isMasterRegion() {
        return replicationConfiguration.isMasterRegion(region);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void registerShardSubscriber(final ShardSubscriber subscriber) {
        final ShardSubscriberImpl shardSubscriberImpl = (ShardSubscriberImpl) subscriber;
        if (subscribers.containsKey(shardSubscriberImpl.getTableName())) {
            subscribers.get(shardSubscriberImpl.getTableName()).put(shardSubscriberImpl.getShardId(),
                shardSubscriberImpl);
        } else {
            LOGGER
                .warn("Table not existed: Cannot register subscriber for table " + shardSubscriberImpl.getTableName());
        }
    }

    /**
     * Resets all appliers (called after changing the replication configuration).
     */
    protected void resetAppliers() {
        applierThreads.clear();
        for (final String table : replicationConfiguration.getTables(region)) {
            applierThreads.put(table, Executors.newCachedThreadPool());
            appliers.put(table, applierFactory.createApplier(replicationConfiguration.getRegionConfiguration(region)
                .getTableConfiguration(table)));
        }
        applierProxy.register(this);
    }

    /**
     * Resets all {@link ShardSubscriber}s with a new {@link ReplicationConfiguration}. This method should be called
     * only after the StreamWorker is shutdown.
     *
     * @param replicationConfig
     *            The new {@link ReplicationConfiguration}
     */
    public synchronized void resetConfiguration(final ReplicationConfiguration replicationConfig) {
        replicationConfiguration = replicationConfig;
        initialize();
    }

    /**
     * Resets all {@link ShardSubscriber}s with a new {@link ReplicationConfiguration}. This method should be called
     * only after the StreamWorker is shutdown.
     *
     */
    private void resetSubscribers() {
        regionConfiguration = replicationConfiguration.getRegionConfiguration(region);
        for (final ConcurrentHashMap<String, ShardSubscriber> shardSubscriberMap : subscribers.values()) {
            // Make sure all ShardSubscribers for this stream was shut down and unregistered.
            while (!shardSubscriberMap.isEmpty()) {
                try {
                    Thread.sleep(SUBSCRIBER_SHUTDOWN_WAIT_TIME_MILLIS);
                } catch (final InterruptedException e) {
                    LOGGER.warn("ReplicationWorker for region " + region + " was interrupted");
                }
            }
        }

        subscribers.clear();
        for (final String table : replicationConfiguration.getTables(region)) {
            LOGGER.debug("resetting subscriber for " + table + " in " + region);
            if (regionConfiguration.getTableConfiguration(table).hasStreams()) {
                subscribers.put(table, new ConcurrentHashMap<String, ShardSubscriber>());
                final TableConfiguration tableConfiguration = regionConfiguration.getTableConfiguration(table);
                assert tableConfiguration != null;
                streamsWorkers.put(table, new Worker(new ShardSubscriberImplFactory(table, this),
                    tableConfiguration.getKinesisClientLibConfiguration(), tableConfiguration.getKinesisClient(),
                    tableConfiguration.getDynamoDBClient(), tableConfiguration.getCloudWatchClient()));
                final KinesisClientLibConfiguration config = tableConfiguration.getKinesisClientLibConfiguration();
                LOGGER.debug("Invoked worker for stream: " + config.getStreamName());
            }
        }
        if (isMasterRegion()) {
            subscriberProxy.register(this);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void shutdownAllStreams() {
        for (final Worker worker : streamsWorkers.values()) {
            worker.shutdown();
        }
        for (final String table : replicationConfiguration.getTables(region)) {
            replicationConfiguration.getRegionConfiguration(region).setTableStatus(table,
                ReplicationTableStatus.STOPPED);
        }
        workerStatus = LocalReplicationWorkerStatus.STOPPED;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void shutdownSingleStream(final String table) {
        if (streamsWorkers.containsKey(table)) {
            streamsWorkers.get(table).shutdown();
            replicationConfiguration.getRegionConfiguration(region).setTableStatus(table,
                ReplicationTableStatus.STOPPED);
        }
        workerStatus = LocalReplicationWorkerStatus.STOPPED;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void startAllStreams() {
        if (workerStatus != LocalReplicationWorkerStatus.INITIALIZED) {
            initialize();
        }
        for (final Entry<String, Worker> entry : streamsWorkers.entrySet()) {
            final Worker tableWorker = entry.getValue();
            final ExecutorService service = Executors.newSingleThreadExecutor();
            service.submit(tableWorker);
            service.shutdown();
            LOGGER.debug("Started worker for : " + entry.getKey() + " in region " + getRegionName());
        }
        for (final String table : replicationConfiguration.getTables(region)) {
            replicationConfiguration.getRegionConfiguration(region).setTableStatus(table,
                ReplicationTableStatus.REPLICATING);
            LOGGER.debug("Table " + table + " is now in working status");
        }
        workerStatus = LocalReplicationWorkerStatus.RUNNING;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void startSingleStream(final String table) {
        if (workerStatus != LocalReplicationWorkerStatus.INITIALIZED) {
            initialize();
        }
        if (streamsWorkers.containsKey(table)) {
            final ExecutorService service = Executors.newSingleThreadExecutor();
            service.submit(streamsWorkers.get(table));
            service.shutdown();
            replicationConfiguration.getRegionConfiguration(region).setTableStatus(table,
                ReplicationTableStatus.REPLICATING);
            LOGGER.debug("Table " + table + " is now in working status");
        }
        workerStatus = LocalReplicationWorkerStatus.RUNNING;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void unregisterShardSubscriber(final ShardSubscriber subscriber) {
        final ShardSubscriberImpl shardSubscriberImpl = (ShardSubscriberImpl) subscriber;
        final ConcurrentHashMap<String, ShardSubscriber> subscriberMap = subscribers.get(shardSubscriberImpl
            .getTableName());
        if (subscriberMap != null) {
            if (subscriberMap.containsKey(shardSubscriberImpl.getShardId())) {
                subscriberMap.remove(subscriber.getShardId());
            }
        } else {
            LOGGER.warn("Table not existed: Cannot unregister subscriber for table "
                + shardSubscriberImpl.getTableName());
        }
    }

}
