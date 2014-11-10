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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.replication.ApplierProxy;
import com.amazonaws.services.dynamodbv2.replication.RegionReplicationWorker;

/**
 * An ApplierProxy based on TableAppliers running on the local JVM.
 */
public class LocalTableApplierProxy implements ApplierProxy {

    /**
     * Logger for LocalTableApplierProxy.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalTableApplierProxy.class);

    /**
     * Contains an RegionReplicationWorker for each region.
     */
    private final HashMap<String, RegionReplicationWorker> applierMap;

    /**
     * Constructs a local table applier proxy.
     */
    public LocalTableApplierProxy() {
        applierMap = new HashMap<String, RegionReplicationWorker>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(final Record updateRecord, final String sourceRegion, final String sourceTable,
        final String shardSubscriberID) {
        for (final String region : applierMap.keySet()) {
            applierMap.get(region).applyUpdateRecord(updateRecord, sourceRegion, sourceTable, shardSubscriberID);
            LOGGER.debug("Applied update record from " + sourceRegion + ": " + sourceTable + "to region: " + region);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean register(final RegionReplicationWorker worker) {
        if (applierMap.containsKey(worker.getRegionName())) {
            LOGGER.warn("RegionReplicationWorker for region " + worker.getRegionName()
                + " could not register because it is already registered.");
            return false;
        }
        applierMap.put(worker.getRegionName(), worker);
        LOGGER.debug(getClass().getName() + " registered ReplicationWorker for region:" + worker.getRegionName());
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean unregister(final String region) {
        if (applierMap.containsKey(region)) {
            applierMap.remove(region);
            return true;
        }
        LOGGER.warn("RegionReplicationWorker for region " + region
            + " could not unregister because it is not registered.");
        return false;

    }
}
