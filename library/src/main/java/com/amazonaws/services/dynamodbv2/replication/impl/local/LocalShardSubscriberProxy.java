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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.replication.RegionReplicationWorker;
import com.amazonaws.services.dynamodbv2.replication.ShardSubscriberProxy;

/**
 * SubscriberProxy for Subscribers running on the local JVM.
 */
public class LocalShardSubscriberProxy implements ShardSubscriberProxy {
    /**
     * Logger for LocalSubscriberProxy.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalShardSubscriberProxy.class);
    /**
     * Lookup map for subscribers. Key is the unique subscriberID.
     */
    private final Map<String, RegionReplicationWorker> subscriberMap;

    /**
     * Constructs a LocalSubscriberProxy with no registered subscribers.
     */
    public LocalShardSubscriberProxy() {
        subscriberMap = new HashMap<String, RegionReplicationWorker>();
    }

    @Override
    public void ack(final String sequenceNumber, final String appliedRegion, final String appliedTable,
        final String sourceRegion, final String sourceTable, final String subscriberID) {
        if (subscriberMap.containsKey(sourceRegion)) {
            subscriberMap.get(sourceRegion).ack(sequenceNumber, appliedRegion, appliedTable, sourceTable, subscriberID);
        } else {
            LOGGER.warn("RegionReplicationWorker for region " + sourceRegion
                + "is not registered. Could not ack for sequence number: " + sequenceNumber + " -> " + appliedTable);
        }
    }

    @Override
    public synchronized boolean register(final RegionReplicationWorker worker) {
        if (subscriberMap.containsKey(worker.getRegionName())) {
            LOGGER.warn("RegionReplicationWorker for region " + worker.getRegionName()
                + " could not register because it is already registered.");
            return false;
        }
        subscriberMap.put(worker.getRegionName(), worker);
        return true;
    }

    @Override
    public synchronized boolean unregister(final String region) {
        if (subscriberMap.containsKey(region)) {
            subscriberMap.remove(region);
            return true;
        }

        LOGGER.warn("RegionReplicationWorker for region " + region
            + " could not unregister because it is not registered.");
        return false;
    }

}
