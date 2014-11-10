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
 * Decouples the communication between a Subscriber and Applier. A subscriber calls apply and the proxy distributes the
 * update to appliers for each replica table. Can be implemented in the local JVM using threads or on separate hosts
 * using sockets.
 */
public interface ApplierProxy {

    /**
     * Forwards an update to all {@link RegionReplicationWorker}s. Each RegionReplicationWorker then calls its applier
     * to apply the update on its region.
     *
     * @param updateRecord
     *            The update to distribute
     * @param sourceRegion
     *            The source region to receive an ack upon successful processing of the update
     * @param sourceTable
     *            The source table to receive an ack upon successful processing of the update
     * @param shardSubscriberID
     *            The subscriber to receive an ack upon successful processing of the update
     */
    void apply(Record updateRecord, String sourceRegion, String sourceTable, String shardSubscriberID);

    /**
     * Registers a {@link RegionReplicationWorker} with the proxy so an {@link Applier} can ack to it.
     *
     * @param worker
     *            The {@link RegionReplicationWorker} to register
     * @return True iff the worker is successfully registered
     */
    boolean register(RegionReplicationWorker worker);

    /**
     * Unregisters a {@link RegionReplicationWorker} with the proxy. {@link Applier}s will no longer be able to ack to
     * it.
     *
     * @param region
     *            The region to unregister
     * @return True if the region is successful unregistered
     */
    boolean unregister(String region);

}
