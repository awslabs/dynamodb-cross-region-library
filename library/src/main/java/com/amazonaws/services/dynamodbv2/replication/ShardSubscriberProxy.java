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


/**
 * Connector between {@link Applier} and {@link ShardSubscriber}. Can be implemented a lookup table for local
 * application or sockets a disributed application.
 */
public interface ShardSubscriberProxy {
    /**
     * Forwards ack to the {@link RegionReplicationWorker} identified by regionAppled. The RegionReplicationWorker then
     * calls ack on the {@link ShardSubscriber} identified by sourceRegion, sourceTable, and subscriberID, with the 
     * parameters sequenceNumber, regionApplied, and tableApplied.
     *
     * @param sequenceNumber
     *            The unique identifier for the update within a shard
     * @param regionApplied
     *            The region where the update has been applied
     * @param tableApplied
     *            The table where the update has been applied
     * @param sourceRegion
     *            The region to send the ack to
     * @param sourceTable
     *            The table to send the ack to
     * @param subscriberID
     *            The unique identifier for the Subscriber
     */
    void ack(String sequenceNumber, String regionApplied, String tableApplied, String sourceRegion, String sourceTable,
        String subscriberID);

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
