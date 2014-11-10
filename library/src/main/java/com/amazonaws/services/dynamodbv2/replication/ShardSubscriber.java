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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;

/**
 * <p>
 * Subscribes to a shard in the stream, distributes updates to {@link Applier}s, and checkpoints progress on the
 * shard when appliers acknowledge persistence of the update to all replication regions.
 * </p>
 * <p>
 * Each subscriber is subscribed to exactly one shard of one stream.
 * </p>
 */
public interface ShardSubscriber extends IRecordProcessor {
    /**
     * Logger for {@link ShardSubscriber}.
     */
    Logger LOGGER = LoggerFactory.getLogger(ShardSubscriber.class);

    /**
     * Marks the update identified by sequenceNumber as applied to tableApplied in the regionApplied.
     *
     * @param sequenceNumber
     *            The unique identifier of an update
     * @param regionApplied
     *            The region to which the update has been applied
     * @param tableApplied
     *            The table to which the update has been applied
     */
    void ack(String sequenceNumber, String regionApplied, String tableApplied);

    /**
     * Gets the latest sequence number where all appliers consumed record up to this point.
     *
     * @return The latest checkpoint
     */
    String getLatestCheckpoint();

    /**
     * Provides the unique identifier for this {@link ShardSubscriber}.
     *
     * @return The unique identifier for this {@link ShardSubscriber}
     */
    String getShardId();
}
