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

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the persistence of an update to all managed regions, identified by sequence number.
 */
public interface MultiRegionCheckpoint {

    /**
     * Acks for a particular region and table.
     *
     * @param region
     *            The region to which the update has been applied
     * @param table
     *            The table to which the update has been applied
     */
    void ack(String region, String table);

    /**
     * Gets the end-to-end latency of replicating the update to all tables.
     *
     * @return The end-to-end latency of replicating the update.
     */
    long getLatencyMillis();

    /**
     * Gets the end-to-end latency of replicating the update to each destination table in each region.
     *
     * @return The end-to-end latency of replicating the update to each destination table in each region
     */
    Map<String, HashMap<String, Long>> getLatencyPerTableMillis();

    /**
     * Gets the sequence number that identifies the record checkpoint.
     *
     * @return the sequence number that identifies the record checkpoint
     */
    String getSequenceNumber();

    /**
     * Gets the status of the record checkpoint. The record checkpoint is done iff all tables and regions have acked for
     * the record.
     *
     * @return True iff all tables and regions have acked for the record
     */
    boolean isReadyToCheckpoint();
}
