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
package com.amazonaws.services.dynamodbv2.replication.impl;

import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.replication.MultiRegionCheckpoint;
import com.amazonaws.services.dynamodbv2.replication.ReplicationConfiguration;
import com.fasterxml.jackson.databind.util.ISO8601Utils;

/**
 * A {@link MultiRegionCheckpoint} backed by a {@link BitSet}.
 */
public class BitSetMultiRegionCheckpoint implements MultiRegionCheckpoint {
    /**
     * Separator between region and table for the {@link #bitMap} key.
     */
    private static final String SEPARATOR = " -> "; // ' ' and '>' are illegal table name characters
    /**
     * The sequenceNumber of the UpdateRecord in the shard with which this MultiRegionCheckpoint is associated.
     */
    private final String sequenceNumber;
    /**
     * Lookup table for which bit in {@link #bitSet} corresponds to the '[region][{@link #SEPARATOR}][table]' key.
     */
    private final Map<String, Integer> bitMap;
    /**
     * The total number of region/table pairs from which this MultiRegionCheckpoint requires acks.
     */
    private final int cardinality;
    /**
     * BitSet used to keep track of acks. A bit represents the region/table combination mapped to that index in
     * {@link #bitMap}. A set bit means the ack has been received.
     */
    private final BitSet bitSet;
    /**
     * The time that users created the update and sent to the DynamoDB.
     */
    private final long createdTime;
    /**
     * The the end-to-end latency of replicating the update to each destination table in each region.
     */
    private final HashMap<String, HashMap<String, Long>> latencyPerRegion;
    /**
     * The end-to-end latency of replicating the update to all tables.
     */
    private long latency;

    /**
     * Constructs a BitSetMultiRegionCheckpoint configured to require acks for every region/table pair specified in the
     * supplied ReplicationConfiguration. It is associated with the update identified by its sequence number.
     *
     * @param configuration
     *            ReplicationConfiguration containing the region/table pairs from which this BitSetMultiRegionCheckpoint
     *            will require acks
     * @param sequenceNumber
     *            The unique identifier of the update to which this {@link BitSetMultiRegionCheckpoint} corresponds
     * @param createdTime
     *            The time that users created the update and sent to the DynamoDB
     */
    public BitSetMultiRegionCheckpoint(final ReplicationConfiguration configuration, final String sequenceNumber,
        final String createdTime) {
        this.sequenceNumber = sequenceNumber;
        // Map each table to a unique integer
        int index = 0;
        bitMap = new HashMap<String, Integer>();
        latencyPerRegion = new HashMap<String, HashMap<String, Long>>();

        for (final String region : configuration.getRegions()) {
            for (final String table : configuration.getTables(region)) {
                bitMap.put(getBitMapKey(region, table), index++);
            }
            latencyPerRegion.put(region, new HashMap<String, Long>());
        }
        cardinality = index;
        bitSet = new BitSet(cardinality);

        if (createdTime != null) {
            this.createdTime = ISO8601Utils.parse(createdTime).getTime();
        } else {
            this.createdTime = new Date().getTime();
        }

        latency = Long.MAX_VALUE;
    }

    @Override
    public void ack(final String region, final String table) {
        if (bitMap.containsKey(getBitMapKey(region, table))) {
            bitSet.set(bitMap.get(getBitMapKey(region, table)), true);
            latencyPerRegion.get(region).put(table, new Date().getTime() - createdTime);
            if (isReadyToCheckpoint()) {
                latency = latencyPerRegion.get(region).get(table);
            }
        } else {
            throw new IllegalArgumentException("Unknown region and table combination: " + region + " -> " + table);
        }
    }

    /**
     * Gets the key for the specified region and table for the bitMap.
     *
     * @param region
     *            The region to use in the lookup
     * @param table
     *            The table to use in the lookup
     * @return The key for the specified region and table for the bitMap.
     */
    private String getBitMapKey(final String region, final String table) {
        return region + SEPARATOR + table;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLatencyMillis() {
        return latency;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, HashMap<String, Long>> getLatencyPerTableMillis() {
        return latencyPerRegion;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isReadyToCheckpoint() {
        return bitSet.cardinality() == cardinality;
    }

}
