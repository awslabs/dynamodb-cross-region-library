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

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.replication.ReplicationCheckpoint;

/**
 * Implementation of {@link ReplicationCheckpoint}.
 */
public class ReplicationCheckpointImpl implements ReplicationCheckpoint {
    /**
     * The table name.
     */
    private final String table;
    /**
     * The region name.
     */
    private final String region;
    /**
     * The map of shardId to SequenceNumber.
     */
    private final HashMap<String, String> sequenceNumbers;

    /**
     * Constructor.
     *
     * @param region
     *            The region name
     * @param table
     *            The table name
     * @param sequenceNumbers
     *            The map of shardId to SequenceNumber
     */
    public ReplicationCheckpointImpl(final String region, final String table,
        final HashMap<String, String> sequenceNumbers) {
        this.table = table;
        this.region = region;
        this.sequenceNumbers = sequenceNumbers;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o) {
        if (o == null || !(o instanceof ReplicationCheckpoint)) {
            return false;
        }
        final ReplicationCheckpoint checkpoint = (ReplicationCheckpoint) o;
        return region.equals(checkpoint.getRegion()) && table.equals(checkpoint.getTable())
            && sequenceNumbers.equals(checkpoint.getSequenceNumberMap());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getRegion() {
        return region;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getSequenceNumberMap() {
        return sequenceNumbers;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTable() {
        return table;
    }
}
