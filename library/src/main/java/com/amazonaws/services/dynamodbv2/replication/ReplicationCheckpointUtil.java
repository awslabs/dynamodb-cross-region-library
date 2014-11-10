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

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.replication.impl.ReplicationCheckpointImpl;

/**
 * Utility functions for converting between SequenceNumber and ReplicationCheckpoint.
 */
public final class ReplicationCheckpointUtil {
    /**
     * Compares two sequence numbers.
     *
     * @param sequenceNumber1
     *            The first sequence number
     * @param sequenceNumber2
     *            The second sequence number
     * @return -1, 0, or 1 if the first sequence number is less than, equal, or greater than the second sequence number
     */
    public static int compareSequenceNumbers(final String sequenceNumber1, final String sequenceNumber2) {
        final BigInteger number1 = new BigInteger(sequenceNumber1);
        final BigInteger number2 = new BigInteger(sequenceNumber2);
        return number1.compareTo(number2);
    }

    /**
     * Converts a Map of ReplicationCheckpoints to a SequenceNumber.
     *
     * @param replicationCheckpoints
     *            The ReplicationCheckpoint map
     * @return The minimum sequence number among sequence numbers of all shards in all ReplicationCheckpoints in the map
     */
    public static String replicationCheckpointMapToSequenceNumber(
        final Map<String, ReplicationCheckpoint> replicationCheckpoints) {
        String retVal = null;
        for (final ReplicationCheckpoint checkpoint : replicationCheckpoints.values()) {
            final String sequenceNumber = replicationCheckpointToSequenceNumber(checkpoint);
            if (retVal != null && sequenceNumber != null) {
                if (compareSequenceNumbers(retVal, sequenceNumber) > 0) {
                    retVal = sequenceNumber;
                }
            } else if (sequenceNumber != null) {
                retVal = sequenceNumber;
            }
        }
        return retVal;
    }

    /**
     * Converts a ReplicationCheckpoint to a SequenceNumber.
     *
     * @param replicationCheckpoint
     *            The ReplicationCheckpoint
     * @return The minimum sequence number among sequence numbers of all shards in the given ReplicationCheckpoint
     */
    public static String replicationCheckpointToSequenceNumber(final ReplicationCheckpoint replicationCheckpoint) {
        String retVal = null;
        for (final String sequenceNumber : replicationCheckpoint.getSequenceNumberMap().values()) {
            if (retVal != null && sequenceNumber != null) {
                if (compareSequenceNumbers(retVal, sequenceNumber) > 0) {
                    retVal = sequenceNumber;
                }
            } else if (sequenceNumber != null) {
                retVal = sequenceNumber;
            }
        }
        return retVal;
    }

    /**
     * Converts a sequenceNumber to a ReplicationCheckpoint via a template ReplicationCheckpoint.
     *
     * @param sequenceNumber
     *            The SequenceNumber
     * @param template
     *            The template ReplicationCheckpoint
     * @return A ReplicationCheckpoint with the sequence Map similar to the template and all values set to the
     *         sequenceNumber
     */
    public static ReplicationCheckpoint sequenceNumberToReplicationCheckpoint(final String sequenceNumber,
        final ReplicationCheckpoint template) {
        final HashMap<String, String> sequenceNumberMap = new HashMap<String, String>();
        for (final String shardId : template.getSequenceNumberMap().keySet()) {
            sequenceNumberMap.put(shardId, sequenceNumber);
        }
        return new ReplicationCheckpointImpl(template.getRegion(), template.getTable(), sequenceNumberMap);
    }

    /**
     * The private constructor.
     */
    private ReplicationCheckpointUtil() {
        // Not called.
    }
}
