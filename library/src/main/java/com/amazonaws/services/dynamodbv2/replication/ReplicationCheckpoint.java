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

import java.util.Map;

/**
 * Representation of a checkpoint for a stream.
 */
public interface ReplicationCheckpoint {
    /**
     * Gets the region name.
     *
     * @return The region name
     */
    String getRegion();

    /**
     * Gets the map of shardID to sequence number.
     *
     * @return The map of shardID to sequence number
     */
    Map<String, String> getSequenceNumberMap();

    /**
     * Gets the table name.
     *
     * @return The table name
     */
    String getTable();
}
