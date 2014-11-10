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
 * Factory for creating {@link MultiRegionCheckpoint}s.
 */
public interface MultiRegionCheckpointFactory {
    /**
     * Creates a {@link MultiRegionCheckpoint} based on the update sequenceNumber.
     *
     * @param createdTime
     *            The time that the update is applied to the master table.
     * @param sequenceNumber
     *            Unique identifier for the update within the shard.
     * @return A {@link MultiRegionCheckpoint} for the update.
     */
    MultiRegionCheckpoint createCheckpoint(String sequenceNumber, String createdTime);
}
