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
package com.amazonaws.services.dynamodbv2.replication.catchup;

import com.amazonaws.services.dynamodbv2.replication.ReplicationCheckpoint;

/**
 * The interface for managing region catchup and bootstrapping for a destination region with changes received from a
 * source region.
 */
public interface ReplicationCatchupCoordinator {
    /**
     * Does catchup for changes in a given checkpoint ranges.
     *
     * @param fromCheckpoint
     *            The {@link ReplicationCheckpoint} to start catching up
     * @param toCheckpoint
     *            The {@link ReplicationCheckpoint} to stop catching up
     */
    void catchUp(ReplicationCheckpoint fromCheckpoint, ReplicationCheckpoint toCheckpoint);

    /**
     * Does bootstrapping to copy data.
     *
     * @param replicationCheckpoint
     *            The lower bound checkpoint for copied data (No copied data is older than this checkpoint)
     */
    void copyTable(ReplicationCheckpoint replicationCheckpoint);
}
