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

import java.io.IOException;
import java.util.List;

import org.apache.http.annotation.ThreadSafe;

import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;

/**
 * Cross region replication coordinator metadata storage.
 */
@ThreadSafe
public interface MetadataStorage {

    /**
     * Gets the metadata for all replication groups.
     *
     * @return The metadata for all replication groups
     *
     * @throws IOException
     *             Error getting metadata
     */
    List<String> readReplicationGroups() throws IOException;

    /**
     * Gets the metadata for a single replication group.
     *
     * @param replicationGroupUUID
     *            The UUID of the replication group
     * @return The metadata for the replication group
     * @throws IOException
     *             Error getting metadata
     * @throws ResourceNotFoundException
     *             Replication group does not exist
     */
    DynamoDBReplicationGroup readReplicationGroup(String replicationGroupUUID) throws IOException;

    /**
     * Writes the metadata for a replication group using a compare and swap. If
     * {@link DynamoDBReplicationGroup#getVersion()} is used for optimistic concurrency, the version for the two
     * arguments must be equal.
     *
     * @param expectedValue
     *            The expected replication group metadata
     * @param newValue
     *            The replication group metadata to write if the expected metadata is present in metadata storage
     * @return The resulting value in the metadata storage
     * @throws IOException
     *             Error writing the metadata
     */
    DynamoDBReplicationGroup compareAndWriteReplicationGroup(DynamoDBReplicationGroup expectedValue, DynamoDBReplicationGroup newValue)
        throws IOException;

}
