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
package com.amazonaws.services.dynamodbv2.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request for the DescribeReplicationGroup operation.
 */
@Data
public class DescribeReplicationGroupRequest {
    /**
     * The name of the replication group.
     */
    @JsonProperty(Constants.REPLICATION_GROUP_UUID)
    private String replicationGroupUUID;

    /**
     * Sets the UUID of the replication group and returns a reference to this instance for chained calls.
     *
     * @param replicationGroupUUID
     *            The name of the replication group
     * @return a reference to this instance for chained calls
     */
    public DescribeReplicationGroupRequest withReplicationGroupUUID(String replicationGroupUUID) {
        setReplicationGroupUUID(replicationGroupUUID);
        return this;
    }
}
