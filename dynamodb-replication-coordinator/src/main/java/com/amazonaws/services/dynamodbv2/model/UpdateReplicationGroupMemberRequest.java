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

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request for the UpdateReplicationGroupMember operation.
 */
@Data
public class UpdateReplicationGroupMemberRequest {
    /**
     * The connectors to run with this replication group member as the destination.
     */
    @JsonProperty(Constants.CONNECTORS)
    private List<DynamoDBConnectorDescription> connectors;

    /**
     * The arn string for the replication group member.
     */
    @JsonProperty(Constants.ARN)
    private String memberArn;

    /**
     * The replication group to remove the the replication group member.
     */
    @JsonProperty(Constants.REPLICATION_GROUP_UUID)
    private String replicationGroupUUID;

    /**
     * Sets the connectors to run with this replication group member as the destination and returns a reference to this instance for chained calls.
     *
     * @param connectors
     *            The connectors to run with this replication group member as the destination
     * @return A reference to this instance for chained calls
     */
    public UpdateReplicationGroupMemberRequest withConnectors(List<DynamoDBConnectorDescription> connectors) {
        setConnectors(connectors);
        return this;
    }

    /**
     * Sets the arn string for the replication group member and returns a reference to this instance for chained calls.
     *
     * @param memberArn
     *            The arn string for the replication group member
     * @return A reference to this instance for chained calls
     */
    public UpdateReplicationGroupMemberRequest withMemberArn(String memberArn) {
        setMemberArn(memberArn);
        return this;
    }

    /**
     * Sets the replication group to remove the the replication group member and returns a reference to this instance for chained calls.
     *
     * @param replicationGroupUUID
     *            the replication group to remove the the replication group member
     * @return A reference to this instance for chained calls
     */
    public UpdateReplicationGroupMemberRequest withReplicationGroupUUID(String replicationGroupUUID) {
        setReplicationGroupUUID(replicationGroupUUID);
        return this;
    }

}
