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
import java.util.Map;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request for the CreateReplicationGroup operation.
 */
@Data
public class CreateReplicationGroupRequest {

    /**
     * The attribute definitions for all members of the replication group.
     */
    @JsonProperty(Constants.ATTRIBUTE_DEFINITIONS)
    private List<AttributeDefinition> attributeDefinitions;

    /**
     * The key schema for all members of the replication group.
     */
    @JsonProperty(Constants.KEY_SCHEMA)
    private List<KeySchemaElement> keySchema;

    /**
     * The members of the replication group.
     */
    @JsonProperty(Constants.REPLICATION_GROUP_MEMBERS)
    private Map<String, DynamoDBReplicationGroupMember> replicationGroupMembers;

    /**
     * The UUID of the replication group.
     */
    @JsonProperty(Constants.REPLICATION_GROUP_UUID)
    private String replicationGroupUUID;

    /**
     * The name of the replication group
     */
    @JsonProperty(Constants.REPLICATION_GROUP_NAME)
    private String replicationGroupName;

    /**
     * The connector type for the replication group
     */
    @JsonProperty(Constants.CONNECTOR_TYPE)
    private String connectorType;

    /**
     * Sets the attribute definitions for all members of the replication group and returns a reference to this instance for chained calls.
     *
     * @param attributeDefinitions
     * @return a reference to this instance for chained calls
     */
    public CreateReplicationGroupRequest withAttributeDefinitions(List<AttributeDefinition> attributeDefinitions) {
        setAttributeDefinitions(attributeDefinitions);
        return this;
    }

    /**
     * Sets the key schema for all members of the replication group and returns a reference to this instance for chained calls.
     *
     * @param keySchema
     *            The key schema for all members of the replication group
     * @return a reference to this instance for chained calls
     */
    public CreateReplicationGroupRequest withKeySchema(List<KeySchemaElement> keySchema) {
        setKeySchema(keySchema);
        return this;
    }

    /**
     * Sets the members of the replication group and returns a reference to this instance for chained calls.
     *
     * @param replicationGroupMembers
     *            The members of the replication group
     * @return a reference to this instance for chained calls
     */
    public CreateReplicationGroupRequest withReplicationGroupMembers(Map<String, DynamoDBReplicationGroupMember> replicationGroupMembers) {
        setReplicationGroupMembers(replicationGroupMembers);
        return this;
    }

    /**
     * Sets the UUID of the replication group and returns a reference to this instance for chained calls.
     *
     * @param replicationGroupUUID
     *            The UUID of the replication group
     * @return a reference to this instance for chained calls
     */
    public CreateReplicationGroupRequest withReplicationGroupUUID(String replicationGroupUUID) {
        setReplicationGroupUUID(replicationGroupUUID);
        return this;
    }

    /**
     * Sets the name of the replication group and returns a reference to this instance for chained calls.
     *
     * @param replicationGroupName
     *            The name of the replication group
     * @return a reference to this instance for chained calls
     */
    public CreateReplicationGroupRequest withReplicationGroupName(String replicationGroupName) {
        setReplicationGroupName(replicationGroupName);
        return this;
    }

    /**
     * Sets the connector type for the replication group and returns a reference to this instance for chained calls.
     *
     * @param connectorType
     *            The connector type for the replication group
     * @return a reference to this instance for chained calls
     */
    public CreateReplicationGroupRequest withConnectorType(String connectorType) {
        setConnectorType(connectorType);
        return this;
    }

}
