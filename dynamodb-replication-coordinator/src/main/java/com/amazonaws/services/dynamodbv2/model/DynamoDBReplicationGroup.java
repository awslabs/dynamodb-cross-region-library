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
import lombok.EqualsAndHashCode;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshalling;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupStatus.DynamoDBReplicationGroupStatusMarshaller;
import com.amazonaws.services.dynamodbv2.streams.connectors.DynamoDBConnectorType;
import com.amazonaws.services.dynamodbv2.streams.connectors.DynamoDBConnectorType.DynamoDBConnectorTypeMarshaller;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A single logical representation of a replicated DynamoDB table implemented by multiple physical DynamoDB tables, bootstrap tasks, and DynamoDB connectors.
 */
@Data
@EqualsAndHashCode(exclude = {"version"})
@DynamoDBTable(tableName = Constants.REPLICATION_GROUPS)
public class DynamoDBReplicationGroup {

    // TODO consider adding a metadata version number to keep track of metadata changes and maintain backwards compatibility

    /**
     * The attribute definitions for all members of the replication group.
     */
    @DynamoDBAttribute(attributeName = Constants.ATTRIBUTE_DEFINITIONS)
    @JsonProperty(Constants.ATTRIBUTE_DEFINITIONS)
    private List<AttributeDefinitionDescription> attributeDefinitions;

    /**
     * The key schema for all members of the replication group.
     */
    @DynamoDBAttribute(attributeName = Constants.KEY_SCHEMA)
    @JsonProperty(Constants.KEY_SCHEMA)
    private List<KeySchemaElementDescription> keySchema;

    /**
     * The members of the replication group, key is the replication group member ARN.
     *
     * @see http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#arn-syntax-ddb
     */
    @DynamoDBAttribute(attributeName = Constants.REPLICATION_GROUP_MEMBERS)
    @JsonProperty(Constants.REPLICATION_GROUP_MEMBERS)
    private Map<String, DynamoDBReplicationGroupMember> replicationGroupMembers;

    /**
     * The uuid of the replication group.
     */
    @DynamoDBHashKey(attributeName = Constants.REPLICATION_GROUP_UUID)
    @JsonProperty(Constants.REPLICATION_GROUP_UUID)
    private String replicationGroupUUID;

    /**
     * The name of the replication group.
     */
    @DynamoDBAttribute(attributeName = Constants.REPLICATION_GROUP_NAME)
    @JsonProperty(Constants.REPLICATION_GROUP_NAME)
    private String replicationGroupName;

    /**
     * The state of the replication group.
     */
    @DynamoDBAttribute(attributeName = Constants.REPLICATION_GROUP_STATUS)
    @DynamoDBMarshalling(marshallerClass = DynamoDBReplicationGroupStatusMarshaller.class)
    @JsonProperty(Constants.REPLICATION_GROUP_STATUS)
    private DynamoDBReplicationGroupStatus replicationGroupStatus;

    /**
     * The mode of operation for the connectors in this DynamoDB Replication Group.
     */
    @DynamoDBAttribute(attributeName = Constants.CONNECTOR_TYPE)
    @DynamoDBMarshalling(marshallerClass = DynamoDBConnectorTypeMarshaller.class)
    @JsonProperty(Constants.CONNECTOR_TYPE)
    private DynamoDBConnectorType connectorType;

    /**
     * Version number for optimistic locking.
     */
    @DynamoDBVersionAttribute
    @JsonProperty(Constants.VERSION)
    private Long version;

    /**
     * Default constructor for Jackson.
     */
    public DynamoDBReplicationGroup() {
    }

    /**
     * Copy constructor. Makes a deep copy of the {@link DynamoDBReplicationGroup}.
     *
     * @param toCopy
     *            The replication group to make a deep copy of
     */
    public DynamoDBReplicationGroup(DynamoDBReplicationGroup toCopy) {
        if (null == toCopy) {
            return;
        }
        // Make a deep copy
        setAttributeDefinitions(AttributeDefinitionDescription.copyAttributeDefinitionDescriptions(toCopy.getAttributeDefinitions()));
        // Make a deep copy
        setKeySchema(KeySchemaElementDescription.copyKeySchemaDescriptions(toCopy.getKeySchema()));
        // Make a deep copy
        setReplicationGroupMembers(DynamoDBReplicationGroupMember.copyReplicationGroupMembers(toCopy.getReplicationGroupMembers()));
        // Immutable
        setReplicationGroupUUID(toCopy.getReplicationGroupUUID());
        // Immutable
        setReplicationGroupName(toCopy.getReplicationGroupName());
        // Immutable
        setReplicationGroupStatus(toCopy.getReplicationGroupStatus());
        // Immutable
        setConnectorType(toCopy.getConnectorType());
        // Immutable
        setVersion(toCopy.getVersion());
    }

    /**
     * CreateReplicationGroupRequest constructor. Uses parameters from {@link CreateReplicationGroupRequest}.
     *
     * @param request
     *            The CreateReplicationGroupRequest object
     */
    public DynamoDBReplicationGroup(CreateReplicationGroupRequest request) {
        if (null == request) {
            return;
        }
        // attribute definitions
        setAttributeDefinitions(AttributeDefinitionDescription.convertToAttributeDefintionDescriptions(request.getAttributeDefinitions()));
        // key schemas
        setKeySchema(KeySchemaElementDescription.convertToKeySchemaElementDescriptions(request.getKeySchema()));
        // replication group members
        setReplicationGroupMembers(DynamoDBReplicationGroupMember.copyReplicationGroupMembers(request.getReplicationGroupMembers()));
        // Immutable, replication group UUID
        setReplicationGroupUUID(request.getReplicationGroupUUID());
        // Immutable, replication group name
        setReplicationGroupName(request.getReplicationGroupName());
        // connector type
        setConnectorType(Enum.valueOf(DynamoDBConnectorType.class, request.getConnectorType()));
    }

    /**
     * Add a replication group member to the replication group
     *
     * @param newM
     *            the new member to be added to the group
     * @return the replication group members after the new member has been added
     */
    public Map<String, DynamoDBReplicationGroupMember> addReplicationGroupMember(DynamoDBReplicationGroupMember newM) {
        replicationGroupMembers.put(newM.getArn(), new DynamoDBReplicationGroupMember(newM));
        return replicationGroupMembers;
    }

    /**
     * Remove a replication group member from the replication group
     *
     * @param arnString
     *            the arn of the replication group member used as the key to the map of replication group members
     * @return the previous value associated with the arn, or null if there was no mapping for the arn
     */
    public DynamoDBReplicationGroupMember removeReplicationGroupMember(String arnString) {
        return replicationGroupMembers.remove(arnString);
    }

    /**
     * Returns true if all required fields are specified.
     *
     * @return True if all required fields are specified, otherwise false
     */
    @DynamoDBIgnore
    @JsonIgnore
    public boolean isValid() {
        if (null != attributeDefinitions && null != keySchema && null != replicationGroupMembers && null != replicationGroupUUID
            && null != replicationGroupStatus && null != connectorType) {
            for (DynamoDBReplicationGroupMember member : replicationGroupMembers.values()) {
                if (!member.isValid()) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Sets the attribute definitions for all members of the replication group and returns a reference to this instance for chained calls.
     *
     * @param attributeDefinitions
     *            Attribute definitions for the replication group
     * @return a reference to this instance for chained calls
     */
    public DynamoDBReplicationGroup withAttributeDefinitions(List<AttributeDefinitionDescription> attributeDefinitions) {
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
    public DynamoDBReplicationGroup withKeySchema(List<KeySchemaElementDescription> keySchema) {
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
    public DynamoDBReplicationGroup withReplicationGroupMembers(Map<String, DynamoDBReplicationGroupMember> replicationGroupMembers) {
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
    public DynamoDBReplicationGroup withReplicationGroupUUID(String replicationGroupUUID) {
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
    public DynamoDBReplicationGroup withReplicationGroupName(String replicationGroupName) {
        setReplicationGroupName(replicationGroupName);
        return this;
    }

    /**
     * Sets the state of the replication group and returns a reference to this instance for chained calls.
     *
     * @param replicationGroupStatus
     *            The state of the replication group
     * @return A reference to this instance for chained calls
     */
    public DynamoDBReplicationGroup withReplicationGroupStatus(DynamoDBReplicationGroupStatus replicationGroupStatus) {
        setReplicationGroupStatus(replicationGroupStatus);
        return this;
    }

    /**
     * Sets the connector type and returns a reference to this instance for chained calls.
     *
     * @param connectorType
     *            The connector type
     * @return A reference to this instance for chained calls
     */
    public DynamoDBReplicationGroup withConnectorType(DynamoDBConnectorType connectorType) {
        setConnectorType(connectorType);
        return this;
    }

    /**
     * Sets the version of the replication group and returns a reference to this instance for chained calls.
     *
     * @param version
     *            The version number
     * @return A reference to this instance for chained calls
     */
    public DynamoDBReplicationGroup withVersion(Long version) {
        setVersion(version);
        return this;
    }

}
