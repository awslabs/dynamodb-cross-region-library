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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Data;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBDocument;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshalling;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMemberStatus.DynamoDBReplicationGroupMemberStatusMarshaller;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a physical table member to a replication group. Optionally specifies a table copy task as well as connectors.
 */
@Data
@DynamoDBDocument
public class DynamoDBReplicationGroupMember {
    /**
     * Makes a deep copy of a list of {@link DynamoDBReplicationGroupMember}.
     *
     * @param toCopy
     *            A list of {@link DynamoDBReplicationGroupMember}
     * @return A deep copy of the provided list of {@link DynamoDBReplicationGroupMember}
     */
    public static Map<String, DynamoDBReplicationGroupMember> copyReplicationGroupMembers(Map<String, DynamoDBReplicationGroupMember> toCopy) {
        Map<String, DynamoDBReplicationGroupMember> copy = new HashMap<String, DynamoDBReplicationGroupMember>();
        for (Map.Entry<String, DynamoDBReplicationGroupMember> entry : toCopy.entrySet()) {
            copy.put(entry.getKey(), new DynamoDBReplicationGroupMember(entry.getValue()));
        }
        return copy;
    }

    /**
     * Optional table copy task executed after table creation, but before connectors are started.
     */
    @DynamoDBAttribute(attributeName = Constants.TABLE_COPY_TASK)
    @JsonProperty(Constants.TABLE_COPY_TASK)
    private DynamoDBTableCopyDescription tableCopyTask;

    /**
     * A parameter indicating whether or not the replication group member should have DynamoDB Streams turned on
     */
    @DynamoDBAttribute(attributeName = Constants.STREAM_ENABLED)
    @JsonProperty(Constants.STREAM_ENABLED)
    private Boolean streamsEnabled;

    /**
     * The connectors to run with this replication group member as the source.
     */
    @DynamoDBAttribute(attributeName = Constants.CONNECTORS)
    @JsonProperty(Constants.CONNECTORS)
    private List<DynamoDBConnectorDescription> connectors;

    /**
     * The arn of the replication group member for information on formatting
     *
     * @see http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#arn-syntax-ddb
     */
    @DynamoDBAttribute(attributeName = Constants.ARN)
    @JsonProperty(Constants.ARN)
    private String arn;

    /**
     * The endpoint of the replication group member.
     *
     * @see http://docs.aws.amazon.com/general/latest/gr/rande.html#ddb_region
     */
    @DynamoDBAttribute(attributeName = Constants.ENDPOINT)
    @JsonProperty(Constants.ENDPOINT)
    private String endpoint;

    /**
     * The global secondary indexes for the replication group member.
     */
    @DynamoDBAttribute(attributeName = Constants.GLOBAL_SECONDARY_INDEXES)
    @JsonProperty(Constants.GLOBAL_SECONDARY_INDEXES)
    private List<SecondaryIndexDesc> globalSecondaryIndexes;

    /**
     * The local secondary indexes for the replication group member.
     */
    @DynamoDBAttribute(attributeName = Constants.LOCAL_SECONDARY_INDEXES)
    @JsonProperty(Constants.LOCAL_SECONDARY_INDEXES)
    private List<SecondaryIndexDesc> localSecondaryIndexes;

    /**
     * The provisioned throughput for the replication group member.
     */
    @DynamoDBAttribute(attributeName = Constants.PROVISIONED_THROUGHPUT)
    @JsonProperty(Constants.PROVISIONED_THROUGHPUT)
    private ProvisionedThroughputDesc provisionedThroughput;

    /**
     * The state of the replication group member.
     */
    @DynamoDBAttribute(attributeName = Constants.REPLICATION_GROUP_MEMBER_STATUS)
    @DynamoDBMarshalling(marshallerClass = DynamoDBReplicationGroupMemberStatusMarshaller.class)
    @JsonProperty(Constants.REPLICATION_GROUP_MEMBER_STATUS)
    private DynamoDBReplicationGroupMemberStatus replicationGroupMemberStatus;

    /**
     * Default constructor for Jackson.
     */
    public DynamoDBReplicationGroupMember() {
    }

    /**
     * Copy constructor for a {@link DynamoDBReplicationGroupMember}. Makes a deep copy.
     *
     * @param toCopy
     *            The {@link DynamoDBReplicationGroupMember} to make a deep copy of
     */
    public DynamoDBReplicationGroupMember(DynamoDBReplicationGroupMember toCopy) {
        if (null == toCopy) {
            return;
        }
        // Deep copy
        if (toCopy.getTableCopyTask() != null) {
            setTableCopyTask(new DynamoDBTableCopyDescription(toCopy.getTableCopyTask()));
        }
        // Deep copy
        setConnectors(DynamoDBConnectorDescription.copyConnectors(toCopy.getConnectors()));
        // Immutable
        setEndpoint(toCopy.getEndpoint());
        // Deep copy
        setGlobalSecondaryIndexes(SecondaryIndexDesc.copySecondaryIndexes(toCopy.getGlobalSecondaryIndexes()));
        // Deep copy
        setLocalSecondaryIndexes(SecondaryIndexDesc.copySecondaryIndexes(toCopy.getLocalSecondaryIndexes()));
        // Immutable
        setArn(toCopy.getArn());
        // Deep copy
        if (toCopy.getProvisionedThroughput() != null) {
            setProvisionedThroughput(new ProvisionedThroughputDesc(toCopy.getProvisionedThroughput()));
        }
        // Immutable
        setReplicationGroupMemberStatus(toCopy.getReplicationGroupMemberStatus());
        // Immutable
        setStreamsEnabled(toCopy.getStreamsEnabled());
    }

    public DynamoDBReplicationGroupMember(AddReplicationGroupMemberRequest addRequest) {
        if (null == addRequest) {
            return;
        }
        // deep copy table copy task
        if (addRequest.getTableCopyTask() != null) {
            setTableCopyTask(new DynamoDBTableCopyDescription(addRequest.getTableCopyTask()));
        }
        // deep copy connectors
        setConnectors(DynamoDBConnectorDescription.copyConnectors(addRequest.getConnectors()));
        // immutable endpoint
        setEndpoint(addRequest.getEndpoint());
        // deep copy GSI
        setGlobalSecondaryIndexes(SecondaryIndexDesc.fromGSIList(addRequest.getGlobalSecondaryIndexes()));
        // deep copy LSI
        setLocalSecondaryIndexes(SecondaryIndexDesc.fromLSIList(addRequest.getLocalSecondaryIndexes()));
        // immutable arn
        setArn(addRequest.getMemberArn());
        // deep copy provisioned throughput
        if (addRequest.getProvisionedThroughput() != null) {
            setProvisionedThroughput(new ProvisionedThroughputDesc(addRequest.getProvisionedThroughput()));
        }
        // immutable streams enabled parameter
        setStreamsEnabled(addRequest.getStreamEnabled());
    }

    /**
     * Gets the region of the {@link DynamoDBReplicationGroupMember} using its endpoint.
     *
     * @return The region of the {@link DynamoDBReplicationGroupMember}
     */
    @DynamoDBIgnore
    @JsonIgnore
    public Region getRegion() {
        return null == endpoint ? null : RegionUtils.getRegionByEndpoint(endpoint);
    }

    /**
     * Returns true if all required fields are specified.
     *
     * @return True if all required fields are specified, otherwise false
     */
    @DynamoDBIgnore
    @JsonIgnore
    public boolean isValid() {
        boolean isValid = (null != endpoint) && (null != arn) && (null != replicationGroupMemberStatus) && (null != streamsEnabled);
        if (isValid) {
            try {
                new DynamoDBArn().withArnString(arn);
            } catch (IllegalArgumentException e) {
                return false;
            }
        }
        return isValid;
    }

    /**
     * Sets the streamsEnabled attribute and returns a reference to this instance for chained calls.
     *
     */
    public DynamoDBReplicationGroupMember withStreamsEnabled(boolean streamsEnabled) {
        setStreamsEnabled(streamsEnabled);
        return this;
    }

    /**
     * Sets the optional table copy task executed after table creation, but before connectors are started and returns a reference to this instance for chained
     * calls.
     *
     * @param tableCopyTask
     *            Optional table copy task executed after table creation, but before connectors are started.
     * @return A reference to this instance for chained calls
     */
    public DynamoDBReplicationGroupMember withTableCopyTask(DynamoDBTableCopyDescription tableCopyTask) {
        setTableCopyTask(tableCopyTask);
        return this;
    }

    /**
     * Sets the connectors to run with this replication group member as the source and returns a reference to this instance for chained calls.
     *
     * @param connectors
     *            The connectors to run with this replication group member as the source
     * @return A reference to this instance for chained calls
     */
    public DynamoDBReplicationGroupMember withConnectors(List<DynamoDBConnectorDescription> connectors) {
        setConnectors(connectors);
        return this;
    }

    /**
     * Sets the endpoint of the replication group member and returns a reference to this instance for chained calls.
     *
     * @param endpoint
     *            The endpoint of the replication group member
     * @return A reference to this instance for chained calls
     * @see http://docs.aws.amazon.com/general/latest/gr/rande.html#ddb_region
     */
    public DynamoDBReplicationGroupMember withEndpoint(String endpoint) {
        setEndpoint(endpoint);
        return this;
    }

    /**
     * Sets the global secondary indexes for the replication group member and returns a reference to this instance for chained calls.
     *
     * @param globalSecondaryIndexes
     *            The global secondary indexes for the replication group member
     * @return a reference to this instance for chained calls
     */
    public DynamoDBReplicationGroupMember withGlobalSecondaryIndexes(List<SecondaryIndexDesc> globalSecondaryIndexes) {
        setGlobalSecondaryIndexes(globalSecondaryIndexes);
        return this;
    }

    /**
     * Sets the local secondary indexes for replication group member and returns a reference to this instance for chained calls.
     *
     * @param localSecondaryIndexes
     *            The local secondary indexes for the replication group member
     * @return a reference to this instance for chained calls
     */
    public DynamoDBReplicationGroupMember withLocalSecondaryIndexes(List<SecondaryIndexDesc> localSecondaryIndexes) {
        setLocalSecondaryIndexes(localSecondaryIndexes);
        return this;
    }

    /**
     * Sets the ARN for the replication group member and returns a reference to this instance for chained calls
     *
     * @param arn
     *            The arn for the replication group member
     * @return A reference to this instance for chained calls
     */
    public DynamoDBReplicationGroupMember withARN(String arn) {
        setArn(arn);
        return this;
    }

    /**
     * Sets the provisioned throughput for the replication group member and returns a reference to this instance for chained calls.
     *
     * @param provisionedThroughput
     *            The provisioned throughput for the replication group member
     * @return a reference to this instance for chained calls
     */
    public DynamoDBReplicationGroupMember withProvisionedThroughput(ProvisionedThroughputDesc provisionedThroughput) {
        setProvisionedThroughput(provisionedThroughput);
        return this;
    }

    /**
     * Sets the state of the replication group member and returns a reference to this instance for chained calls.
     *
     * @param replicationGroupMemberStatus
     *            The state of the replication group member
     * @return A reference to this instance for chained calls
     */
    public DynamoDBReplicationGroupMember withReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus replicationGroupMemberStatus) {
        setReplicationGroupMemberStatus(replicationGroupMemberStatus);
        return this;
    }
}
