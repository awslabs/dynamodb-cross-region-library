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
 * Request for the AddReplicationGroup operation.
 */
@Data
public class AddReplicationGroupMemberRequest {

    /**
     * Optional table copy task executed after table creation, but before connectors are started.
     */
    @JsonProperty(Constants.TABLE_COPY_TASK)
    private DynamoDBTableCopyDescription tableCopyTask;

    /**
     * The connectors to run with this replication group member as the destination.
     */
    @JsonProperty(Constants.CONNECTORS)
    private List<DynamoDBConnectorDescription> connectors;

    /**
     * The endpoint of the replication group member.
     *
     * @see http://docs.aws.amazon.com/general/latest/gr/rande.html#ddb_region
     */
    @JsonProperty(Constants.ENDPOINT)
    private String endpoint;

    /**
     * The global secondary indexes for the replication group member.
     */
    @JsonProperty(Constants.GLOBAL_SECONDARY_INDEXES)
    private List<GlobalSecondaryIndex> globalSecondaryIndexes;

    /**
     * The local secondary indexes for the replication group member.
     */
    @JsonProperty(Constants.LOCAL_SECONDARY_INDEXES)
    private List<LocalSecondaryIndex> localSecondaryIndexes;

    /**
     * The arn for the new replication group member.
     *
     * @see http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#arn-syntax-ddb
     */
    @JsonProperty(Constants.ARN)
    private String memberArn;

    /**
     * The provisioned throughput for the replication group member.
     */
    @JsonProperty(Constants.PROVISIONED_THROUGHPUT)
    private ProvisionedThroughput provisionedThroughput;

    /**
     * The replication group to add the the replication group member.
     */
    @JsonProperty(Constants.REPLICATION_GROUP_UUID)
    private String replicationGroupUUID;

    /**
     * Indicates whether Streams is to be enabled for the replication group member
     */
    @JsonProperty(Constants.STREAM_ENABLED)
    private Boolean streamEnabled;

    /**
     * Sets the optional table copy task executed after table creation, but before connectors are started and returns a reference to this instance for chained
     * calls.
     *
     * @param tableCopyTask
     *            Optional table copy task executed after table creation, but before connectors are started.
     * @return A reference to this instance for chained calls
     */
    public AddReplicationGroupMemberRequest withTableCopyTask(DynamoDBTableCopyDescription tableCopyTask) {
        setTableCopyTask(tableCopyTask);
        return this;
    }

    /**
     * Sets the connectors to run with this replication group member as the destination and returns a reference to this instance for chained calls.
     *
     * @param connectors
     *            The connectors to run with this replication group member as the destination
     * @return A reference to this instance for chained calls
     */
    public AddReplicationGroupMemberRequest withConnectors(List<DynamoDBConnectorDescription> connectors) {
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
    public AddReplicationGroupMemberRequest withEndpoint(String endpoint) {
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
    public AddReplicationGroupMemberRequest withGlobalSecondaryIndexes(List<GlobalSecondaryIndex> globalSecondaryIndexes) {
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
    public AddReplicationGroupMemberRequest withLocalSecondaryIndexes(List<LocalSecondaryIndex> localSecondaryIndexes) {
        setLocalSecondaryIndexes(localSecondaryIndexes);
        return this;
    }

    /**
     * Sets the table arn for the replication group member and returns a reference to this instance for chained calls.
     *
     * @param memberArn
     *            The ARN for the replication group member
     * @return A reference to this instance for chained calls
     */
    public AddReplicationGroupMemberRequest withMemberArn(String arn) {
        setMemberArn(arn);
        return this;
    }

    /**
     * Sets the provisioned throughput for the replication group member and returns a reference to this instance for chained calls.
     *
     * @param provisionedThroughput
     *            The provisioned throughput for the replication group member
     * @return a reference to this instance for chained calls
     */
    public AddReplicationGroupMemberRequest withProvisionedThroughput(ProvisionedThroughput provisionedThroughput) {
        setProvisionedThroughput(provisionedThroughput);
        return this;
    }

    /**
     * Sets the replication group to add the the replication group member and returns a reference to this instance for chained calls.
     *
     * @param replicationGroupUUID
     *            the replication group to add the the replication group member
     * @return A reference to this instance for chained calls
     */
    public AddReplicationGroupMemberRequest withReplicationGroupUUID(String replicationGroupUUID) {
        setReplicationGroupUUID(replicationGroupUUID);
        return this;
    }

    /**
     * Sets the replication group member streams enabled parameter to indicate whether the table needs to be have DynamoDB Streams enabled. Returns a reference
     * to this instance for chained calls.
     *
     * @param streamEnabled
     *            the variable indicating whether the table needs to have streams enabled
     * @return A reference to this instance for chained calls
     */
    public AddReplicationGroupMemberRequest withStreamsEnabled(Boolean streamEnabled) {
        setStreamEnabled(streamEnabled);
        return this;
    }

}
