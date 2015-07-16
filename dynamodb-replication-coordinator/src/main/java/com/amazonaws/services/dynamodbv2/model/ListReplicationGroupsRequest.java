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
 * Request for the ListReplicationGroups operation.
 */
@Data
public class ListReplicationGroupsRequest {
    /**
     * The first table name that this operation will evaluate. Use the value that was returned for
     * LastEvaluatedReplicationGroupName in a previous operation, so that you can obtain the next page of results.
     */
    @JsonProperty(Constants.EXCLUSIVE_START_REPLICATION_GROUP_NAME)
    private String exclusiveStartReplicationGroupName;
    /**
     * The maximum number of replication groups to return.
     */
    @JsonProperty(Constants.LIMIT)
    private Integer limit;

    /**
     * Sets the first table name that this operation will evaluate. Use the value that was returned for
     * LastEvaluatedReplicationGroupName in a previous operation, so that you can obtain the next page of results.
     * Returns a reference to this instance for chained calls.
     *
     * @param exclusiveStartReplicationGroupName
     *            The first table name that this operation will evaluate. Use the value that was returned for
     *            LastEvaluatedReplicationGroupName in a previous operation, so that you can obtain the next page of
     *            results.
     * @return a reference to this instance for chained calls
     */
    public ListReplicationGroupsRequest withExclusiveStartReplicationGroupName(String exclusiveStartReplicationGroupName) {
        setExclusiveStartReplicationGroupName(exclusiveStartReplicationGroupName);
        return this;
    }

    /**
     * Sets the maximum number of replication groups to return and returns a reference to this instance for chained
     * calls.
     *
     * @param limit
     *            the maximum number of replication groups to return
     * @return a reference to this instance for chained calls
     */
    public ListReplicationGroupsRequest withLimit(Integer limit) {
        setLimit(limit);
        return this;
    }
}
