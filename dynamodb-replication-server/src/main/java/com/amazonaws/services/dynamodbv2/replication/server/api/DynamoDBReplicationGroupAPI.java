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
package com.amazonaws.services.dynamodbv2.replication.server.api;

import lombok.Data;

import com.amazonaws.services.dynamodbv2.model.Constants;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationCoordinatorRequests;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Represents the general API framework for coordinator server, supports all commands in {@link DynamoDBReplicationCoordinatorRequests}
 *
 */
@Data
public class DynamoDBReplicationGroupAPI {
    /*
     * The name of the DynamoDB replication group API command
     */
    @JsonProperty(Constants.COMMAND)
    private String command;

    /*
     * The arguments to the command
     */
    @JsonProperty(Constants.ARGUMENTS)
    private JsonNode commandArguments;

    /*
     * Expected current version of the replication group
     */
    @JsonProperty(Constants.VERSION)
    private Long version;
}
