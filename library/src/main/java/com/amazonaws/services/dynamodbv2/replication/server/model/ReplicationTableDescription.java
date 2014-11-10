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
package com.amazonaws.services.dynamodbv2.replication.server.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ReplicationTableDescription {

    @JsonProperty("TableName")
    public String tableName;
    @JsonProperty("Region")
    public String region;
    @JsonProperty("TableStatus")
    public String tableStatus;
    @JsonProperty("Master")
    public boolean isMaster;
    @JsonProperty("AccountId")
    public String accountId;
    @JsonProperty("KinesisApplicationName")
    public String kinesisApplicationName;

}
