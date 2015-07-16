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

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshaller;

/**
 * Lifecycle statuses for a {@link DynamoDBReplicationGroupMember}.
 */
public enum DynamoDBReplicationGroupMemberStatus {
    /**
     * Bootstrap task has completed and all connectors are running.
     */
    ACTIVE,
    /**
     * The bootstrap task was cancelled.
     */
    BOOTSTRAP_CANCELLED,
    /**
     * The bootstrap task has successfully completed.
     */
    BOOTSTRAP_COMPLETE,
    /**
     * The bootstrap task has failed; retry or report error.
     */
    BOOTSTRAP_FAILED,
    /**
     * The DynamoDB table is being initialized with the provided bootstrap task.
     */
    BOOTSTRAPPING,
    /**
     * Replication group member has been created and is creating the associated DynamoDB table.
     */
    CREATING,
    /**
     * Replication group member has encountered an error during creation.
     */
    CREATE_FAILED,
    /**
     * Replication group member is being deleted and is stopping the bootstrap task and all connectors.
     */
    DELETING,
    /**
     * Replication group member has encountered an error during deletion.
     */
    DELETE_FAILED,
    /**
     * One or more connectors are being started or stopped and/or DynamoDB Streams are being enabled/disabled for the DynamoDB table.
     */
    UPDATING,
    /**
     * Replication group member has encounted an error during update.
     */
    UPDATE_FAILED,
    /**
     * Replication group member has been created, the DynamoDB table is ACTIVE, and another group member is blocking the table copy task.
     */
    WAITING;

    /**
     * Marshaller for {@link DynamoDBMapper}.
     */
    public static class DynamoDBReplicationGroupMemberStatusMarshaller implements DynamoDBMarshaller<DynamoDBReplicationGroupMemberStatus> {

        @Override
        public String marshall(DynamoDBReplicationGroupMemberStatus getterReturnResult) {
            return getterReturnResult.toString();
        }

        @Override
        public DynamoDBReplicationGroupMemberStatus unmarshall(Class<DynamoDBReplicationGroupMemberStatus> clazz, String obj) {
            return DynamoDBReplicationGroupMemberStatus.valueOf(obj);
        }

    }
}
