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
 * Lifecycle statuses of a {@link DynamoDBReplicationGroup}.
 */
public enum DynamoDBReplicationGroupStatus {
    /**
     * All replication group members are {@link ReplicationGroupMemberStatus#ACTIVE}.
     */
    ACTIVE,
    /**
     * Replication group has been created and is waiting for all replication group members to become
     * {@link ReplicationGroupMemberStatus#ACTIVE}.
     */
    CREATING,
    /**
     * Replication group is being deleted and is waiting for all replication group members to finish deleting.
     */
    DELETING,
    /**
     * One or more replication group members is in a state other than {@link ReplicationGroupMemberStatus#ACTIVE}.
     */
    UPDATING;

    /**
     * Marshaller for {@link DynamoDBMapper}.
     */
    public static class DynamoDBReplicationGroupStatusMarshaller implements DynamoDBMarshaller<DynamoDBReplicationGroupStatus> {

        @Override
        public String marshall(DynamoDBReplicationGroupStatus getterReturnResult) {
            return getterReturnResult.toString();
        }

        @Override
        public DynamoDBReplicationGroupStatus unmarshall(Class<DynamoDBReplicationGroupStatus> clazz, String obj) {
            return DynamoDBReplicationGroupStatus.valueOf(obj);
        }

    }
}
