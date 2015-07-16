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
package com.amazonaws.services.dynamodbv2.replication.coordinator.state;

import static com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationCoordinator.coordinatorAssert;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMember;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMemberStatus;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupStatus;
import com.amazonaws.services.dynamodbv2.replication.AccountMapToAwsAccess;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationCoordinator;
import com.amazonaws.services.dynamodbv2.replication.MetadataStorage;

/**
 * Event change where a replication group was deleted from the metadata table (i.e. Streams record type is REMOVE).
 */
public class DynamoDBReplicationGroupDeletionCompleted extends DynamoDBReplicationGroupTransition {

    private static final Logger LOGGER = Logger.getLogger(DynamoDBReplicationGroupDeletionCompleted.class);

    /**
     * Default constructor
     *
     * @param oldGroup
     *            old image of the replication group
     * @param newGroup
     *            new image of the replication group
     */
    public DynamoDBReplicationGroupDeletionCompleted(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        super(oldGroup, newGroup);
    }

    @Override
    public void transition(MetadataStorage metadata, AccountMapToAwsAccess awsAccess) {
        LOGGER.info("Deletion of group with name: " + oldGroup.getReplicationGroupName() + " and UUID: " + oldGroup.getReplicationGroupUUID() + " is completed.");
        // nothing to do
    }

    @Override
    public void validateGroup(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        coordinatorAssert(newGroup == null, NEW_GROUP_NONNULL_DELETED);
        coordinatorAssert(oldGroup.getReplicationGroupStatus().equals(DynamoDBReplicationGroupStatus.DELETING),
            INVALID_REPLICATION_GROUP_STATUS + ", group was forcefully deleted without being in DELETING state first, make sure your EC2 container resources are cleaned up!");
        for (DynamoDBReplicationGroupMember member : oldGroup.getReplicationGroupMembers().values()) {
            DynamoDBReplicationCoordinator.coordinatorAssert(member.getReplicationGroupMemberStatus().equals(DynamoDBReplicationGroupMemberStatus.DELETING),
                ALL_REPLICATION_GROUP_MEMBERS_MUST_BE_DELETING);
        }
    }
}
