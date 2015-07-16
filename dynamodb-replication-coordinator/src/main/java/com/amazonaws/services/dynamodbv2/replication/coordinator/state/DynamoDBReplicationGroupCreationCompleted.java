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

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMember;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMemberStatus;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupStatus;
import com.amazonaws.services.dynamodbv2.replication.AccountMapToAwsAccess;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationCoordinator;
import com.amazonaws.services.dynamodbv2.replication.MetadataStorage;

/**
 * Represents the transition for replication group creation completed, where its status changes from CREATING to ACTIVE
 */
public class DynamoDBReplicationGroupCreationCompleted extends DynamoDBReplicationGroupTransition {

    private static final Logger LOGGER = Logger.getLogger(DynamoDBReplicationGroupCreationCompleted.class);

    /**
     * Default constructor
     *
     * @param oldGroup
     *            old image of the replication group
     * @param newGroup
     *            new image of the replication group
     */
    public DynamoDBReplicationGroupCreationCompleted(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        super(oldGroup, newGroup);
    }

    @Override
    public void transition(MetadataStorage metadata, AccountMapToAwsAccess awsAccess) {
        LOGGER.info("Creation of group with name: " + newGroup.getReplicationGroupName() + " and UUID: " + newGroup.getReplicationGroupUUID() + " is completed.");
        // nothing much to do here
    }

    @Override
    public void validateGroup(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        DynamoDBReplicationCoordinator.coordinatorAssert(oldGroup.getReplicationGroupStatus().equals(DynamoDBReplicationGroupStatus.CREATING),
            INVALID_REPLICATION_GROUP_STATUS);
        for (DynamoDBReplicationGroupMember member : newGroup.getReplicationGroupMembers().values()) {
            DynamoDBReplicationCoordinator.coordinatorAssert(member.getReplicationGroupMemberStatus().equals(DynamoDBReplicationGroupMemberStatus.ACTIVE),
                ALL_REPLICATION_GROUP_MEMBERS_MUST_BE_ACTIVE);
        }
    }
}
