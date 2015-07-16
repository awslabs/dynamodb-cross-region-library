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
import com.amazonaws.services.dynamodbv2.replication.AccountMapToAwsAccess;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationCoordinator;
import com.amazonaws.services.dynamodbv2.replication.MetadataStorage;

/**
 * Represents the transition for replication group update started, where its status changes from ACTIVE to UPDATING
 */
public class DynamoDBReplicationGroupUpdateStarted extends DynamoDBReplicationGroupTransition {

    private static final Logger LOGGER = Logger.getLogger(DynamoDBReplicationGroupUpdateStarted.class);

    /**
     * Default constructor
     *
     * @param oldGroup
     *            old image of the replication group
     * @param newGroup
     *            new image of the replication group
     */
    public DynamoDBReplicationGroupUpdateStarted(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        super(oldGroup, newGroup);
    }

    @Override
    public void transition(MetadataStorage metadata, AccountMapToAwsAccess awsAccess) {
        // find the replication group member that was updated
        LOGGER.info("Updating group with name: " + newGroup.getReplicationGroupName() + " and UUID: " + newGroup.getReplicationGroupUUID()
            + " started, evaluating group member differences next.");
        evaluateGroupMemberDiff(oldGroup, newGroup).transition(metadata, awsAccess);
    }

    @Override
    public void validateGroup(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        // Make sure there is a replication group member in either creating, updating or deleting state
        for (DynamoDBReplicationGroupMember member : newGroup.getReplicationGroupMembers().values()) {
            DynamoDBReplicationGroupMemberStatus status = member.getReplicationGroupMemberStatus();
            if (VALID_UPDATING_GROUP_MEMBER_STATUS.contains(status)) {
                return; // found a replication group member in creating, updating or deleting state
            }
        }
        DynamoDBReplicationCoordinator.coordinatorFail(REPLICATION_GROUP_STATUS_CANNOT_TRANSITION_TO_UPDATING);
    }

}
