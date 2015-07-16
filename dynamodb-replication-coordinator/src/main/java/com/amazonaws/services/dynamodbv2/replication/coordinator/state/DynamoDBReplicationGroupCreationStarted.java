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
import static com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationCoordinator.coordinatorFail;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMember;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMemberStatus;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupStatus;
import com.amazonaws.services.dynamodbv2.replication.AccountMapToAwsAccess;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationCoordinator;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationUtilities;
import com.amazonaws.services.dynamodbv2.replication.MetadataStorage;

/**
 * Event change where a replication group was added to the metadata table (i.e. Streams record type is INSERT). Create the replication group member with a given
 * priority and transition it into the next state (WAITING).
 */
public class DynamoDBReplicationGroupCreationStarted extends DynamoDBReplicationGroupTransition {

    private static final Logger LOGGER = Logger.getLogger(DynamoDBReplicationGroupCreationStarted.class);

    /**
     * Default constructor
     *
     * @param oldGroup
     *            old image of the replication group
     * @param newGroup
     *            new image of the replication group
     */
    public DynamoDBReplicationGroupCreationStarted(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        super(oldGroup, newGroup);
    }

    @Override
    public void transition(MetadataStorage metadata, AccountMapToAwsAccess awsAccess) {
        while (true) {
            try {
                // Retrieve current image of the replication group
                DynamoDBReplicationGroup curr = metadata.readReplicationGroup(newGroup.getReplicationGroupUUID());
                if (null == curr) {
                    return; // Group no longer exists, nothing to do here
                }

                // Assert replication group is creating
                if (!curr.getReplicationGroupStatus().equals(DynamoDBReplicationGroupStatus.CREATING)) {
                    return; // Group is no longer creating, nothing to do here
                }

                // Make a copy of the current replication group that we will update
                DynamoDBReplicationGroup updated = new DynamoDBReplicationGroup(curr);

                // If this is an empty replication group, set the group status to ACTIVE
                if (updated.getReplicationGroupMembers().isEmpty()) {
                    LOGGER.info("Created empty replication group with name: " + updated.getReplicationGroupName() + " and UUID: "
                        + updated.getReplicationGroupUUID() + ".");
                    updated.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.ACTIVE);
                } else {
                    // Create a queue of replication group members sorted by priority which is determined by presence of bootstrap task and connectors
                    PriorityQueue<DynamoDBReplicationGroupMember> creatingCandidates = new PriorityQueue<DynamoDBReplicationGroupMember>(updated
                        .getReplicationGroupMembers().size(), DynamoDBReplicationUtilities.CREATING_PRIORITY_GROUP_MEMBER_COMPARATOR);

                    // Add only replication group members with status CREATING to the queue
                    for (DynamoDBReplicationGroupMember member : updated.getReplicationGroupMembers().values()) {
                        switch (member.getReplicationGroupMemberStatus()) {
                            case CREATING:
                                creatingCandidates.add(member);
                                continue;
                            case DELETING:
                                continue;
                            default:
                                return; // Group creation has already been initiated
                        }
                    }

                    if (creatingCandidates.isEmpty()) {
                        LOGGER.warn("Replication Group in CREATING status does not contain any members with CREATING status");
                        return; // no replication member in CREATING status
                    } else {
                        // create table for the first replication group member in the queue, as necessary
                        DynamoDBReplicationGroupMember createMember = creatingCandidates.peek();
                        try {
                            DynamoDBReplicationUtilities.createTableIfNotExists(newGroup, createMember, awsAccess);
                            LOGGER.info("Created table for replication member with ARN: " + createMember.getArn());
                            createMember.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.WAITING);
                        } catch (Exception e) {
                            LOGGER.error("Unable to create table for replication member with ARN: " + createMember.getArn());
                            createMember.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.CREATE_FAILED);
                        }

                        // clean up the attributes that are no longer needed after table has been created
                        createMember.setGlobalSecondaryIndexes(null);
                        createMember.setLocalSecondaryIndexes(null);
                        createMember.setProvisionedThroughput(null);

                        // add the updated replication group member to the group
                        updated.addReplicationGroupMember(createMember);
                    }
                }
                if (updated.equals(metadata.compareAndWriteReplicationGroup(curr, updated))) {
                    return;
                } else {
                    // retry
                    continue;
                }
            } catch (IOException e) {
                coordinatorFail(FAILED_TO_ACCESS_METADATA + " with exception: " + e);
            }
        }

    }

    @Override
    public void validateGroup(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        coordinatorAssert(oldGroup == null, OLD_GROUP_NONNULL_CREATING);
        coordinatorAssert(newGroup.isValid(), INVALID_REPLICATION_GROUP);
        coordinatorAssert(newGroup.getReplicationGroupStatus().equals(DynamoDBReplicationGroupStatus.CREATING), INVALID_REPLICATION_GROUP_STATUS);
        for (DynamoDBReplicationGroupMember member : newGroup.getReplicationGroupMembers().values()) {
            DynamoDBReplicationCoordinator.coordinatorAssert(member.getReplicationGroupMemberStatus().equals(DynamoDBReplicationGroupMemberStatus.CREATING),
                ALL_REPLICATION_GROUP_MEMBERS_MUST_BE_CREATING);
        }
    }
}
