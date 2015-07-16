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

import static com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationCoordinator.coordinatorFail;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.DynamoDBConnectorDescription;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMember;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMemberStatus;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupStatus;
import com.amazonaws.services.dynamodbv2.replication.AccountMapToAwsAccess;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationCoordinator;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationUtilities;
import com.amazonaws.services.dynamodbv2.replication.MetadataStorage;

/**
 * Event change where a replication group starts deleting (i.e. Streams record old image is ACTIVE, CREATING or UPDATING, new image is DELETING).
 */
public class DynamoDBReplicationGroupDeletionStarted extends DynamoDBReplicationGroupTransition {

    private static final Logger LOGGER = Logger.getLogger(DynamoDBReplicationGroupDeletionStarted.class);

    /**
     * Default constructor
     *
     * @param oldGroup
     *            old image of the replication group
     * @param newGroup
     *            new image of the replication group
     */
    public DynamoDBReplicationGroupDeletionStarted(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        super(oldGroup, newGroup);
    }

    @Override
    public void transition(MetadataStorage metadata, AccountMapToAwsAccess awsAccess) {
        while (true) {
            try {
                // Retrieve current image of the replication group
                DynamoDBReplicationGroup curr = metadata.readReplicationGroup(newGroup.getReplicationGroupUUID());
                if (null == curr) {
                    return; // Group no longer, exists nothing to do here
                }

                // Assert replication group is deleting
                if (!curr.getReplicationGroupStatus().equals(DynamoDBReplicationGroupStatus.DELETING)) {
                    return; // Group is no longer deleting, nothing to do here
                }

                // Check if all members have been deleted, if so, delete group
                if (curr.getReplicationGroupMembers().isEmpty()) {
                    LOGGER.info("All replication members deleted, ready to delete replication group with name: " + curr.getReplicationGroupName()
                        + " and UUID: " + curr.getReplicationGroupUUID() + ".");
                    if (metadata.compareAndWriteReplicationGroup(curr, null /* delete group */) == null /* group deleted */) {
                        return;
                    } else {
                        // retry
                        continue;
                    }
                }

                // Make a copy of the replication group to update
                DynamoDBReplicationGroup groupToUpdate = new DynamoDBReplicationGroup(curr);

                // priority queue of members in DELETING or BOOTSTRAP_CANCELLED state
                PriorityQueue<DynamoDBReplicationGroupMember> deletingQueue = new PriorityQueue<>(groupToUpdate.getReplicationGroupMembers().size(),
                    DynamoDBReplicationUtilities.CREATING_PRIORITY_GROUP_MEMBER_COMPARATOR);

                // Search for a deleting replication group member
                DynamoDBReplicationGroupMember deletingMember = null;
                for (DynamoDBReplicationGroupMember member : groupToUpdate.getReplicationGroupMembers().values()) {
                    if (!member.getReplicationGroupMemberStatus().equals(DynamoDBReplicationGroupMemberStatus.DELETE_FAILED)) {
                        deletingQueue.add(member);
                    }
                }

                // pick first member to delete
                deletingMember = deletingQueue.peek();

                // If no deleting member found, then all replication group members left are in DELETE_FAILED state
                if (deletingMember == null) {
                    return;
                }

                // Delete replication group member
                try {
                    if (deletingMember.getTableCopyTask() != null
                        && DynamoDBReplicationUtilities.checkIfTableCopyServiceExists(awsAccess, deletingMember, deletingMember.getTableCopyTask())) {
                        DynamoDBReplicationGroupMemberStatus oldMStatus = oldGroup.getReplicationGroupMembers().get(deletingMember.getArn())
                            .getReplicationGroupMemberStatus();
                        if (oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.BOOTSTRAPPING)
                            || oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.WAITING)) {
                            return; // nothing to process while waiting for the bootstrapping process to terminate and transition to BOOTSTRAP_CANCELLED
                        } else {
                            // delete table copy resources
                            deleteMemberResources(awsAccess, deletingMember, true /* delete table copy */, false /* no need to delete connectors */);
                        }
                    }

                    // delete EC2 autoscaling group for connectors
                    if (deletingMember.getConnectors() != null && !deletingMember.getConnectors().isEmpty()) {
                        LOGGER.info("Deleting connector resources for replication member with ARN: " + deletingMember.getArn());
                        for (DynamoDBConnectorDescription connector : deletingMember.getConnectors()) {
                            DynamoDBReplicationUtilities.deleteConnectorService(awsAccess, deletingMember, connector);
                        }
                    }

                    // delete the replication group member from the replication group
                    groupToUpdate.removeReplicationGroupMember(deletingMember.getArn());
                } catch (Exception e) {
                    LOGGER.error("Deletion of replication member with ARN " + deletingMember.getArn() + " failed! Exception: " + e);
                    groupToUpdate.getReplicationGroupMembers().get(deletingMember.getArn())
                        .setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.DELETE_FAILED);
                }

                // write the updated replication group to metadata table
                if (groupToUpdate.equals(metadata.compareAndWriteReplicationGroup(curr, groupToUpdate))) {
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

    private void deleteMemberResources(AccountMapToAwsAccess awsAccess, DynamoDBReplicationGroupMember deletingMember, boolean deleteTableCopyResources,
        boolean deleteConnectorResources) throws Exception {
        if (deleteTableCopyResources) {
            DynamoDBReplicationUtilities.deleteTableCopyService(awsAccess, deletingMember);
        }

        if (deleteConnectorResources) {
            // delete ECS resources for connectors
            if (deletingMember.getConnectors() != null && !deletingMember.getConnectors().isEmpty()) {
                LOGGER.info("Deleting connector resources for replication member with ARN: " + deletingMember.getArn());
                for (DynamoDBConnectorDescription connector : deletingMember.getConnectors()) {
                    DynamoDBReplicationUtilities.deleteConnectorService(awsAccess, deletingMember, connector);
                }
            }
        }
    }

    @Override
    public void validateGroup(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        DynamoDBReplicationCoordinator.coordinatorAssert(newGroup.getReplicationGroupStatus().equals(DynamoDBReplicationGroupStatus.DELETING),
            INVALID_REPLICATION_GROUP_STATUS);
        for (DynamoDBReplicationGroupMember member : newGroup.getReplicationGroupMembers().values()) {
            DynamoDBReplicationGroupMemberStatus memberStatus = member.getReplicationGroupMemberStatus();
            DynamoDBReplicationCoordinator.coordinatorAssert(
                memberStatus.equals(DynamoDBReplicationGroupMemberStatus.DELETING)
                    || memberStatus.equals(DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_CANCELLED)
                    || memberStatus.equals(DynamoDBReplicationGroupMemberStatus.DELETE_FAILED), ALL_REPLICATION_GROUP_MEMBERS_MUST_BE_DELETING);
        }
    }
}
