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

import com.amazonaws.services.dynamodbv2.model.DynamoDBConnectorDescription;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMember;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMemberStatus;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupStatus;
import com.amazonaws.services.dynamodbv2.replication.AccountMapToAwsAccess;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBMetadataStorage;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationUtilities;
import com.amazonaws.services.dynamodbv2.replication.MetadataStorage;

/**
 * Represents a replication group member transition where the action is performed on a member based on priority, hence the action may not be applied to the
 * member that contains the changing status.
 *
 * For the following transitions:
 * <ul>
 * <li>Replication Group Member Creation Started</li>
 * <li>Replication Group Member Creation Queued</li>
 * <li>Replication Group Member Bootstrap Started</li>
 * <li>Replication Group Member Bootstrap Failed</li>
 * <li>Replication Group Member Replication Started</li>
 * <li>Replication Group Member Update Completed</li>
 * </ul>
 *
 */
public class DynamoDBReplicationGroupMemberPriorityTransition extends DynamoDBReplicationGroupMemberTransition {

    private static final Logger LOGGER = Logger.getLogger(DynamoDBReplicationGroupMemberPriorityTransition.class);

    /**
     * Default constructor
     *
     * @param oldGroup
     *            old image of the replication group
     * @param newGroup
     *            new image of the replication group
     * @param oldM
     *            old image of the replication group member that changes status
     * @param newM
     *            new image of the replication group member that changes status
     */
    public DynamoDBReplicationGroupMemberPriorityTransition(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup,
        DynamoDBReplicationGroupMember oldM, DynamoDBReplicationGroupMember newM) {
        super(oldGroup, newGroup, oldM, newM);
    }

    @Override
    public void transition(MetadataStorage metadata, AccountMapToAwsAccess awsAccess) {
        while (true) {
            try {
                // read current image of replication group
                DynamoDBReplicationGroup curr = metadata.readReplicationGroup(oldGroup.getReplicationGroupUUID());

                // if group no longer exists or group is already active or group is being deleted, do nothing
                if (null == curr || curr.getReplicationGroupStatus().equals(DynamoDBReplicationGroupStatus.ACTIVE)
                    || curr.getReplicationGroupStatus().equals(DynamoDBReplicationGroupStatus.DELETING)) {
                    return;
                }

                // priority queue of members in CREATING state
                PriorityQueue<DynamoDBReplicationGroupMember> creatingQueue = new PriorityQueue<>(newGroup.getReplicationGroupMembers().size(),
                    DynamoDBReplicationUtilities.CREATING_PRIORITY_GROUP_MEMBER_COMPARATOR);

                // priority queue of members in WAITING state
                PriorityQueue<DynamoDBReplicationGroupMember> waitingQueue = new PriorityQueue<>(newGroup.getReplicationGroupMembers().size(),
                    DynamoDBReplicationUtilities.CREATING_PRIORITY_GROUP_MEMBER_COMPARATOR);

                // keep track of whether there is a bootstrapping member
                boolean bootstrappingMemberExists = false;

                // keep track of number of active members
                int numActiveMembers = 0;

                // make a copy of the current replication group
                DynamoDBReplicationGroup groupToUpdate = new DynamoDBReplicationGroup(curr);

                // populate creating, waiting queues; find bootstrapping task; tally ACTIVE members
                for (DynamoDBReplicationGroupMember member : curr.getReplicationGroupMembers().values()) {
                    switch (member.getReplicationGroupMemberStatus()) {
                        case ACTIVE:
                            numActiveMembers++;
                            continue;
                        case CREATING:
                            creatingQueue.add(member);
                            continue;
                        case WAITING:
                            waitingQueue.add(member);
                            continue;
                        case BOOTSTRAPPING:
                            bootstrappingMemberExists = true;
                            continue;
                        case BOOTSTRAP_FAILED:
                        case CREATE_FAILED:
                        case DELETE_FAILED:
                        case UPDATE_FAILED:
                            continue;
                        default:
                            break;
                    }
                }

                // set group status to ACTIVE if applicable
                if (curr.getReplicationGroupMembers().isEmpty() || numActiveMembers == curr.getReplicationGroupMembers().size()) {
                    groupToUpdate.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.ACTIVE);
                } else {
                    if (VALID_FAILURE_GROUP_MEMBER_STATUS.contains(newM.getReplicationGroupMemberStatus())) {
                        LOGGER.error("Replication member with ARN: " + newM.getArn() + " is in FAILED status!");
                        if (newM.getReplicationGroupMemberStatus().equals(DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_FAILED)) {
                            try {
                                // remove the table copy service since bootstrapping failed
                                DynamoDBReplicationUtilities.deleteTableCopyService(awsAccess, newM);
                            } catch (Exception e) {
                                LOGGER.error("Could not clean up table copy service for replication member with ARN: " + newM.getArn());
                            }
                        }
                    }

                    // check if there are members in WAITING status
                    if (!waitingQueue.isEmpty()) {
                        DynamoDBReplicationGroupMember waitingMember = waitingQueue.peek();
                        try {
                            // if there is no bootstrapping member and member has a bootstrap task, proceed to bootstrap member
                            if (waitingMember.getTableCopyTask() != null && !bootstrappingMemberExists) {
                                // launch bootstrap task
                                LOGGER.info("Launching bootstrap task for replication member with ARN: " + waitingMember.getArn());
                                if (metadata instanceof DynamoDBMetadataStorage) {
                                    DynamoDBReplicationUtilities.launchTableCopyService(awsAccess, groupToUpdate, waitingMember,
                                        (DynamoDBMetadataStorage) metadata);
                                } else {
                                    LOGGER.error("Cannot launch table copy task with unsupported metadata storage type!");
                                    throw new IllegalArgumentException();
                                }
                                groupToUpdate.getReplicationGroupMembers().get(waitingMember.getArn())
                                    .setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.BOOTSTRAPPING);
                            } else if (waitingMember.getTableCopyTask() == null) { // no bootstrap task, proceed to move member to ACTIVE
                                if (waitingMember.getConnectors() != null && !waitingMember.getConnectors().isEmpty()) {
                                    LOGGER.info("Launching connector resources for replication member with ARN: " + waitingMember.getArn());
                                    // launch connector resource
                                    for (DynamoDBConnectorDescription connector : waitingMember.getConnectors()) {
                                        DynamoDBReplicationUtilities.launchConnectorService(awsAccess, groupToUpdate, waitingMember, connector);
                                    }
                                }
                                groupToUpdate.getReplicationGroupMembers().get(waitingMember.getArn())
                                    .setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.ACTIVE);
                            }
                        } catch (Exception e) {
                            LOGGER.error("Launching resources failed for replication member with ARN: " + waitingMember.getArn() + "! Exception: " + e);
                            groupToUpdate.getReplicationGroupMembers().get(waitingMember.getArn())
                                .setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.CREATE_FAILED);
                        }
                    }

                    // if we haven't made any changes to the replication group, check if there are members in CREATING status
                    if (groupToUpdate.equals(curr) && !creatingQueue.isEmpty()) {
                        DynamoDBReplicationGroupMember creatingMember = creatingQueue.peek();
                        try {
                            DynamoDBReplicationUtilities.createTableIfNotExists(curr, creatingMember, awsAccess);
                            LOGGER.info("Created table for replication member with ARN: " + creatingMember.getArn());

                            DynamoDBReplicationGroupMember updatedCreatingMember = groupToUpdate.getReplicationGroupMembers().get(creatingMember.getArn());
                            updatedCreatingMember.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.WAITING);

                            // clean up the attributes that are no longer needed after table has been created
                            updatedCreatingMember.setGlobalSecondaryIndexes(null);
                            updatedCreatingMember.setLocalSecondaryIndexes(null);
                            updatedCreatingMember.setProvisionedThroughput(null);
                        } catch (Exception e) {
                            LOGGER.error("Creation of table failed for replication member with ARN: " + creatingMember.getArn() + "! Exception: " + e);
                            groupToUpdate.getReplicationGroupMembers().get(creatingMember.getArn())
                                .setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.CREATE_FAILED);
                        }
                    }
                }
                if (groupToUpdate.equals(curr)) {
                    return; // nothing to process
                } else if (groupToUpdate.equals(metadata.compareAndWriteReplicationGroup(curr, groupToUpdate))) {
                    return;
                } else {
                    // retry
                }
            } catch (IOException e) {
                coordinatorFail(FAILED_TO_ACCESS_METADATA + " with exception: " + e);
            }
        }
    }

    @Override
    public void validateGroup(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        // the replication group should be in either CREATING or UPDATING state
        coordinatorAssert(oldGroup != null && newGroup != null, INVALID_REPLICATION_GROUP);
        DynamoDBReplicationGroupStatus replicationGroupStatus = newGroup.getReplicationGroupStatus();
        coordinatorAssert(
            replicationGroupStatus.equals(DynamoDBReplicationGroupStatus.CREATING) || replicationGroupStatus.equals(DynamoDBReplicationGroupStatus.UPDATING),
            INVALID_REPLICATION_GROUP_STATUS + " replication must be either CREATING or UPDATING.");
    }

    @Override
    public void validateGroupMember(DynamoDBReplicationGroupMember oldM, DynamoDBReplicationGroupMember newM) {
        coordinatorAssert(null != newM, INVALID_REPLICATION_GROUP_MEMBER_TRANSITION + " replication group member cannot be null.");
        if (null != oldM) {
            DynamoDBReplicationGroupMemberStatus oldMStatus = oldM.getReplicationGroupMemberStatus();
            DynamoDBReplicationGroupMemberStatus newMStatus = newM.getReplicationGroupMemberStatus();
            switch (newMStatus) {
                case WAITING:
                    coordinatorAssert(oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.CREATING), INVALID_REPLICATION_GROUP_MEMBER_TRANSITION
                        + " must transition to WAITING from CREATING state");
                    break;
                case BOOTSTRAPPING:
                    coordinatorAssert(
                        oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.WAITING)
                            || oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_FAILED), INVALID_REPLICATION_GROUP_MEMBER_TRANSITION
                            + " must transition to BOOTSTRAPPING from either WAITING or BOOTSTRAP_FAILED state.");
                    break;
                case BOOTSTRAP_FAILED:
                    coordinatorAssert(oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.BOOTSTRAPPING), INVALID_REPLICATION_GROUP_MEMBER_TRANSITION
                        + " must transition to BOOTSTRAP_FAILED from BOOTSTRAPPING state.");
                    break;
                case ACTIVE:
                    coordinatorAssert(
                        oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.WAITING)
                            || oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_COMPLETE)
                            || oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.UPDATING), INVALID_REPLICATION_GROUP_MEMBER_TRANSITION
                            + " must transition to ACTIVE from either WAITING, BOOTSTRAP_COMPLETE, or UPDATING state.");
                    break;
                case CREATE_FAILED:
                    coordinatorAssert(
                        oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.CREATING) || oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.WAITING),
                        INVALID_REPLICATION_GROUP_MEMBER_TRANSITION + " must transition to CREATE_FAILED from CREATING or WAITING state.");
                    break;
                case UPDATE_FAILED:
                    coordinatorAssert(oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.UPDATING), INVALID_REPLICATION_GROUP_MEMBER_TRANSITION
                        + " must transition to UPDATE_FAILED from UPDATING state.");
                    break;
                case DELETE_FAILED:
                    coordinatorAssert(oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.DELETING), INVALID_REPLICATION_GROUP_MEMBER_TRANSITION
                        + " must transition to DELETE_FAILED from DELETING state.");
                    break;
                default:
                    coordinatorFail(INVALID_REPLICATION_GROUP_MEMBER_TRANSITION);
            }
        } else {
            coordinatorAssert(newM.getReplicationGroupMemberStatus().equals(DynamoDBReplicationGroupMemberStatus.CREATING),
                INVALID_REPLICATION_GROUP_MEMBER_TRANSITION + " replication member must be in CREATING state when it is first added to the replication group.");
        }
    }
}
