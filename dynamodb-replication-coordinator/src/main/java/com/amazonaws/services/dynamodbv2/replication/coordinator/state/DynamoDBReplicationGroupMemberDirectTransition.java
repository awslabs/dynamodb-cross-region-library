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
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.DynamoDBConnectorDescription;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMember;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMemberStatus;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupStatus;
import com.amazonaws.services.dynamodbv2.replication.AccountMapToAwsAccess;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationUtilities;
import com.amazonaws.services.dynamodbv2.replication.MetadataStorage;

/**
 * Represents a replication group member transition where the action is performed on the member that contains the changing status.
 *
 * For the following transitions:
 * <ul>
 * <li>Replication Group Member Creation Cancelled</li>
 * <li>Replication Group Member Bootstrap Cancelled</li>
 * <li>Replication Group Member Bootstrap Completed</li>
 * <li>Replication Group Member Deletion Started</li>
 * <li>Replication Group Member Deletion Completed</li>
 * <li>Replication Group Member Update Started</li>
 * </ul>
 *
 */
public class DynamoDBReplicationGroupMemberDirectTransition extends DynamoDBReplicationGroupMemberTransition {

    private static final Logger LOGGER = Logger.getLogger(DynamoDBReplicationGroupMemberDirectTransition.class);

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
    public DynamoDBReplicationGroupMemberDirectTransition(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup,
        DynamoDBReplicationGroupMember oldM, DynamoDBReplicationGroupMember newM) {
        super(oldGroup, newGroup, oldM, newM);
    }

    @Override
    public void transition(MetadataStorage metadata, AccountMapToAwsAccess awsAccess) {
        while (true) {
            try {
                // Get current image of the replication group
                DynamoDBReplicationGroup curr = metadata.readReplicationGroup(oldGroup.getReplicationGroupUUID());

                // if group no longer exists or group is already active or group is being deleted, do nothing
                if (null == curr || curr.getReplicationGroupStatus().equals(DynamoDBReplicationGroupStatus.ACTIVE)
                    || curr.getReplicationGroupStatus().equals(DynamoDBReplicationGroupStatus.DELETING)) {
                    return;
                }

                // try to update the replication group
                DynamoDBReplicationGroup groupToUpdate = new DynamoDBReplicationGroup(curr);

                if (null != newM) { // modification to existing replication group member
                    DynamoDBReplicationGroupMember memberToUpdate = groupToUpdate.getReplicationGroupMembers().get(newM.getArn());
                    DynamoDBReplicationGroupMemberStatus currStatus = memberToUpdate.getReplicationGroupMemberStatus();

                    // validate the transition by making sure the replication group member is still in the state we are processing
                    if (currStatus != newM.getReplicationGroupMemberStatus()) {
                        return; // replication group member has changed status since we started processing, this transition is no longer valid
                    }

                    switch (newM.getReplicationGroupMemberStatus()) {
                        case BOOTSTRAP_CANCELLED:
                            try {
                                DynamoDBReplicationUtilities.deleteTableCopyService(awsAccess, memberToUpdate);

                                // delete the replication group member from the replication group
                                groupToUpdate.removeReplicationGroupMember(memberToUpdate.getArn());
                            } catch (Exception e) {
                                LOGGER.error("Deleting table copy service failed for replication member with ARN: " + memberToUpdate.getArn());
                            }
                            break;
                        case BOOTSTRAP_COMPLETE:
                            try {
                                // remove the table copy service now that bootstrapping is completed
                                DynamoDBReplicationUtilities.deleteTableCopyService(awsAccess, memberToUpdate);

                                // launch resources for connectors
                                if (memberToUpdate.getConnectors() != null && !memberToUpdate.getConnectors().isEmpty()) {
                                    LOGGER.info("Launching connector resources for replication member with ARN: " + memberToUpdate.getArn());
                                    for (DynamoDBConnectorDescription connector : memberToUpdate.getConnectors()) {
                                        DynamoDBReplicationUtilities.launchConnectorService(awsAccess, groupToUpdate, memberToUpdate, connector);
                                    }
                                }

                                // set replication group member status to ACTIVE
                                memberToUpdate.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.ACTIVE);
                            } catch (Exception e) {
                                // log error and set replication group member status to CREATE_FAILED
                                LOGGER.error("Launching connector resources failed for replication member with ARN: " + memberToUpdate.getArn()
                                    + "! Exception: " + e);
                                memberToUpdate.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.CREATE_FAILED);
                            }
                            break;
                        case DELETING:
                            try {
                                if (memberToUpdate.getTableCopyTask() != null
                                    && DynamoDBReplicationUtilities.checkIfTableCopyServiceExists(awsAccess, memberToUpdate, memberToUpdate.getTableCopyTask())) {
                                    return; // nothing to process while waiting for the bootstrapping process to terminate and transition to BOOTSTRAP
                                            // CANCELLED
                                }

                                // delete EC2 autoscaling group for connectors
                                if (memberToUpdate.getConnectors() != null && !memberToUpdate.getConnectors().isEmpty()) {
                                    LOGGER.info("Deleting connector resources for replication member with ARN: " + memberToUpdate.getArn());
                                    for (DynamoDBConnectorDescription connector : memberToUpdate.getConnectors()) {
                                        DynamoDBReplicationUtilities.deleteConnectorService(awsAccess, memberToUpdate, connector);
                                    }
                                }

                                // delete the replication group member from the replication group
                                groupToUpdate.removeReplicationGroupMember(memberToUpdate.getArn());
                            } catch (Exception e) {
                                // log error and set replication group member status to DELETE_FAILED
                                LOGGER.error("Deleting connector resources failed for replication member with ARN: " + memberToUpdate.getArn()
                                    + "! Exception: " + e);
                                memberToUpdate.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.DELETE_FAILED);
                            }
                            break;
                        case UPDATING:
                            try {
                                // evaluate connectors difference
                                final Collection<DynamoDBConnectorDescription> addedConnectors = new ArrayList<DynamoDBConnectorDescription>();
                                final Collection<DynamoDBConnectorDescription> removedConnectors = new ArrayList<DynamoDBConnectorDescription>();
                                evaluateConnectorsDifference(oldM, memberToUpdate, addedConnectors, removedConnectors);

                                // launch resources for new connectors
                                LOGGER.info("Launching resources for added connectors for replication member with ARN: " + memberToUpdate.getArn());
                                for (DynamoDBConnectorDescription addedConnector : addedConnectors) {
                                    DynamoDBReplicationUtilities.launchConnectorService(awsAccess, groupToUpdate, memberToUpdate, addedConnector);
                                }

                                // clean up resources for old connectors
                                LOGGER.info("Deleting resources for removed connectors for replication member with ARN: " + memberToUpdate.getArn());
                                for (DynamoDBConnectorDescription removedConnector : removedConnectors) {
                                    DynamoDBReplicationUtilities.deleteConnectorService(awsAccess, memberToUpdate, removedConnector);
                                }

                                // once finished, set replication group member status to ACTIVE
                                memberToUpdate.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.ACTIVE);
                            } catch (Exception e) {
                                LOGGER.error("Updating connector resources failed for replication member with ARN: " + memberToUpdate.getArn()
                                    + "! Exception: " + e);
                                memberToUpdate.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.UPDATE_FAILED);
                            }
                            break;
                        default:
                            coordinatorFail(INVALID_REPLICATION_GROUP_MEMBER_TRANSITION);
                    }
                } else { // removed replication group member
                    // check if all replication group members are in ACTIVE, if so set group status to ACTIVE
                    int numActiveMembers = 0;
                    for (DynamoDBReplicationGroupMember member : curr.getReplicationGroupMembers().values()) {
                        if (member.getReplicationGroupMemberStatus().equals(DynamoDBReplicationGroupMemberStatus.ACTIVE)) {
                            numActiveMembers++;
                        }
                    }
                    if (curr.getReplicationGroupMembers().isEmpty() || numActiveMembers == curr.getReplicationGroupMembers().size()) {
                        groupToUpdate.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.ACTIVE);
                    } else {
                        // not all replication group members are active, nothing to do here
                        return;
                    }
                }

                // try to write the changes
                if (groupToUpdate.equals(metadata.compareAndWriteReplicationGroup(curr, groupToUpdate))) {
                    return;
                }
                // else retry
            } catch (IOException e) {
                coordinatorFail(FAILED_TO_ACCESS_METADATA + " with exception: " + e);
            }
        }
    }

    private void evaluateConnectorsDifference(DynamoDBReplicationGroupMember oldM, DynamoDBReplicationGroupMember newM,
        Collection<DynamoDBConnectorDescription> addedConnectors, Collection<DynamoDBConnectorDescription> removedConnectors) throws Exception {
        final Collection<DynamoDBConnectorDescription> oldConnectors = oldM.getConnectors();
        final Collection<DynamoDBConnectorDescription> newConnectors = newM.getConnectors();

        // we do not support any other updates apart from connector changes
        if (oldConnectors.equals(newConnectors)) {
            coordinatorFail(INVALID_REPLICATION_GROUP_MEMBER_TRANSITION + " since a replication member was updated without any changes in the connectors.");
        }

        addedConnectors.addAll(newConnectors);
        addedConnectors.removeAll(oldConnectors);
        removedConnectors.addAll(oldConnectors);
        removedConnectors.removeAll(newConnectors);
    }

    @Override
    public void validateGroup(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        // the replication group should be in either CREATING or UPDATING state
        coordinatorAssert(oldGroup != null && newGroup != null, INVALID_REPLICATION_GROUP);
        DynamoDBReplicationGroupStatus replicationGroupStatus = newGroup.getReplicationGroupStatus();
        coordinatorAssert(
            replicationGroupStatus.equals(DynamoDBReplicationGroupStatus.CREATING) || replicationGroupStatus.equals(DynamoDBReplicationGroupStatus.UPDATING),
            INVALID_REPLICATION_GROUP_STATUS);
    }

    @Override
    public void validateGroupMember(DynamoDBReplicationGroupMember oldM, DynamoDBReplicationGroupMember newM) {
        coordinatorAssert(null != oldM, INVALID_REPLICATION_GROUP_MEMBER_TRANSITION);
        if (null != newM) {
            coordinatorAssert(oldM.getArn().equals(newM.getArn()), MEMBER_TRANSITION_MUST_BE_ONE_MEMBER);
            DynamoDBReplicationGroupMemberStatus oldMStatus = oldM.getReplicationGroupMemberStatus();
            DynamoDBReplicationGroupMemberStatus newMStatus = newM.getReplicationGroupMemberStatus();
            switch (newMStatus) {
                case DELETING:
                    break;
                case BOOTSTRAP_CANCELLED:
                    coordinatorAssert(
                        oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.BOOTSTRAPPING)
                            || oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.DELETING)
                            || oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.WAITING), INVALID_REPLICATION_GROUP_MEMBER_TRANSITION
                            + " replication member must transition to BOOTSTRAP_CANCELLED from BOOTSTRAPPING, DELETING or WAITING.");
                    break;
                case BOOTSTRAP_COMPLETE:
                    coordinatorAssert(
                        oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.BOOTSTRAPPING)
                            || oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.WAITING), INVALID_REPLICATION_GROUP_MEMBER_TRANSITION
                            + " replication member must transition to BOOTSTRAP_COMPLETE from BOOTSTRAPPING or WAITING.");
                    break;
                case UPDATING:
                    coordinatorAssert(oldMStatus.equals(DynamoDBReplicationGroupMemberStatus.ACTIVE), INVALID_REPLICATION_GROUP_MEMBER_TRANSITION
                        + " replication member must transition to UPDATING from ACTIVE.");
                    break;
                default:
                    coordinatorFail(INVALID_REPLICATION_GROUP_MEMBER_TRANSITION);
            }
        } else {
            coordinatorAssert(oldM.getReplicationGroupMemberStatus().equals(DynamoDBReplicationGroupMemberStatus.DELETING)
                || oldM.getReplicationGroupMemberStatus().equals(DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_CANCELLED),
                INVALID_REPLICATION_GROUP_MEMBER_TRANSITION);
        }
    }
}
