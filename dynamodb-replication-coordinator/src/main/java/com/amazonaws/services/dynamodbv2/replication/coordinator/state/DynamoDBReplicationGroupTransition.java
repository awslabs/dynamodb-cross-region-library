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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMember;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMemberStatus;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupStatus;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.replication.AccountMapToAwsAccess;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationUtilities;
import com.amazonaws.services.dynamodbv2.replication.MetadataStorage;

/**
 * Abtract class representing a replication group transition from one state to the next, triggered by a status change in any of the members of the replication
 * group or in the replication group itself.
 */
public abstract class DynamoDBReplicationGroupTransition {

    private static final Logger LOGGER = Logger.getLogger(DynamoDBReplicationGroupTransition.class);

    /*
     * old image of the replication group
     */
    protected final DynamoDBReplicationGroup oldGroup;

    /*
     * new image of the replication group
     */
    protected final DynamoDBReplicationGroup newGroup;

    /**
     * Default constructor
     *
     * @param oldGroup
     *            old image of the replication group
     * @param newGroup
     *            new image of the replication group
     */
    public DynamoDBReplicationGroupTransition(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        validateGroup(oldGroup, newGroup);
        this.oldGroup = oldGroup == null ? null : new DynamoDBReplicationGroup(oldGroup);
        this.newGroup = newGroup == null ? null : new DynamoDBReplicationGroup(newGroup);
    }

    /**
     * Executes all steps to necessary to move to the next state. Uses DynamoDB optimistic locking model by reading the current version from the metadata
     * storage and making the transition only if it is still valid. If the metadata changes between the read and write, the process is repeated. This is an
     * idempotent operation.
     *
     * @param metadata
     *            Metadata storage with access to the metadata table
     * @param awsAccess
     *            AWS access providing clients to tables of different regions etc.
     */
    public abstract void transition(MetadataStorage metadata, AccountMapToAwsAccess awsAccess);

    /**
     * Validates the transition.
     *
     * @param oldGroup
     *            Old image of the replication group
     * @param newGroup
     *            New image of the replication group
     * @throws IllegalStateException
     *             Illegal transition detected
     */
    public abstract void validateGroup(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup);

    /*
     * Error messages to log
     */
    public static final String ALL_REPLICATION_GROUP_MEMBERS_MUST_BE_ACTIVE = "All replication group members must be ACTIVE for a replication group to be ACTIVE";
    public static final String ALL_REPLICATION_GROUP_MEMBERS_MUST_BE_CREATING = "All replication group members must be CREATING when creating a replication group";
    public static final String ALL_REPLICATION_GROUP_MEMBERS_MUST_BE_DELETING = "All replication group members must be DELETING for a replication group to be DELETING";
    public static final String ATTR_DEFN_CANNOT_BE_MODIFIED = "Attribute definition of a replication group cannot be modified";
    public static final String CONNECTOR_TYPE_CANNOT_BE_MODIFIED = "Connector type of a replication group cannot be modified";
    public static final String FAILED_TO_ACCESS_METADATA = "Could not access metadata";
    public static final String INVALID_COMMON_MEMBER_SET = "Invalid common members across old and new images of replication group.";
    public static final String INVALID_REPLICATION_GROUP_MEMBER_TRANSITION = "Invalid replication group member transition";
    public static final String INVALID_REPLICATION_GROUP_MEMBER_TRANSITION_FROM_CREATING = "Invalid replication group member transition from CREATING";
    public static final String INVALID_REPLICAITON_GROUP_MEMBER_TRANSITION_FROM_WAITING = "Invalid replicaiton group member transition from WAITING";
    public static final String INVALID_REPLICATION_GROUP_MEMBER_TRANSITION_FROM_ACTIVE = "Invalid replication group member transition from ACTIVE";
    public static final String INVALID_REPLICATION_GROUP_MEMBER_TRANSITION_FROM_BOOTSTRAP_COMPLETE = "Invalid replication group member transition from BOOTSTRAP_COMPLETE";
    public static final String INVALID_REPLICATION_GROUP_MEMBER_TRANSITION_FROM_BOOTSTRAP_FAILED = "Invalid replication group member transition from bootstrap failed";
    public static final String INVALID_REPLICATION_GROUP_MEMBER_TRANSITION_FROM_BOOTSTRAPPING = "Invalid replication group member transition from BOOTSTRAPPING";
    public static final String INVALID_REPLICATION_GROUP_MEMBER_TRANSITION_FROM_DELETING = "A DELETING replication group member cannot transition to another status";
    public static final String INVALID_REPLICATION_GROUP_MEMBER_TRANSITION_FROM_UPDATING = "Invalid replication group member transition from UPDATING";
    public static final String INVALID_REPLICATION_GROUP = "Invalid replication group";
    public static final String INVALID_REPLICATION_GROUP_MEMBER_STATUS = "Invalid replication group member status";
    public static final String INVALID_REPLICATION_GROUP_STATUS = "Invalid replication group status";
    public static final String INVALID_REPLICATION_GROUP_TRANSITION_FROM_DELETING = "Invalid replication group transition from DELETING";
    public static final String INVALID_STREAM_RECORD = "Stream record cannot be null";
    public static final String KEY_SCHEMA_CANNOT_BE_MODIFIED = "Key Schema cannot be modified";
    public static final String MEMBER_TRANSITION_MUST_BE_ONE_MEMBER = "Member transition must be on the same replication member of a group";
    public static final String NEW_IMAGE_IS_NOT_VALID = "New image is not valid";
    public static final String NEW_GROUP_NONNULL_DELETED = "New replication group must be null if an old group has been deleted";
    public static final String NO_DIFFERENCE_BETWEEN_REPLICATION_GROUP_IMAGES = "No difference between replication group images";
    public static final String OLD_AND_NEW_IMAGES_MUST_NOT_BOTH_BE_NULL = "Old and new images must not both be null";
    public static final String OLD_GROUP_NONNULL_CREATING = "Old replication group must be null if a new group is being created";
    public static final String OLD_IMAGE_IS_NOT_VALID = "Old image is not valid";
    public static final String ONLY_ONE_REPLICATION_GROUP_MEMBER_MODIFIED = "Only one replication group member may be modified per metadata update";
    public static final String REPLICATION_GROUP_UUIDS_MUST_BE_EQUAL = "Replication group UUIDs must be equal";
    public static final String REPLICATION_GROUP_STATUS_CANNOT_TRANSITION_TO_UPDATING = "Replication group status cannot transition to UPDATING without a CREATING, DELETING, or UPDATING replication group member";
    public static final String UNRECOGNIZED_REPLICATION_GROUP_MEMBER_CHANGE = "Unrecognized replication group member change";
    public static final String UNSUPPORTED_GROUP_IMAGE_DIFFERENCE = "Unsupported group image difference";

    /*
     * Valid updating group member statuses
     */
    public static final Set<DynamoDBReplicationGroupMemberStatus> VALID_UPDATING_GROUP_MEMBER_STATUS = new HashSet<DynamoDBReplicationGroupMemberStatus>();
    static {
        VALID_UPDATING_GROUP_MEMBER_STATUS.add(DynamoDBReplicationGroupMemberStatus.CREATING);
        VALID_UPDATING_GROUP_MEMBER_STATUS.add(DynamoDBReplicationGroupMemberStatus.UPDATING);
        VALID_UPDATING_GROUP_MEMBER_STATUS.add(DynamoDBReplicationGroupMemberStatus.DELETING);
    }

    /*
     * All group member failure statuses
     */
    public static final Set<DynamoDBReplicationGroupMemberStatus> VALID_FAILURE_GROUP_MEMBER_STATUS = new HashSet<DynamoDBReplicationGroupMemberStatus>();
    static {
        VALID_FAILURE_GROUP_MEMBER_STATUS.add(DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_FAILED);
        VALID_FAILURE_GROUP_MEMBER_STATUS.add(DynamoDBReplicationGroupMemberStatus.CREATE_FAILED);
        VALID_FAILURE_GROUP_MEMBER_STATUS.add(DynamoDBReplicationGroupMemberStatus.DELETE_FAILED);
        VALID_FAILURE_GROUP_MEMBER_STATUS.add(DynamoDBReplicationGroupMemberStatus.UPDATE_FAILED);
    }

    /**
     * Detects the event change given a specific Streams record.
     *
     * @param streamRecord
     *            The given DynamoDB Streams record that reflects a metadata change
     * @return The transition class representing the state change
     */
    public static DynamoDBReplicationGroupTransition getTransition(StreamRecord streamRecord) {
        if (streamRecord != null) {
            DynamoDBReplicationGroup oldGroup = null;
            DynamoDBReplicationGroup newGroup = null;
            if (null != streamRecord.getOldImage()) {
                oldGroup = DynamoDBReplicationUtilities.ITEM_CONVERTER.unconvert(DynamoDBReplicationGroup.class, streamRecord.getOldImage());
            }
            if (null != streamRecord.getNewImage()) {
                newGroup = DynamoDBReplicationUtilities.ITEM_CONVERTER.unconvert(DynamoDBReplicationGroup.class, streamRecord.getNewImage());
            }
            return evaluateGroupDiff(oldGroup, newGroup);
        } else {
            coordinatorFail(INVALID_STREAM_RECORD);
            throw new IllegalArgumentException(); // Unreachable - for safety
        }
    }

    /**
     * Evaluates the difference between two replication groups, and invokes the appropriate methods to evaluate group member differences as needed.
     *
     * @param oldGroup
     *            The old image of the replication group
     * @param newGroup
     *            The new image of the replication group
     * @return The detected transition corresponding to the change in the replication group
     */
    private static DynamoDBReplicationGroupTransition evaluateGroupDiff(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        // Validate the old and new image of the replication group
        coordinatorAssert(null == newGroup || newGroup.isValid(), NEW_IMAGE_IS_NOT_VALID);
        coordinatorAssert(null == oldGroup || oldGroup.isValid(), OLD_IMAGE_IS_NOT_VALID);
        coordinatorAssert(null != oldGroup || null != newGroup, OLD_AND_NEW_IMAGES_MUST_NOT_BOTH_BE_NULL);

        if (null == oldGroup) { // Insert
            LOGGER.info("Creating new replication group with name: " + newGroup.getReplicationGroupName() + " and UUID: " + newGroup.getReplicationGroupUUID()
                + ".");
            return new DynamoDBReplicationGroupCreationStarted(null /* oldGroup */, newGroup);
        } else if (null == newGroup) { // Remove
            LOGGER
                .info("Deleted replication group with name: " + oldGroup.getReplicationGroupName() + " and UUID: " + oldGroup.getReplicationGroupUUID() + ".");
            return new DynamoDBReplicationGroupDeletionCompleted(oldGroup, null /* newGroup */);
        } else { // Modify
            // Assert replication group names are the same
            coordinatorAssert(oldGroup.getReplicationGroupUUID().equals(newGroup.getReplicationGroupUUID()), REPLICATION_GROUP_UUIDS_MUST_BE_EQUAL);

            // Assert key schemas are the same
            coordinatorAssert(oldGroup.getKeySchema().equals(newGroup.getKeySchema()), KEY_SCHEMA_CANNOT_BE_MODIFIED);

            // Assert attribute definitions are the same
            coordinatorAssert(oldGroup.getAttributeDefinitions().equals(newGroup.getAttributeDefinitions()), ATTR_DEFN_CANNOT_BE_MODIFIED);

            // Assert connector type is the same
            coordinatorAssert(oldGroup.getConnectorType().equals(newGroup.getConnectorType()), CONNECTOR_TYPE_CANNOT_BE_MODIFIED);

            // Assert there has been a change
            coordinatorAssert(!oldGroup.equals(newGroup), NO_DIFFERENCE_BETWEEN_REPLICATION_GROUP_IMAGES);

            if (oldGroup.getReplicationGroupStatus().equals(newGroup.getReplicationGroupStatus())) {
                // Group has the same status
                return groupNoStatusChange(oldGroup, newGroup);
            } else { // Group has changed status
                switch (oldGroup.getReplicationGroupStatus()) {
                    case ACTIVE:
                        return groupTransitionFromActive(oldGroup, newGroup);
                    case CREATING:
                        return groupTransitionFromCreating(oldGroup, newGroup);
                    case DELETING:
                        // Cannot transition out of DELETING
                        coordinatorFail(INVALID_REPLICATION_GROUP_TRANSITION_FROM_DELETING);
                        throw new IllegalStateException(); // Unreachable - for safety
                    case UPDATING:
                        return groupTransitionFromUpdating(oldGroup, newGroup);
                    default:
                        coordinatorFail(INVALID_REPLICATION_GROUP_STATUS);
                        throw new IllegalStateException(); // Unreachable - for safety
                }
            }
        }
    }

    /**
     * Helper function evaluated when there is no replication group status change, meaning changes occurred with the attribute definition or replication group
     * members.
     *
     * @param oldGroup
     *            The old image of the replication group
     * @param newGroup
     *            The new image of the replication group
     * @return The detected transition in the replication group
     */
    private static DynamoDBReplicationGroupTransition groupNoStatusChange(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        // Group members have changed
        if (!oldGroup.getReplicationGroupMembers().equals(newGroup.getReplicationGroupMembers())) {
            return evaluateGroupMemberDiff(oldGroup, newGroup);
        } else { // Unsupported change
            coordinatorFail(UNSUPPORTED_GROUP_IMAGE_DIFFERENCE);
            throw new IllegalStateException(); // Unreachable - for safety
        }
    }

    /**
     * Helper function evaluating event change for a replication group where the starting state of the group is ACTIVE
     *
     * @param oldGroup
     *            The old image of the replication group
     * @param newGroup
     *            The new image of the replication group
     * @return The detected transition in the replication group
     */
    private static DynamoDBReplicationGroupTransition groupTransitionFromActive(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        coordinatorAssert(oldGroup.getReplicationGroupStatus().equals(DynamoDBReplicationGroupStatus.ACTIVE), INVALID_REPLICATION_GROUP_STATUS);
        switch (newGroup.getReplicationGroupStatus()) {
            case DELETING:
                LOGGER.info("Deleting replication group with name: " + newGroup.getReplicationGroupName() + " and UUID: " + newGroup.getReplicationGroupUUID()
                    + ".");
                return new DynamoDBReplicationGroupDeletionStarted(oldGroup, newGroup);
            case UPDATING:
                LOGGER.info("Updating replication group with name: " + newGroup.getReplicationGroupName() + " and UUID: " + newGroup.getReplicationGroupUUID()
                    + ".");
                return new DynamoDBReplicationGroupUpdateStarted(oldGroup, newGroup);
            default:
                coordinatorFail(INVALID_REPLICATION_GROUP_STATUS + " must transition to either UPDATING or DELETING from ACTIVE.");
                throw new IllegalStateException(); // Unreachable - for safety
        }
    }

    /**
     * Helper function evaluating event change for a replication group where the starting state of the group is CREATING
     *
     * @param oldGroup
     *            The old image of the replication group
     * @param newGroup
     *            The new image of the replication group
     * @return The detected transition in the replication group
     */
    private static DynamoDBReplicationGroupTransition groupTransitionFromCreating(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        coordinatorAssert(oldGroup.getReplicationGroupStatus().equals(DynamoDBReplicationGroupStatus.CREATING), INVALID_REPLICATION_GROUP_STATUS);
        switch (newGroup.getReplicationGroupStatus()) {
            case ACTIVE:
                LOGGER.info("Created replication group with name: " + newGroup.getReplicationGroupName() + " and UUID: " + newGroup.getReplicationGroupUUID()
                    + ".");
                return new DynamoDBReplicationGroupCreationCompleted(oldGroup, newGroup);
            case DELETING:
                LOGGER.info("Deleting replication group with name: " + newGroup.getReplicationGroupName() + " and UUID: " + newGroup.getReplicationGroupUUID()
                    + ".");
                return new DynamoDBReplicationGroupDeletionStarted(oldGroup, newGroup);
            default:
                coordinatorFail(INVALID_REPLICATION_GROUP_STATUS + " must transition to either ACTIVE or DELETING from CREATING.");
                throw new IllegalStateException(); // Unreachable - for safety
        }
    }

    /**
     * Helper function evaluating event change for a replication group where the starting state of the group is UPDATING
     *
     * @param oldGroup
     *            The old image of the replication group
     * @param newGroup
     *            The new image of the replication group
     * @return The detected transition change in the replication group
     */
    private static DynamoDBReplicationGroupTransition groupTransitionFromUpdating(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        coordinatorAssert(oldGroup.getReplicationGroupStatus().equals(DynamoDBReplicationGroupStatus.UPDATING), INVALID_REPLICATION_GROUP_STATUS);
        switch (newGroup.getReplicationGroupStatus()) {
            case ACTIVE:
                LOGGER.info("Updated replication group with name: " + newGroup.getReplicationGroupName() + " and UUID: " + newGroup.getReplicationGroupUUID()
                    + ".");
                return new DynamoDBReplicationGroupUpdateCompleted(oldGroup, newGroup);
            case DELETING:
                LOGGER.info("Deleting replication group with name: " + newGroup.getReplicationGroupName() + " and UUID: " + newGroup.getReplicationGroupUUID()
                    + ".");
                return new DynamoDBReplicationGroupDeletionStarted(oldGroup, newGroup);
            default:
                coordinatorFail(INVALID_REPLICATION_GROUP_STATUS + " must transition to either ACTIVE or DELETING from UPDATING.");
                throw new IllegalStateException(); // Unreachable - for safety
        }

    }

    /**
     * Evaluates the difference between members of two replication groups.
     *
     * @param oldMembers
     *            A list of replication group members in the old image of the replication group
     * @param newMembers
     *            A list of replication group members in the new image of the replication group
     * @return The detected transition corresponding to the change in the members of the replication group
     */
    protected static DynamoDBReplicationGroupTransition evaluateGroupMemberDiff(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup) {
        // Validate the old and new image of the replication group
        coordinatorAssert(null == newGroup || newGroup.isValid(), NEW_IMAGE_IS_NOT_VALID);
        coordinatorAssert(null == oldGroup || oldGroup.isValid(), OLD_IMAGE_IS_NOT_VALID);
        coordinatorAssert(null != oldGroup || null != newGroup, OLD_AND_NEW_IMAGES_MUST_NOT_BOTH_BE_NULL);

        // get members of both the old and new groups
        Collection<DynamoDBReplicationGroupMember> oldMembers = oldGroup.getReplicationGroupMembers().values();
        Collection<DynamoDBReplicationGroupMember> newMembers = newGroup.getReplicationGroupMembers().values();

        // The event change between the two replication groups
        DynamoDBReplicationGroupTransition detectedTransition = null;

        // Get a unique list of DynamoDB arn strings for members of the old and new images of the replication group
        Collection<String> oldMemberArns = getDynamoDBArnStrings(oldMembers);
        Collection<String> newArns = getDynamoDBArnStrings(newMembers);

        // Get common DynamoDBArns across the old and new images of the replication group
        Set<String> arnIntersection = getGroupMembersIntersectionByArn(oldMemberArns, newArns);

        Set<DynamoDBReplicationGroupMember> oldCommonMembers = new TreeSet<DynamoDBReplicationGroupMember>(
            DynamoDBReplicationUtilities.SAME_GROUP_MEMBER_COMPARATOR);
        Set<DynamoDBReplicationGroupMember> newCommonMembers = new TreeSet<DynamoDBReplicationGroupMember>(
            DynamoDBReplicationUtilities.SAME_GROUP_MEMBER_COMPARATOR);
        Set<DynamoDBReplicationGroupMember> addedMembers = new HashSet<DynamoDBReplicationGroupMember>();
        Set<DynamoDBReplicationGroupMember> removedMembers = new HashSet<DynamoDBReplicationGroupMember>();

        // Get the modified members (both their old and new images), newly added members, and removed members
        getModifiedAndNewOldMembers(arnIntersection, oldMembers, newMembers, oldCommonMembers, newCommonMembers, addedMembers, removedMembers);

        // Process modified members
        Iterator<DynamoDBReplicationGroupMember> oldIt = oldCommonMembers.iterator();
        Iterator<DynamoDBReplicationGroupMember> newIt = newCommonMembers.iterator();
        while (oldIt.hasNext() && newIt.hasNext()) {
            DynamoDBReplicationGroupMember oldM = oldIt.next();
            DynamoDBReplicationGroupMember newM = newIt.next();
            if (!oldM.equals(newM)) {
                coordinatorAssert(null == detectedTransition, ONLY_ONE_REPLICATION_GROUP_MEMBER_MODIFIED);
                detectedTransition = replicationGroupMemberModified(oldGroup, newGroup, oldM, newM);
            }
        }

        // Process removed members
        if (!removedMembers.isEmpty()) {
            coordinatorAssert(removedMembers.size() == 1, ONLY_ONE_REPLICATION_GROUP_MEMBER_MODIFIED + " multiple members were removed in the same update.");
            if (newGroup.getReplicationGroupStatus().equals(DynamoDBReplicationGroupStatus.DELETING)) {
                detectedTransition = new DynamoDBReplicationGroupDeletionStarted(oldGroup, newGroup);
            } else {
                DynamoDBReplicationGroupMember removedMember = removedMembers.iterator().next();
                LOGGER.info("Removed replication member with ARN: " + removedMember.getArn() + " from group with UUID: " + newGroup.getReplicationGroupUUID()
                    + ".");
                detectedTransition = new DynamoDBReplicationGroupMemberDirectTransition(oldGroup, newGroup, removedMember, null);
            }
        }

        // Process added members
        if (!addedMembers.isEmpty()) {
            coordinatorAssert(addedMembers.size() == 1, ONLY_ONE_REPLICATION_GROUP_MEMBER_MODIFIED + " multiple members were added in the same update.");
            DynamoDBReplicationGroupMember addedMember = addedMembers.iterator().next();
            LOGGER.info("Adding replication member with ARN: " + addedMember.getArn() + " to group with UUID: " + newGroup.getReplicationGroupUUID() + ".");
            detectedTransition = new DynamoDBReplicationGroupMemberPriorityTransition(oldGroup, newGroup, null, addedMember);
        }

        coordinatorAssert(null != detectedTransition, UNRECOGNIZED_REPLICATION_GROUP_MEMBER_CHANGE);
        return detectedTransition;
    }

    /**
     * Obtain a list of unique arn strings that is contained in the list of replication group members
     *
     * @param members
     *            list of replication group members
     * @return a list of unique arn strings
     */
    private static Collection<String> getDynamoDBArnStrings(Collection<DynamoDBReplicationGroupMember> members) {
        Set<String> arns = new HashSet<String>();
        for (DynamoDBReplicationGroupMember member : members) {
            arns.add(member.getArn());
        }
        return arns;
    }

    /**
     * Calculates the intersection between two collections of arn {@link String}.
     *
     * @param set1
     *            base collection for the result
     * @param set2
     *            retain members in toAdd based on the members present in this collection
     * @return the intersection between two collections of arn {@link String}.
     */
    private static Set<String> getGroupMembersIntersectionByArn(Collection<String> set1, Collection<String> set2) {
        Set<String> intersection = new TreeSet<String>();
        intersection.addAll(set1);
        intersection.retainAll(set2);
        return intersection;
    }

    /**
     * Calculates the modified, old and new replication group members based on the pre-calculated common DynamoDBArns between the old and new images of the
     * replication group. Both the old image and the new image of the modified replication group members are returned.
     *
     * @param commonArn
     *            common ARNs across the old image and new image of the replication group
     * @param oldMembers
     *            replication group members in the old image
     * @param newMembers
     *            replication group members in the new image
     * @param oldCommonMembers
     *            modified replication group members as retained in the old image
     * @param newCommonMembers
     *            modified replication group members as retained in the new image
     * @param addedMembers
     *            newly added replication group members
     * @param removedMembers
     *            removed replication group members
     */
    private static void getModifiedAndNewOldMembers(Set<String> commonArn, Collection<DynamoDBReplicationGroupMember> oldMembers,
        Collection<DynamoDBReplicationGroupMember> newMembers, Set<DynamoDBReplicationGroupMember> oldCommonMembers,
        Set<DynamoDBReplicationGroupMember> newCommonMembers, Set<DynamoDBReplicationGroupMember> addedMembers,
        Set<DynamoDBReplicationGroupMember> removedMembers) {

        // get the old common members, and removed members
        for (DynamoDBReplicationGroupMember oldMember : oldMembers) {
            if (commonArn.contains(oldMember.getArn())) {
                oldCommonMembers.add(oldMember);
            } else {
                removedMembers.add(oldMember);
            }
        }

        // get the new common members, and added members
        for (DynamoDBReplicationGroupMember newMember : newMembers) {
            if (commonArn.contains(newMember.getArn())) {
                newCommonMembers.add(newMember);
            } else {
                addedMembers.add(newMember);
            }
        }

        // sanity check
        coordinatorAssert(commonArn.size() == oldCommonMembers.size() && commonArn.size() == newCommonMembers.size(), INVALID_COMMON_MEMBER_SET);
    }

    /**
     * Helper function determining the change in a specific replication group member
     *
     * @param oldM
     *            The old image of the replication group member
     * @param newM
     *            The new image of the replication group member
     * @return The detected transition in the replication group member
     */
    private static DynamoDBReplicationGroupTransition replicationGroupMemberModified(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup,
        DynamoDBReplicationGroupMember oldM, DynamoDBReplicationGroupMember newM) {
        DynamoDBReplicationGroupMemberStatus oldMStatus = oldM.getReplicationGroupMemberStatus();
        DynamoDBReplicationGroupMemberStatus newMStatus = newM.getReplicationGroupMemberStatus();
        if (!DynamoDBReplicationUtilities.isInEnum(oldMStatus.name(), DynamoDBReplicationGroupMemberStatus.class)
            || !DynamoDBReplicationUtilities.isInEnum(newMStatus.name(), DynamoDBReplicationGroupMemberStatus.class)) {
            coordinatorFail(INVALID_REPLICATION_GROUP_STATUS);
            throw new IllegalStateException(); // Unreachable - for safety
        }
        if (!(oldM.getReplicationGroupMemberStatus().equals(newM.getReplicationGroupMemberStatus()))) {
            switch (newM.getReplicationGroupMemberStatus()) {
                case DELETE_FAILED:
                    if (newGroup.getReplicationGroupStatus().equals(DynamoDBReplicationGroupStatus.DELETING)) {
                        return new DynamoDBReplicationGroupDeletionStarted(oldGroup, newGroup);
                    }
                    // else fall through
                case BOOTSTRAP_FAILED:
                    // fall through
                case CREATE_FAILED:
                    // fall through
                case UPDATE_FAILED:
                    // fall through
                case ACTIVE:
                    // fall through
                case BOOTSTRAPPING:
                    // fall through
                case CREATING:
                    // fall through
                case WAITING:
                    LOGGER.info("Replication member with ARN: " + newM.getArn() + " and state: " + newM.getReplicationGroupMemberStatus()
                        + " triggering priority transition");
                    return new DynamoDBReplicationGroupMemberPriorityTransition(oldGroup, newGroup, oldM, newM);
                case BOOTSTRAP_CANCELLED:
                    if (newGroup.getReplicationGroupStatus().equals(DynamoDBReplicationGroupStatus.DELETING)) {
                        return new DynamoDBReplicationGroupDeletionStarted(oldGroup, newGroup);
                    }
                    // else fall through
                case BOOTSTRAP_COMPLETE:
                    // fall through
                case DELETING:
                    // fall through
                case UPDATING:
                    LOGGER.info("Replication member with ARN: " + newM.getArn() + " and state: " + newM.getReplicationGroupMemberStatus()
                        + " triggering direct transition");
                    return new DynamoDBReplicationGroupMemberDirectTransition(oldGroup, newGroup, oldM, newM);
                default:
                    coordinatorFail(INVALID_REPLICATION_GROUP_MEMBER_STATUS);
                    throw new IllegalStateException(); // Unreachable - for safety
            }
        } else {
            // we do not support changes in other parts of the replication group member (connectors update should always be accompanied by a status update)
            coordinatorFail(INVALID_REPLICATION_GROUP_MEMBER_TRANSITION + " member modification detected without a status change!");
            throw new IllegalStateException(); // Unreachable, here for safety
        }
    }
}
