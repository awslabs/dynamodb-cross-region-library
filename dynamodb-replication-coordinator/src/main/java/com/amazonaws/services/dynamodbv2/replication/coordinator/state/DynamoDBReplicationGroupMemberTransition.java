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

import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMember;

/**
 * Abtract class representing a replication group member transition from one state to the next, triggered by a status change in any of the members of the
 * replication group.
 */
public abstract class DynamoDBReplicationGroupMemberTransition extends DynamoDBReplicationGroupTransition {

    /*
     * Old image of the replication group member that changed status
     */
    protected final DynamoDBReplicationGroupMember oldM;

    /*
     * New image of the replication group member that changed status
     */
    protected final DynamoDBReplicationGroupMember newM;

    /**
     * Default constructor
     *
     * @param oldGroup
     *            old image of the replication group
     * @param newGroup
     *            new image of the replication group
     * @param oldM
     *            old image of the replication group member that changed status
     * @param newM
     *            new image of the replication group member that changed status
     */
    public DynamoDBReplicationGroupMemberTransition(DynamoDBReplicationGroup oldGroup, DynamoDBReplicationGroup newGroup, DynamoDBReplicationGroupMember oldM,
        DynamoDBReplicationGroupMember newM) {
        super(oldGroup, newGroup);
        validateGroupMember(oldM, newM);
        this.oldM = oldM == null ? null : new DynamoDBReplicationGroupMember(oldM);
        this.newM = newM == null ? null : new DynamoDBReplicationGroupMember(newM);
    }

    /**
     * Validates the group member transition.
     *
     * @param oldM
     *            The old image of the group member
     * @param newM
     *            The new image of the group member
     * @throws IllegalStateException
     *             Illegal transition
     */
    public abstract void validateGroupMember(DynamoDBReplicationGroupMember oldM, DynamoDBReplicationGroupMember newM);
}
