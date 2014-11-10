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
package com.amazonaws.services.dynamodbv2.replication;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;

/**
 * A user-supplied policy for determining if and how an update should be persisted to the other regions.
 */
public interface ReplicationPolicy {

    /**
     * Specifies the policy for applying deletes to an item.
     * <ul>
     * <li>{@link DeleteBehavior#TOMBSTONE} will replace an item with a tombstone record so conflict policies can be
     * applied between the last value and future updates.</li>
     * </ul>
     */
    enum DeleteBehavior {
        /**
         * Will leave a tombstone record in the table for comparison with future updates.
         */
        TOMBSTONE {
            /**
             * {@inheritDoc}
             */
            @Override
            public String toString() {
                return "TOMBSTONE";
            }
        },
    }

    /**
     * Behaviors for when {@link ReplicationPolicy#isValidUpdate(Map, Map, Map)} returns false:
     * <ul>
     * <li>{@link PolicyViolationBehavior#ROLLBACK} will revert the invalid change in the source region.</li>
     * <li>{@link PolicyViolationBehavior#PROPAGATE} will override the replication policy and propagate the write to
     * other regions.</li>
     * </ul>
     */
    enum PolicyViolationBehavior {
        /**
         * Will revert the invalid change in the source region.
         */
        ROLLBACK {
            /**
             * {@inheritDoc}
             */
            @Override
            public String toString() {
                return "ROLLBACK";
            }
        },
        /**
         * Will ignore an invalid write and leave the table in an inconsistent state.
         */
        IGNORE {
            /**
             * {@inheritDoc}
             */
            @Override
            public String toString() {
                return "IGNORE";
            }
        }
    }

    /**
     * Provides expected conditions for newItem to be persisted to a region based on the conflict policy.
     *
     * @return Expected conditions for newItem to be persisted to a region based on the conflict policy, without
     *         ExpressionAttributes
     */
    String getConditionExpression();

    /**
     * Provides the policy to apply when deleting an item.
     *
     * @return Delete behavior for the replication application
     */
    DeleteBehavior getDeleteBehavior();

    /**
     * ExpressionAttributes for putItem operation, new item to be persisted to a region based on the conflict policy.
     *
     * @param newItem
     *            The item to apply the conflict policy and generate expectations
     * @return ExpressionAttributes for newItem to be persisted to a region based on the conflict policy
     */
    Map<String, AttributeValue> getExpressionAttributes(Map<String, AttributeValue> newItem);

    /**
     * ExpressionAttributes for updateItem operation, updated item to be persisted to a region based on the conflict
     * policy.
     *
     * @param request
     *            The updateRequest containing the expression attribute to apply the conflict policy
     * @return ExpressionAttributes for newItem to be persisted to a region based on the conflict policy
     */
    Map<String, AttributeValue> getExpressionAttributes(UpdateItemRequest request);

    /**
     * Provides the policy to apply when an illegal update has been written to a table.
     *
     * @return The policy to apply when an illegal update has been written to a table
     */
    PolicyViolationBehavior getPolicyViolationBehavior();

    /**
     * Determines if the update to newItem should be persisted given that oldItem is found in the table.
     *
     * @param oldItem
     *            The item replaced in the origin region
     * @param newItem
     *            The new item in the origin region
     * @return True if according to the normal policy, oldItem is allowed to be replaced by newItem
     */
    boolean isValidUpdate(Map<String, AttributeValue> oldItem, Map<String, AttributeValue> newItem);
}
