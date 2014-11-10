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
package com.amazonaws.services.dynamodbv2.replication.impl;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.replication.ReplicationPolicy;

/**
 * Basic replication policy relying on time stamp.
 */
public class BasicTimestampReplicationPolicy implements ReplicationPolicy {
    /**
     * The time stamp attribute name.
     */
    public static final String TIMESTAMP_KEY = "TIMESTAMP___________KEY";
    private final DeleteBehavior deleteBehavior;
    private final PolicyViolationBehavior policyViolationBehavior;

    /**
     * Implements a new BasicTimestampReplicationPolicy class, only support tombstone delete for now.
     *
     * @param policyViolationBehavior
     *            The behavior for policy violation
     */
    public BasicTimestampReplicationPolicy(final PolicyViolationBehavior policyViolationBehavior) {
        deleteBehavior = DeleteBehavior.TOMBSTONE; /* only supports tombstone delete right now */
        this.policyViolationBehavior = policyViolationBehavior;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getConditionExpression() {
        return "attribute_not_exists(" + TIMESTAMP_KEY + ") OR " + TIMESTAMP_KEY + " < " + ":" + TIMESTAMP_KEY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DeleteBehavior getDeleteBehavior() {
        return deleteBehavior;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, AttributeValue> getExpressionAttributes(final Map<String, AttributeValue> newItem) {
        if (!newItem.containsKey(TIMESTAMP_KEY)) {
            throw new IllegalArgumentException(TIMESTAMP_KEY + " is a required field");
        }
        final Map<String, AttributeValue> expressionAttributes = new HashMap<String, AttributeValue>();
        expressionAttributes.put(":" + TIMESTAMP_KEY, newItem.get(TIMESTAMP_KEY));
        return expressionAttributes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, AttributeValue> getExpressionAttributes(final UpdateItemRequest request) {
        if (!request.getExpressionAttributeValues().containsKey(":" + TIMESTAMP_KEY)) {
            throw new IllegalArgumentException(TIMESTAMP_KEY + " is a required field");
        }
        final Map<String, AttributeValue> expressionAttributes = new HashMap<String, AttributeValue>();
        expressionAttributes.put(":" + TIMESTAMP_KEY, request.getExpressionAttributeValues().get(":" + TIMESTAMP_KEY));
        return expressionAttributes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PolicyViolationBehavior getPolicyViolationBehavior() {
        return policyViolationBehavior;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValidUpdate(final Map<String, AttributeValue> oldItem, final Map<String, AttributeValue> newItem) {
        final String oldTimestampISO8601 = oldItem.get(TIMESTAMP_KEY).getS();
        final String newTimestampISO8601 = newItem.get(TIMESTAMP_KEY).getS();
        return oldTimestampISO8601.compareTo(newTimestampISO8601) < 0;
    }
}
