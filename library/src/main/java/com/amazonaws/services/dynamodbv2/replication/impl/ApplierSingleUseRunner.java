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

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.replication.Applier;

/**
 * Wraps an {@link Applier} as a {@link Runnable} associated with a single update.
 */
public class ApplierSingleUseRunner implements Runnable {
    /**
     * The {@link Applier} this {@link ApplierSingleUseRunner} will use to apply the associated update.
     */
    private final Applier applier;
    /**
     * The key of the item affected by the associated update.
     */
    private final Map<String, AttributeValue> key;
    /**
     * The previous version of the item affected by the associated update.
     */
    private final Map<String, AttributeValue> oldItem;
    /**
     * The new version of the item affected by the associated update.
     */
    private final Map<String, AttributeValue> newItem;
    /**
     * The unique identifier of the associated update.
     */
    private final String sequenceNumber;
    /**
     * The unique identifier for the Subscriber to which this {@link Applier} will ack.
     */
    private final String subscriberID;

    /**
     * The table to which this {@link Applier} will ack.
     */
    private final String sourceTable;

    /**
     * The region to which this {@link Applier} will ack.
     */
    private final String sourceRegion;

    /**
     * Constructs an {@link ApplierSingleUseRunner} that uses the supplied {@link Applier} to apply the update defined
     * by key, oldItem, newItem, and sequenceNumber. The applier will ack to the Subscriber referenced by subscriberID.
     *
     * @param applier
     *            The {@link Applier} this {@link ApplierSingleUseRunner} will use to apply the associated update
     * @param key
     *            The key of the item affected by the associated update
     * @param oldItem
     *            The previous version of the item affected by the associated update
     * @param newItem
     *            The new version of the item affected by the associated update
     * @param sequenceNumber
     *            The unique identifier of the associated update
     * @param sourceRegion
     *            The region to which this {@link Applier} will ack
     * @param sourceTable
     *            The table to which this {@link Applier} will ack
     * @param subscriberID
     *            The unique identifier for the Subscriber to which this {@link Applier} will ack.
     */
    public ApplierSingleUseRunner(final Applier applier, final Map<String, AttributeValue> key,
        final Map<String, AttributeValue> oldItem, final Map<String, AttributeValue> newItem,
        final String sequenceNumber, final String sourceRegion, final String sourceTable, final String subscriberID) {
        this.applier = applier;
        this.key = key;
        this.oldItem = oldItem;
        this.newItem = newItem;
        this.sequenceNumber = sequenceNumber;
        this.subscriberID = subscriberID;
        this.sourceTable = sourceTable;
        this.sourceRegion = sourceRegion;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        applier.apply(key, oldItem, newItem, sequenceNumber, sourceRegion, sourceTable, subscriberID);
    }

}
