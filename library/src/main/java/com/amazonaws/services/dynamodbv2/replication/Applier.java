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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Receives updates from {@link ShardSubscriber}s, persists them to the tables associated with the applier, and finally
 * acknowledges successful persistence to the {@link ShardSubscriber} from which the update was received.
 */
public abstract class Applier {
    /**
     * Logger for the Applier.
     */
    protected static final Logger LOGGER = LoggerFactory.getLogger(Applier.class);
    /**
     * Key for the tombstone field in items when tombstone is the chosen delete method.
     */
    public static final String TOMBSTONE_KEY = "$___DELETED___$";
    /**
     * AttributeValue for a deleted item when tombstone is the chosen delete method.
     */
    public static final AttributeValue TOMBSTONE_ATTRIBUTE_VALUE = new AttributeValue().withS("DELETED");

    /**
     * Gets the conditional expression for the given attributes to exists.
     *
     * @param itemKey
     *            The attributes to make the expression
     * @return The conditional expression
     */
    public static final String getUpdateCondition(final Map<String, AttributeValue> itemKey) {
        final StringBuilder sb = new StringBuilder();
        for (final String key : itemKey.keySet()) {
            sb.append("attribute_exists(" + key + ")" + " AND ");
        }
        return sb.toString();
    }

    /**
     * The region to which this Applier applies updates.
     */
    private final String region;
    /**
     * The ReplicationPolicy this Applier uses to evaluate updates and resolve conflicts.
     */
    private final ReplicationPolicy policy;
    /**
     * The AmazonDynamoDB this Applier uses to apply updates.
     */
    private final AmazonDynamoDB dynamoDB;
    /**
     * The SubscriberProxy this Applier uses to acknowledge an applied update.
     */
    private final ShardSubscriberProxy shardSubscriberProxy;

    /**
     * Constructs an {@link Applier} that applies updates to the specified region using the supplied
     * {@link AmazonDynamoDB} based on the provided {@link ReplicationPolicy}. Following an applied update, it acks
     * using the provided {@link ShardSubscriberProxy}.
     *
     * @param region
     *            The region to which this Applier applies updates
     * @param policy
     *            The ReplicationPolicy this Applier uses to evaluate updates and resolve conflicts
     * @param dynamoDB
     *            The AmazonDynamoDB this Applier uses to apply updates
     * @param shardSubsciberProxy
     *            The {@link ShardSubscriberProxy} this {@link Applier} uses to acknowledge an applied update
     */
    public Applier(final String region, final ReplicationPolicy policy, final AmazonDynamoDB dynamoDB,
        final ShardSubscriberProxy shardSubsciberProxy) {
        if (region == null) {
            throw new IllegalArgumentException("Region cannot be null");
        }
        if (policy == null) {
            throw new IllegalArgumentException("ReplicationPolicy cannot be null");
        }
        if (dynamoDB == null) {
            throw new IllegalArgumentException("DynamoDB cannot be null");
        }
        if (shardSubsciberProxy == null) {
            throw new IllegalArgumentException("ShardSubscriberProxy cannot be null");
        }
        this.region = region;
        this.policy = policy;
        this.dynamoDB = dynamoDB;
        shardSubscriberProxy = shardSubsciberProxy;
    }

    /**
     * Applies an update to the region(s) associated with the applier.
     *
     * @param key
     *            The key of changed item
     * @param oldItem
     *            The previous version of the updated item
     * @param newItem
     *            The new version of the updated item
     * @param sequenceNumber
     *            The unique identifier for the update within the shard
     * @param sourceRegion
     *            The source region that provided the update to the applier
     * @param sourceTable
     *            The source table that provided the update to the applier
     * @param shardSubscriberID
     *            The identifier for the {@link ShardSubscriber} that provided the update to the applier
     */
    public abstract void apply(Map<String, AttributeValue> key, Map<String, AttributeValue> oldItem,
        Map<String, AttributeValue> newItem, String sequenceNumber, String sourceRegion, String sourceTable,
        String shardSubscriberID);

    /**
     * Gets the AmazonDynamoDB this Applier uses to apply updates.
     *
     * @return The AmazonDynamoDB this Applier uses to apply updates
     */
    protected AmazonDynamoDB getAmazonDynamoDB() {
        return dynamoDB;
    }

    /**
     * Gets the region to which this Applier applies updates.
     *
     * @return The region to which this Applier applies updates
     */
    protected String getRegion() {
        return region;
    }

    /**
     * Gets the ReplicationPolicy this Applier uses to evaluate updates and resolve conflicts.
     *
     * @return The ReplicationPolicy this Applier uses to evaluate updates and resolve conflicts
     */
    protected ReplicationPolicy getReplicationPolicy() {
        return policy;
    }

    /**
     * Gets the SubscriberProxy this Applier uses to acknowledge an applied update.
     *
     * @return The SubscriberProxy this Applier uses to acknowledge an applied update
     */
    protected ShardSubscriberProxy getShardSubscriberProxy() {
        return shardSubscriberProxy;
    }
}
