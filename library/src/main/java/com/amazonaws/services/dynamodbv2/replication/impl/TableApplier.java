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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.replication.Applier;
import com.amazonaws.services.dynamodbv2.replication.ReplicationConfiguration;
import com.amazonaws.services.dynamodbv2.replication.ReplicationPolicy;
import com.amazonaws.services.dynamodbv2.replication.ShardSubscriberProxy;
import com.amazonaws.services.dynamodbv2.replication.TableConfiguration;


/**
 * Applier implementation that makes writes to a single specified table.
 */
public class TableApplier extends Applier {
    /**
     * The table to which this TableApplier applies updates.
     */
    private final String table;
    /**
     * The replication configuration.
     */
    private final ReplicationConfiguration configuration;

    /**
     * Constructs a {@link TableApplier} that applies updates to the specified region and table. It uses the
     * {@link AmazonDynamoDB} provided from the {@link ReplicationConfiguration} to apply the updates based on the supplied
     * {@link ReplicationPolicy}. Following an update application, the {@link TableApplier} acknowledges the applied
     * update using the supplied {@link ShardSubscriberProxy}.
     *
     * @param configuration
     *            The replication configuration
     * @param region
     *            The region name
     * @param table
     *            The table name
     * @param policy
     *            The {@link ReplicationPolicy} this {@link TableApplier} uses to evaluate updates and resolve conflicts
     * @param shardSubscriberProxy
     *            The {@link ShardSubscriberProxy} this {@link TableApplier} uses to acknowledge an applied update
     */
    public TableApplier(final ReplicationConfiguration configuration, final String region, final String table,
        final ReplicationPolicy policy, final ShardSubscriberProxy shardSubscriberProxy) {
        super(region, policy, configuration.getRegionConfiguration(region).getTableConfiguration(table)
            .getDynamoDBClient(), shardSubscriberProxy);
        this.table = table;
        this.configuration = configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(final Map<String, AttributeValue> key,
 final Map<String, AttributeValue> oldItem,
        final Map<String, AttributeValue> newItem,
 final String sequenceNumber, final String sourceRegion,
        final String sourceTable, final String shardSubscriberID) {
        try {
            if (oldItem == null && newItem == null) {
                LOGGER.error("Illegal update! Old and new item are null. Sequence Number: " + sequenceNumber
                    + ", SubscriberID: " + shardSubscriberID);
            } else if (oldItem == null) {
                assert newItem != null;
                // Insert
                applyPut(key, null, newItem);
            } else if (newItem == null) {
                assert oldItem != null;
                // Delete
                switch (getReplicationPolicy().getDeleteBehavior()) {
                    case TOMBSTONE:
                        LOGGER.warn("Deletion detected in stream even though the delete behavior is tombstone: "
                            + oldItem);
                        break;
                    default:
                        assert false : "Unknown delete behavior";
                        throw new UnsupportedOperationException("Unknown delete behavior: "
                            + getReplicationPolicy().getDeleteBehavior());
                }
            } else {
                // Update/Put
                if (getReplicationPolicy().isValidUpdate(oldItem, newItem)) {
                    applyPut(key, oldItem, newItem);
                } else {
                    // Rollback or propagate policy violation based on user policy
                    switch (getReplicationPolicy().getPolicyViolationBehavior()) {
                        case ROLLBACK:
                            rollback(key, oldItem, newItem);
                            break;
                        case IGNORE:
                            // ignore the write
                            break;
                        default:
                            LOGGER.error("Unsupported policy violation behavior:"
                                + getReplicationPolicy().getPolicyViolationBehavior());
                            assert false : getReplicationPolicy().getPolicyViolationBehavior();
                            break;
                    }
                }
            }
        } finally {
            // Always ack. Error handling is separate from the replicator application
            getShardSubscriberProxy().ack(sequenceNumber, getRegion(), table, sourceRegion, sourceTable,
                shardSubscriberID);
            configuration.getRegionConfiguration(getRegion()).getTableConfiguration(table)
                .incAppliedRecordCount(shardSubscriberID);
        }

    }

    /**
     * A normal update consists of two put item requests.
     * <ol>
     * <li>Replace the old item with the new item if the old item is found in the table</li>
     * <li>Apply conflict resolution policy and attempt to replace the existing item with the new item based on those
     * conditions</li>
     * </ol>
     *
     * @param key
     *            The key of the updated item
     * @param oldItem
     *            The item that is expected in the table
     * @param newItem
     *            The item to replace the existing item
     */
    private void applyPut(final Map<String, AttributeValue> key, final Map<String, AttributeValue> oldItem,
        final Map<String, AttributeValue> newItem) {
        String conditionExpression = new String();
        final PutItemRequest request = new PutItemRequest();
        request.setTableName(table);
        request.setItem(newItem);
        conditionExpression = getReplicationPolicy().getConditionExpression();
        request.setConditionExpression(conditionExpression);
        request.setExpressionAttributeValues(getReplicationPolicy().getExpressionAttributes(newItem));
        try {
            getAmazonDynamoDB().putItem(request);
            LOGGER.debug("Successful PutRequest: " + request + " to table: " + table + " in region: " + getRegion());
        } catch (final ConditionalCheckFailedException e) {
            LOGGER.debug("Conditional write failed for item: " + newItem + " to table: " + table + " in region "
                + getRegion());
        } catch (final AmazonClientException e) {
            LOGGER.warn("Error updating (" + getRegion() + "->" + table + "): " + request, e);
        }
    }

    /**
     * Get the conditional expression for the current item of the applied table to be the same as the old item of the
     * source table.
     *
     * @param oldItem
     *            The old item to compare.
     * @return The conditional expression for two items to be the same
     */
    private String getSameItemExpectations(final Map<String, AttributeValue> oldItem) {
        // What we are doing here assumes at least one top level attribute is monotonically increasing between writes
        // (i.e. timestamp). If that top level attribute was missing, additional attributes could be added to the item
        // and we would not detect them.
        final StringBuilder sb = new StringBuilder();
        if (oldItem == null || oldItem.size() < 1) {
            throw new IllegalArgumentException("Item must be nonnull and have 1 or more attributes");
        }
        final List<String> itemKeys = new ArrayList<String>(oldItem.keySet());
        for (int i = 0; i < itemKeys.size() - 1; i++) {
            final String currKey = itemKeys.get(i);
            sb.append(currKey + "= :" + currKey);
            sb.append(" AND ");
        }
        final String lastKey = itemKeys.get(itemKeys.size() - 1);
        sb.append(lastKey + " = :" + lastKey);
        return sb.toString();
    }

    /**
     * Reverses new and old items and attempts to revert the change by the normal update process.
     *
     * @param key
     *            The key of the updated item
     * @param oldItem
     *            The old item that will attempt to persist over new item
     * @param newItem
     *            The new item that violates the normal policy to be rolled back
     * @see #applyNormalUpdate(Map, Map, Map)
     */
    private void rollback(final Map<String, AttributeValue> key, final Map<String, AttributeValue> oldItem,
        final Map<String, AttributeValue> newItem) {
        final PutItemRequest request = new PutItemRequest();
        request.setTableName(table);
        request.setItem(oldItem);
        request.setConditionExpression(getSameItemExpectations(newItem));
        request.setExpressionAttributeValues(newItem);
        try {
            getAmazonDynamoDB().putItem(request);
            LOGGER.debug("Successfully rolled back illegal write: " + oldItem + " -> " + newItem);
        } catch (final ConditionalCheckFailedException e) {
            LOGGER.debug("Could not rollback illegal write because update has since occured: " + oldItem + " -> "
                + newItem);
        }
    }
}
