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
package com.amazonaws.services.dynamodbv2.replication.catchup;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.replication.ReplicationConfiguration;
import com.amazonaws.services.dynamodbv2.replication.TableConfiguration;

/**
 * Runnable task for scanning a single segment of the source table and inserting scan results into the destination
 * region.
 */
public class ReplicationScanSegmentTask implements Runnable {
    /**
     * The default number of items for a single scan.
     */
    private static final int DEFAULT_ITEM_LIMIT = 100;
    /**
     * The Logger for {@link ReplicationScanSegmentTask}.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationScanSegmentTask.class.getName());
    /**
     * The configuration for the catchup region group.
     */
    private final ReplicationConfiguration catchupConfiguration;
    /**
     * The source region to get the changes.
     */
    private final String sourceRegion;
    /**
     * The source table to get the changes.
     */
    private final String sourceTable;
    /**
     * The number of items for a single scan.
     */
    private final int itemLimit;
    /**
     * The total of segments for parallel scan.
     */
    private final int totalSegments;
    /**
     * The segment number of this task.
     */
    private final int segment;

    /**
     * Constructor.
     *
     * @param sourceRegion
     *            The source region
     * @param sourceTable
     *            The source table
     * @param catchupConfiguration
     *            The catchup replication configuration
     * @param totalSegments
     *            The total of segments for parallel scan
     * @param segment
     *            The segment number of this task
     */
    public ReplicationScanSegmentTask(final String sourceRegion, final String sourceTable,
        final ReplicationConfiguration catchupConfiguration, final int totalSegments, final int segment) {
        this(sourceRegion, sourceTable, catchupConfiguration, DEFAULT_ITEM_LIMIT, totalSegments, segment);
    }

    /**
     * Constructor.
     *
     * @param sourceRegion
     *            The source region
     * @param sourceTable
     *            The source table
     * @param catchupConfiguration
     *            The catchup replication configuration
     * @param itemLimit
     *            The number of items for a single scan
     * @param totalSegments
     *            The total of segments for parallel scan
     * @param segment
     *            The segment number of this task
     */
    public ReplicationScanSegmentTask(final String sourceRegion, final String sourceTable,
        final ReplicationConfiguration catchupConfiguration, final int itemLimit, final int totalSegments,
        final int segment) {
        super();
        this.sourceRegion = sourceRegion;
        this.sourceTable = sourceTable;
        this.catchupConfiguration = catchupConfiguration;
        this.itemLimit = itemLimit;
        this.totalSegments = totalSegments;
        this.segment = segment;
    }

    /**
     * Helper method for run. Issues putItem requests for a given item to all destination tables.
     *
     * @param item
     *            The item to send
     */
    private void propagateScanResult(final Map<String, AttributeValue> item) {
        for (final String region : catchupConfiguration.getRegions()) {
            if (!region.equals(sourceRegion)) {
                for (final TableConfiguration tableConfig : catchupConfiguration.getRegionConfiguration(region)
                    .getTableConfigurations().values()) {
                    final PutItemRequest putItemRequest = new PutItemRequest().withTableName(tableConfig.getTable())
                        .withItem(item);
                    tableConfig.getDynamoDBClient().putItem(putItemRequest);
                }
            }
        }

    }

    @Override
    public void run() {
        Map<String, AttributeValue> exclusiveStartKey = null;
        final AmazonDynamoDB sourceClient = catchupConfiguration.getRegionConfiguration(sourceRegion)
            .getTableConfiguration(sourceTable).getDynamoDBClient();
        try {
            while (true) {
                final ScanRequest scanRequest = new ScanRequest().withTableName(sourceTable).withLimit(itemLimit)
                    .withExclusiveStartKey(exclusiveStartKey).withTotalSegments(totalSegments).withSegment(segment);
                final ScanResult scanResult = sourceClient.scan(scanRequest);
                for (final Map<String, AttributeValue> item : scanResult.getItems()) {
                    propagateScanResult(item);
                }
                exclusiveStartKey = scanResult.getLastEvaluatedKey();
                if (exclusiveStartKey == null) {
                    break;
                }
            }
        } catch (final AmazonServiceException ase) {
            LOGGER.warn("Parallel Scan failed on segment " + segment + " " + ase.getMessage());
        }
    }
}
