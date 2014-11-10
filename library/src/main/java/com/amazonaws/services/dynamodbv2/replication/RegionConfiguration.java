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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.replication.TableConfiguration.ReplicationTableStatus;

/**
 * Configuration for a region in a replication group.
 */
public interface RegionConfiguration {
    /**
     * Adds a table (a table replica) to the region.
     *
     * @param table
     *            The table name.
     * @return True iff the table is successfully added to the region.
     */
    boolean addTable(final String table);
 
    /**
     * Gets the CloudWatch Credentials for the region.
     *
     * @return The CloudWatch Credentials
     */
    AWSCredentialsProvider getCloudWatchCredentialsProvider();

    /**
     * Gets the region CloudWatch end point.
     *
     * @return The region end point
     */
    String getCloudWatchEndpoint();

    /**
     * Gets the DynamoDB Credentials for the region.
     *
     * @return The DynamoDB Credentials
     */
    AWSCredentialsProvider getDynamoDBCredentialsProvider();

    /**
     * Gets the region DynamoDB end point.
     *
     * @return The region end point
     */
    String getDynamoDBEndpoint();

    /**
     * Gets the region name.
     *
     * @return The region name
     */
    String getRegion();

    /**
     * Gets configurations of all tables in the region.
     *
     * @return The map of table name to table configuration
     */
    Map<String, TableConfiguration> getTableConfigurations();

    /**
     * Gets the configuration for a given table.
     *
     * @param table
     *            The table name
     * @return The table configuration
     */
    TableConfiguration getTableConfiguration(final String table);

    /**
     * Gets the status of a table.
     *
     * @param table
     *            The table to get the status
     * @return Status of the table
     */
    ReplicationTableStatus getTableStatus(String table);

    /**
     * Gets the Streams Credentials for the region.
     * 
     * @return The Streams Credentials
     */
    AWSCredentialsProvider getStreamsCredentialsProvider();

    /**
     * Gets the region DynamoDB Streams end point.
     * 
     * @return The region end point
     */
    String getStreamsEndpoint();

    /**
     * Removes a table (a table replica) from the region.
     *
     * @param table
     *            The table name.
     * @return True iff the table is successfully removed from the region.
     */
    boolean removeTable(final String table);

    /**
     * Sets a new status for a table.
     *
     * @param table
     *            The table to set
     * @param status
     *            The new status
     */
    void setTableStatus(String table, ReplicationTableStatus status);
}
