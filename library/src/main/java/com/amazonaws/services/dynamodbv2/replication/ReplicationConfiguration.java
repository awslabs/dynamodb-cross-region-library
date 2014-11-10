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

import java.util.Collection;
import java.util.Set;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;

/**
 * The configuration of a replication application that manages replication for a single DynamoDB table.
 */
public interface ReplicationConfiguration {

    /**
     * Adds a region to the replication configuration.
     *
     * @param region
     *            The name of the new region
     * @param tables
     *            Set of tables (table replicas) in the region
     * @param cloudWatchEndpoint
     *            The endpoint of CloudWatch in the new region
     * @param dynamoDBEndpoint
     *            The endpoint of DynamoDB in the new region
     * @param streamsEndpoint
     *            The endpoint of DynamoDB Streams in the new region
     * @param cloudWatchCredentialsProvider
     *            The CloudWatch credentials for the region
     * @param dynamoDBCredentialsProvider
     *            The DynamoDB credentials for the region
     * @param streamsCredentialsProvider
     *            The Streams credentials for the region
     * @return True iff the region is successfully added
     */
    boolean addRegion(final String region, final Set<String> tables, final String cloudWatchEndpoint,
        final String dynamoDBEndpoint, final String streamsEndpoint,
        final AWSCredentialsProvider cloudWatchCredentialsProvider,
        final AWSCredentialsProvider dynamoDBCredentialsProvider,
        final AWSCredentialsProvider streamsCredentialsProvider);

    /**
     * Adds a new region configuration to the replication configuration.
     *
     * @param newRegion
     *            the new RegionConfiguration to add
     * @return True iff the region is successfully added
     */
    boolean addRegionConfiguration(RegionConfiguration newRegion);

    /**
     * Adds a table to be tracked by the replication application.
     *
     * @param region
     *            The region in which the new table is located.
     * @param table
     *            The table to bootstrap as a replica.
     * @return True iff the table is successfully added into the region
     */
    boolean addTable(String region, String table);

    /**
     * Gets {@link AmazonCloudWatchClient} for a given table in a given region.
     *
     * @param region
     *            The target region
     * @param table
     *            The target table
     * @return The CloudWatch Client
     */
    AmazonCloudWatchClient getCloudWatchClient(String region, String table);

    /**
     * Gets the credentials provider for write access to DynamoDB tables managed by the application in the specified
     * region.
     *
     * @param region
     *            The region to which the credentials provide write access
     * @return The credentials provider for write access to DynamoDB tables managed by the application in the specified
     *         region
     */
    AWSCredentialsProvider getCloudWatchCredentialsProvider(String region);

    /**
     * Gets the credentials provider for write access to cloud watch tables managed by the application in the specified
     * region.
     *
     * @param region
     *            The region to which the credentials provide write access
     * @return The credentials provider for write access to DynamoDB tables managed by the application in the specified
     *         region
     */
    AWSCredentialsProvider getDynamoDBCredentialsProvider(String region);

    /**
     * Gets Kinesis Application name for a given table in a given region.
     *
     * @param region
     *            The target region
     * @param table
     *            The target table
     * @return The Kinesis Application name
     */
    String getKinesisApplicationName(String region, String table);

    /**
     * Gets the configuration for a given region.
     *
     * @param region
     *            The region name
     * @return The region configuration
     */
    RegionConfiguration getRegionConfiguration(String region);

    /**
     * Gets the regions managed by the replication application.
     *
     * @return The regions managed by the replication application
     */
    Collection<String> getRegions();

    /**
     * Gets the credentials provider for write access to DynamoDB Streams tables managed by the application in the
     * specified region.
     *
     * @param region
     *            The region to which the credentials provide write access
     * @return The credentials provider for write access to DynamoDB Streams tables managed by the application in the
     *         specified region
     */
    AWSCredentialsProvider getStreamsCredentialsProvider(String region);

    /**
     * Gets the tables managed in a specific region by the replication application.
     *
     * @param region
     *            The region managed by the replication application
     * @return The tables managed in a specific region by the replication application
     */
    Collection<String> getTables(String region);

    /**
     * @param region
     *            The region to check
     * @return True if the region is a master region
     */
    boolean isMasterRegion(String region);

    /**
     * Removes all replica tables in a region currently being managed by the replication application.
     *
     * @param region
     *            The region to be removed
     * @return True iff the region is successfully removed
     */
    boolean removeRegion(String region);

    /**
     * Removes a replica table in a region currently being managed by the replication application.
     *
     * @param region
     *            The region in which the table resides
     * @param table
     *            The table to stop managing.
     * @return True iff the table is successfully added into the region
     */
    boolean removeTable(String region, String table);
}
