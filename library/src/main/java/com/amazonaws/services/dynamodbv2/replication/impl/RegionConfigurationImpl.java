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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.replication.RegionConfiguration;
import com.amazonaws.services.dynamodbv2.replication.TableConfiguration;
import com.amazonaws.services.dynamodbv2.replication.TableConfiguration.ReplicationTableStatus;

/**
 * Implementation of RegionConfiguration.
 */
public class RegionConfigurationImpl implements RegionConfiguration {
    /**
     * The region name.
     */
    private final String region;
    /**
     * The table configuration map. The key is the table name.
     */
    private final HashMap<String, TableConfiguration> tableConfigurations;
    /**
     * The region CloudWatch endpoint.
     */
    private final String cloudWatchEndpoint;
    /**
     * The CloudWatch credentials for the region.
     */
    private final AWSCredentialsProvider cloudWatchCredentialsProvider;
    /**
     * The DynamoDB credentials for the region.
     */
    private final AWSCredentialsProvider dynamoDBCredentialsProvider;
    /**
     * The region DynamoDB endpoint.
     */
    private final String dynamoDBEndpoint;
    /**
     * The region DynamoDB Streams endpoint.
     */
    private final String streamsEndpoint;
    /**
     * The Streams credentials for the region.
     */
    private final AWSCredentialsProvider streamsCredentialsProvider;

    /**
     * Constructor.
     *
     * @param region
     *            The region name
     * @param cloudWatchEndpoint
     *            The endpoint of CloudWatch in the new region
     * @param dynamoDBEndpoint
     *            The region DynamoDB endpoint
     * @param streamsEndpoint
     *            The region DynamoDB Streams endpoint
     * @param cloudWatchCredentialsProvider
     *            The Cloudwatch credentials for the region
     * @param dynamoDBCredentialsProvider
     *            The DynamoDB credentials
     * @param streamsCredentialsProvider
     *            The Streams credentials
     */
    public RegionConfigurationImpl(final String region, final String cloudWatchEndpoint, final String dynamoDBEndpoint,
        final String streamsEndpoint, final AWSCredentialsProvider cloudWatchCredentialsProvider,
        final AWSCredentialsProvider dynamoDBCredentialsProvider,
        final AWSCredentialsProvider streamsCredentialsProvider) {
        this.region = region;
        this.cloudWatchEndpoint = cloudWatchEndpoint;
        this.dynamoDBEndpoint = dynamoDBEndpoint;
        this.streamsEndpoint = streamsEndpoint;
        this.cloudWatchCredentialsProvider = cloudWatchCredentialsProvider;
        this.dynamoDBCredentialsProvider = dynamoDBCredentialsProvider;
        this.streamsCredentialsProvider = streamsCredentialsProvider;
        tableConfigurations = new HashMap<String, TableConfiguration>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addTable(final String table) {
        if (!tableConfigurations.containsKey(table)) {
            tableConfigurations.put(table, new TableConfigurationImpl(region, table, cloudWatchEndpoint,
                dynamoDBEndpoint, streamsEndpoint, cloudWatchCredentialsProvider, dynamoDBCredentialsProvider,
                streamsCredentialsProvider));
            return true;
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AWSCredentialsProvider getCloudWatchCredentialsProvider() {
        return cloudWatchCredentialsProvider;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCloudWatchEndpoint() {
        return cloudWatchEndpoint;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AWSCredentialsProvider getDynamoDBCredentialsProvider() {
        return dynamoDBCredentialsProvider;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDynamoDBEndpoint() {
        return dynamoDBEndpoint;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getRegion() {
        return region;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableConfiguration getTableConfiguration(final String table) {
        if (tableConfigurations.containsKey(table)) {
            return tableConfigurations.get(table);
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, TableConfiguration> getTableConfigurations() {
        return tableConfigurations;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReplicationTableStatus getTableStatus(final String table) {
        if (tableConfigurations.containsKey(table)) {
            return tableConfigurations.get(table).getStatus();
        }
        return ReplicationTableStatus.DOES_NOT_EXIST;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AWSCredentialsProvider getStreamsCredentialsProvider() {
        return streamsCredentialsProvider;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getStreamsEndpoint() {
        return streamsEndpoint;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeTable(final String table) {
        if (!tableConfigurations.containsKey(table)) {
            return false;
        }
        tableConfigurations.remove(table);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTableStatus(final String table, final ReplicationTableStatus status) {
        if (tableConfigurations.containsKey(table)) {
            tableConfigurations.get(table).setStatus(status);
        }
    }
}
