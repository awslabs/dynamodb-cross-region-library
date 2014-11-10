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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.replication.RegionConfiguration;
import com.amazonaws.services.dynamodbv2.replication.ReplicationConfiguration;
import com.amazonaws.services.dynamodbv2.replication.TableConfiguration;

/**
 * Implementation of ReplicationConfiguration.
 *
 */
public class ReplicationConfigurationImpl implements ReplicationConfiguration {
    /**
     * The region configuration map. They key is the region name.
     */
    private final HashMap<String, RegionConfiguration> regionConfigurations;

    /**
     * Constructor.
     */
    public ReplicationConfigurationImpl() {
        regionConfigurations = new HashMap<String, RegionConfiguration>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean addRegion(final String region, final Set<String> tables,
        final String cloudWatchEndpoint, final String dynamoDBEndpoint, final String streamsEndpoint,
        final AWSCredentialsProvider cloudWatchCredentialsProvider,
        final AWSCredentialsProvider dynamoDBCredentialsProvider,
        final AWSCredentialsProvider streamsCredentialsProvider) {
        if (regionConfigurations.containsKey(region) || tables.size() == 0) {
            return false;
        }
        final RegionConfiguration regionConfiguration = new RegionConfigurationImpl(region, cloudWatchEndpoint,
            dynamoDBEndpoint, streamsEndpoint, cloudWatchCredentialsProvider, dynamoDBCredentialsProvider,
            streamsCredentialsProvider);
        for (final String table : tables) {
            if (!regionConfiguration.addTable(table)) {
                return false;
            }
        }
        regionConfigurations.put(region, regionConfiguration);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addRegionConfiguration(final RegionConfiguration newRegion) {
        if (regionConfigurations.containsKey(newRegion.getRegion())) {
            return false;
        }
        regionConfigurations.put(newRegion.getRegion(), newRegion);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean addTable(final String region, final String table) {
        final RegionConfiguration regionConfiguration = regionConfigurations.get(region);
        if (regionConfiguration == null) {
            return false;
        }
        return regionConfiguration.addTable(table);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AmazonCloudWatchClient getCloudWatchClient(final String region, final String table) {
        final RegionConfiguration regionConfiguration = regionConfigurations.get(region);
        if (regionConfiguration == null) {
            return null;
        }
        final TableConfiguration tableConfiguration = regionConfiguration.getTableConfiguration(table);
        if (tableConfiguration == null) {
            return null;
        }
        return tableConfiguration.getCloudWatchClient();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AWSCredentialsProvider getCloudWatchCredentialsProvider(final String region) {
        final RegionConfiguration regionConfiguration = regionConfigurations.get(region);
        if (regionConfiguration == null) {
            return null;
        }
        return regionConfiguration.getCloudWatchCredentialsProvider();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AWSCredentialsProvider getDynamoDBCredentialsProvider(final String region) {
        final RegionConfiguration regionConfiguration = regionConfigurations.get(region);
        if (regionConfiguration == null) {
            return null;
        }
        return regionConfiguration.getDynamoDBCredentialsProvider();
    }

    @Override
    public String getKinesisApplicationName(final String region, final String table) {
        final RegionConfiguration regionConfiguration = regionConfigurations.get(region);
        if (regionConfiguration == null) {
            return null;
        }
        final TableConfiguration tableConfiguration = regionConfiguration.getTableConfiguration(table);
        if (tableConfiguration == null) {
            return null;
        }
        return tableConfiguration.getKinesisApplicationName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized RegionConfiguration getRegionConfiguration(final String region) {
        return regionConfigurations.get(region);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Collection<String> getRegions() {
        return regionConfigurations.keySet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Collection<String> getTables(final String region) {
        if (!regionConfigurations.containsKey(region)) {
            return new HashSet<String>();
        }
        return regionConfigurations.get(region).getTableConfigurations().keySet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AWSCredentialsProvider getStreamsCredentialsProvider(final String region) {
        final RegionConfiguration regionConfiguration = regionConfigurations.get(region);
        if (regionConfiguration == null) {
            return null;
        }
        return regionConfiguration.getStreamsCredentialsProvider();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean isMasterRegion(final String region) {
        final RegionConfiguration regionConfiguration = regionConfigurations.get(region);
        if (regionConfiguration == null) {
            return false;
        }

        for (final TableConfiguration tableConfiguration : regionConfiguration.getTableConfigurations().values()) {
            if (tableConfiguration.hasStreams()) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean removeRegion(final String region) {
        if (regionConfigurations.remove(region) == null) {
            return false;
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean removeTable(final String region, final String table) {
        final RegionConfiguration regionConfiguration = regionConfigurations.get(region);
        if (regionConfiguration == null) {
            return false;
        }
        return regionConfiguration.removeTable(table);
    }
}
