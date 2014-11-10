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

import java.util.UUID;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.replication.RegionConfiguration;
import com.amazonaws.services.dynamodbv2.replication.TableConfiguration.ReplicationTableStatus;
import com.amazonaws.services.dynamodbv2.replication.impl.RegionConfigurationImpl;
import com.amazonaws.services.dynamodbv2.replication.impl.TableConfigurationImpl;

/**
 * RegionConfiguration for bootstrapping and catchup application.
 */
public class CatchupRegionConfiguration extends RegionConfigurationImpl {
    /**
     * Default application name.
     */
    private static final String APPLICATION_NAME_PREFIX = "CatchUpApplication";
    /**
     * Application name.
     */
    private final String applicationName;

    /**
     * Constructs a {@link CatchupRegionConfiguration} from another {@link RegionConfiguration}.
     *
     * @param regionRepConfig
     *            The sample region configuration
     */
    public CatchupRegionConfiguration(final RegionConfiguration regionRepConfig) {
        super(regionRepConfig.getRegion(), regionRepConfig.getCloudWatchEndpoint(), regionRepConfig
            .getDynamoDBEndpoint(), regionRepConfig.getStreamsEndpoint(), regionRepConfig
            .getCloudWatchCredentialsProvider(), regionRepConfig.getDynamoDBCredentialsProvider(), regionRepConfig
            .getStreamsCredentialsProvider());
        applicationName = APPLICATION_NAME_PREFIX + UUID.randomUUID();
    }

    /**
     * Constructor.
     *
     * @param region
     *            The region name
     * @param cloudWatchEndpoint
     *            The CloudWatch endpoint
     * @param dynamoDBEndpoint
     *            The region DynamoDB endpoint
     * @param streamsEndpoint
     *            The region DynamoDB Streams endpoint
     * @param cloudWatchCredentialsProvider
     *            The CloudWatch credentials
     * @param dynamoDBCredentialsProvider
     *            The DynamoDB credentials
     * @param streamsCredentialsProvider
     *            The Streams credentials
     */
    public CatchupRegionConfiguration(final String region, final String cloudWatchEndpoint,
        final String dynamoDBEndpoint, final String streamsEndpoint,
        final AWSCredentialsProvider cloudWatchCredentialsProvider,
        final AWSCredentialsProvider dynamoDBCredentialsProvider,
        final AWSCredentialsProvider streamsCredentialsProvider) {
        super(region, cloudWatchEndpoint, dynamoDBEndpoint, streamsEndpoint, cloudWatchCredentialsProvider,
            dynamoDBCredentialsProvider, streamsCredentialsProvider);
        applicationName = APPLICATION_NAME_PREFIX + UUID.randomUUID();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addTable(final String table) {
        if (!getTableConfigurations().containsKey(table)) {
            getTableConfigurations().put(
                table,
                new TableConfigurationImpl(applicationName, getRegion(), table, getCloudWatchEndpoint(),
                    getDynamoDBEndpoint(), getStreamsEndpoint(), getCloudWatchCredentialsProvider(),
                    getDynamoDBCredentialsProvider(), getStreamsCredentialsProvider()));
            setTableStatus(table, ReplicationTableStatus.BOOTSTRAPPING);
            return true;
        }
        return false;
    }

}
