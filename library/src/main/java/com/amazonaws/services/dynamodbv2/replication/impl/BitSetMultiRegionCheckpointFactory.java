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

import com.amazonaws.services.dynamodbv2.replication.MultiRegionCheckpoint;
import com.amazonaws.services.dynamodbv2.replication.MultiRegionCheckpointFactory;
import com.amazonaws.services.dynamodbv2.replication.ReplicationConfiguration;

/**
 * Factory for {@link BitSetMultiRegionCheckpoint}.
 */
public class BitSetMultiRegionCheckpointFactory implements MultiRegionCheckpointFactory {
    /**
     * The replication configuration.
     */
    private final ReplicationConfiguration configuration;

    /**
     * Constructor.
     *
     * @param configuration
     *            The replication configuration
     *
     */
    public BitSetMultiRegionCheckpointFactory(final ReplicationConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MultiRegionCheckpoint createCheckpoint(final String sequenceNumber, final String createdTime) {
        final BitSetMultiRegionCheckpoint multiRegionCheckpoint = new BitSetMultiRegionCheckpoint(configuration,
            sequenceNumber, createdTime);
        return multiRegionCheckpoint;
    }

}
