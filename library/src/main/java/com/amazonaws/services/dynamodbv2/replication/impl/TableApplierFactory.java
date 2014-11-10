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

import com.amazonaws.services.dynamodbv2.replication.Applier;
import com.amazonaws.services.dynamodbv2.replication.ApplierFactory;
import com.amazonaws.services.dynamodbv2.replication.ReplicationConfiguration;
import com.amazonaws.services.dynamodbv2.replication.ReplicationPolicy;
import com.amazonaws.services.dynamodbv2.replication.ShardSubscriberProxy;
import com.amazonaws.services.dynamodbv2.replication.TableConfiguration;

/**
 * {@link ApplierFactory} for {@link TableApplier}.
 */
public class TableApplierFactory implements ApplierFactory {
    /**
     * ReplicationPolicy used by created {@link TableApplier}s to evaluate updates and resolve conflicts.
     */
    private final ReplicationPolicy policy;
    /**
     * SubscriberProxy used by created {@link TableApplier}s to acknowledge an applied update.
     */
    private final ShardSubscriberProxy proxy;
    /**
     * The replication configuration.
     */
    private final ReplicationConfiguration configuration;

    /**
     * Constructs a TableApplierFactory that uses the specified ReplicationConfiguration, ReplicationPolicy, and SubscriberProxy
     * to construct {@link TableApplier}s.
     *
     * @param configuration
     *            The replication configuration
     * @param policy
     *            ReplicationPolicy used by created {@link TableApplier}s to evaluate updates and resolve conflicts
     * @param proxy
     *            SubscriberProxy used by created {@link TableApplier}s to acknowledge an applied update
     */
    public TableApplierFactory(final ReplicationConfiguration configuration, final ReplicationPolicy policy,
        final ShardSubscriberProxy proxy) {
        this.configuration = configuration;
        this.policy = policy;
        this.proxy = proxy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Applier createApplier(final TableConfiguration tableConfig) {
        return new TableApplier(configuration, tableConfig.getRegion(), tableConfig.getTable(), policy, proxy);
    }

}
