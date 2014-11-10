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
package com.amazonaws.services.dynamodbv2.replication.server;

import com.amazonaws.services.dynamodbv2.replication.RegionReplicationCoordinator;
import com.amazonaws.services.dynamodbv2.replication.ReplicationPolicy.PolicyViolationBehavior;
import com.amazonaws.services.dynamodbv2.replication.impl.BasicTimestampReplicationPolicy;
import com.amazonaws.services.dynamodbv2.replication.impl.ReplicationConfigurationImpl;
import com.amazonaws.services.dynamodbv2.replication.impl.local.LocalRegionReplicationCoordinator;
import com.amazonaws.services.dynamodbv2.replication.impl.local.LocalShardSubscriberProxy;
import com.amazonaws.services.dynamodbv2.replication.impl.local.LocalTableApplierProxy;

/**
 * Runs a ReplicationCoordinatorServer with a timestamp based replication policy.
 */
public class LocalServerRunner {

    public static void main(String[] args) {
        RegionReplicationCoordinator coordinator = new LocalRegionReplicationCoordinator(
            new ReplicationConfigurationImpl(), new BasicTimestampReplicationPolicy(PolicyViolationBehavior.IGNORE),
            new LocalShardSubscriberProxy(),
            new LocalTableApplierProxy());
        ReplicationCoordinatorServer server = new ReplicationCoordinatorServer(coordinator);
        server.run();
    }
}
