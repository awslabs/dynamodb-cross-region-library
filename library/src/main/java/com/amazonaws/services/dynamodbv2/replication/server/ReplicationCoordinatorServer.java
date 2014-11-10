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

import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.replication.RegionReplicationCoordinator;

/**
 * Runnable Jetty server that provides HTTP hooks to ReplicationCoordinator API.
 */
public class ReplicationCoordinatorServer implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationCoordinatorServer.class);
    public static final int DEFAULT_PORT = 8080;

    private final RegionReplicationCoordinator coordinator;
    private final int port;

    public ReplicationCoordinatorServer(RegionReplicationCoordinator coordinator) {
        this(coordinator, DEFAULT_PORT);
    }

    public ReplicationCoordinatorServer(RegionReplicationCoordinator coordinator, int port) {
        this.coordinator = coordinator;
        this.port = port;
    }

    @Override
    public void run() {
        Server server = null;
        try {
            server = new Server(port);
            server.setHandler(new ReplicationGroupHandler(coordinator));
            server.start();
            server.join();
        } catch (Exception e) {
            LOGGER.error("Error running HTTP server", e);
        }
    }
}
