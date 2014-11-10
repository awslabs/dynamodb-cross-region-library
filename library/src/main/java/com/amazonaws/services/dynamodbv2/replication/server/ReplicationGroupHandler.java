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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.replication.RegionReplicationCoordinator;
import com.amazonaws.services.dynamodbv2.replication.ReplicationConfiguration;
import com.amazonaws.services.dynamodbv2.replication.TableConfiguration;
import com.amazonaws.services.dynamodbv2.replication.server.model.ReplicationCoordinatorDescription;
import com.amazonaws.services.dynamodbv2.replication.server.model.ReplicationTableDescription;
import com.amazonaws.services.dynamodbv2.replication.server.model.TableIdentifier;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Handler for requests to the {@link ReplicationCoordinatorServer}.
 */
public class ReplicationGroupHandler extends AbstractHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationGroupHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Constants for parsing requests
    static final String SLASH = "/";
    static final String ADD_TABLES_TARGET = SLASH + "AddTables";
    static final String ADD_TABLES_TARGET_ = ADD_TABLES_TARGET + SLASH;
    static final String ADD_REGIONS_TARGET = SLASH + "AddRegions";
    static final String ADD_REGIONS_TARGET_ = ADD_REGIONS_TARGET + SLASH;
    static final String REMOVE_TABLES_TARGET = SLASH + "RemoveTables";
    static final String REMOVE_TABLES_TARGET_ = REMOVE_TABLES_TARGET + SLASH;
    static final String REMOVE_REGIONS_TARGET = SLASH + "RemoveRegions";
    static final String REMOVE_REGIONS_TARGET_ = REMOVE_REGIONS_TARGET + SLASH;
    static final String DESCRIBE_REPLICATION_COORDINATOR_TARGET = SLASH + "DescribeReplicationCoordinator";
    static final String DESCRIBE_REPLICATION_COORDINATOR_TARGET_ = DESCRIBE_REPLICATION_COORDINATOR_TARGET + SLASH;
    static final String DESCRIBE_REPLICATION_GROUP = SLASH + "DescribeReplicationGroup";
    static final String DESCRIBE_REPLICATION_GROUP_ = DESCRIBE_REPLICATION_GROUP + SLASH;
    static final String DESCRIBE_TABLES = SLASH + "DescribeTables";
    static final String DESCRIBE_TABLES_ = DESCRIBE_TABLES + SLASH;
    static final String DESCRIBE_REGIONS = SLASH + "DescribeRegions";
    static final String DESCRIBE_REGIONS_ = DESCRIBE_REGIONS + SLASH;
    static final String START_REPLICATION_TARGET = SLASH + "StartReplication";
    static final String START_REPLICATION_TARGET_ = START_REPLICATION_TARGET + SLASH;
    static final String STOP_REPLICATION_TARGET = SLASH + "StopReplication";
    static final String STOP_REPLICATION_TARGET_ = STOP_REPLICATION_TARGET + SLASH;

    // Constants for response generation
    private static final String JSON_CONTENT_TYPE = "application/json";

    private final RegionReplicationCoordinator coordinator;

    public ReplicationGroupHandler(RegionReplicationCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
        throws IOException, ServletException {
        switch (target) {
            case ADD_TABLES_TARGET:
            case ADD_TABLES_TARGET_:
                addTables(target, baseRequest, request, response);
                break;
            case REMOVE_REGIONS_TARGET:
            case REMOVE_REGIONS_TARGET_:
                removeRegions(target, baseRequest, request, response);
                break;
            case REMOVE_TABLES_TARGET:
            case REMOVE_TABLES_TARGET_:
                removeTables(target, baseRequest, request, response);
                break;
            case DESCRIBE_REPLICATION_COORDINATOR_TARGET:
            case DESCRIBE_REPLICATION_COORDINATOR_TARGET_:
                switch (request.getMethod().toUpperCase()) {
                    case "GET":
                        break;
                    default:
                        sendError(response, HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                            "DescribeReplicationCoordinator only supports GET");
                        return;
                }
                describeReplicationCoordinator(target, baseRequest, request, response);
                break;
            case DESCRIBE_REPLICATION_GROUP:
            case DESCRIBE_REPLICATION_GROUP_:
                switch (request.getMethod().toUpperCase()) {
                    case "GET":
                        break;
                    default:
                        sendError(response, HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                            "DescribeReplicationGroup only supports GET");
                        return;
                }
                describeReplicationGroup(target, baseRequest, request, response);
                break;
            case DESCRIBE_TABLES:
            case DESCRIBE_TABLES_:
                switch (request.getMethod().toUpperCase()) {
                    case "GET":
                        break;
                    default:
                        sendError(response, HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                            "DescribeTables only supports GET");
                        return;
                }
                describeTables(target, baseRequest, request, response);
                break;
            case DESCRIBE_REGIONS:
            case DESCRIBE_REGIONS_:
                switch (request.getMethod().toUpperCase()) {
                    case "GET":
                        break;
                    default:
                        sendError(response, HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                            "DescribeRegions only supports GET");
                        return;
                }
                describeRegions(target, baseRequest, request, response);
                break;
            case START_REPLICATION_TARGET:
            case START_REPLICATION_TARGET_:
                switch (request.getMethod().toUpperCase()) {
                    case "POST":
                        break;
                    default:
                        sendError(response, HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                            "StartReplication only supports POST");
                        return;
                }
                startReplication(target, baseRequest, request, response);
                break;
            case STOP_REPLICATION_TARGET:
            case STOP_REPLICATION_TARGET_:
                switch (request.getMethod().toUpperCase()) {
                    case "POST":
                        break;
                    default:
                        sendError(response, HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                            "StopReplication only supports POST");
                        return;
                }
                stopReplication(target, baseRequest, request, response);
                break;
            default:
                setHelpResponse(target, baseRequest, request, response);
                break;
        }
        baseRequest.setHandled(true);
    }

    private void startReplication(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) {
        coordinator.startRegionReplicationWorkers();
        describeReplicationCoordinator(target, baseRequest, request, response);
    }

    private void stopReplication(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) {
        coordinator.stopRegionReplicationWorkers();
        describeReplicationCoordinator(target, baseRequest, request, response);
    }

    private void describeReplicationCoordinator(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) {
        ReplicationCoordinatorDescription coordDesc = new ReplicationCoordinatorDescription();
        coordDesc.replicationCoordinatorStatus = coordinator.getReplicationCoordinatorStatus().toString();
        try {
            response.setContentType(JSON_CONTENT_TYPE);
            MAPPER.writeValue(response.getOutputStream(), coordDesc);
        } catch (IOException e) {
            LOGGER.error("Error generating response", e);
            sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error generating response");
        }
    }

    private void describeRegions(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) {
        String regionsParam = request.getParameter("regions");
        if (regionsParam == null) {
            sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Missing parameter: regions");
            return;
        }
        List<String> regions = Arrays.asList(regionsParam.split(","));
        ReplicationConfiguration configuration = coordinator.getReplicationConfiguration();
        List<TableConfiguration> configs = new LinkedList<TableConfiguration>();
        for (String region : regions) {
            for (String table : configuration.getTables(region)) {
                configs.add(coordinator.getTableConfiguration(region, table));
            }
        }
        try {
            response.setContentType(JSON_CONTENT_TYPE);
            MAPPER.writeValue(response.getOutputStream(), getGroupDescription(configs));
        } catch (IOException e) {
            LOGGER.error("Error generating response", e);
            sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error generating response");
        }

    }

    private void describeTables(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) {
        // TODO Validate request
        // TODO Parse arguments
        // TODO Set response
        List<TableIdentifier> tables = null;
        List<TableConfiguration> configs = new LinkedList<TableConfiguration>();
        for (TableIdentifier tableId : tables) {
            configs.add(coordinator.getTableConfiguration(tableId.region, tableId.tableName));
        }
        try {
            response.setContentType(JSON_CONTENT_TYPE);
            MAPPER.writeValue(response.getOutputStream(), getGroupDescription(configs));
        } catch (IOException e) {
            LOGGER.error("Error generating response", e);
            sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error generating response");
        }
    }

    private void removeRegions(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) {
        // TODO Validate request
        // TODO Parse arguments
        String regionName = null;
        // TODO Set response
        coordinator.removeRegion(regionName);
    }

    private void describeReplicationGroup(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) {
        // TODO Validate request
        // TODO Parse arguments
        // TODO Set response
        ReplicationConfiguration configuration = coordinator.getReplicationConfiguration();
        List<TableConfiguration> configs = new LinkedList<TableConfiguration>();
        for (String region : configuration.getRegions()) {
            for (String table : configuration.getTables(region)) {
                configs.add(coordinator.getTableConfiguration(region, table));
            }
        }
        try {
            response.setContentType(JSON_CONTENT_TYPE);
            MAPPER.writeValue(response.getOutputStream(), getGroupDescription(configs));
        } catch (IOException e) {
            LOGGER.error("Error generating response", e);
            sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error generating response");
        }
    }

    private void removeTables(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) {
        // TODO Validate request
        // TODO Parse arguments
        // TODO Set response
        // TODO missing coordinator.removeTable
    }

    private void addTables(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
        // TODO Validate request
        // TODO Parse arguments
        // TODO Set responses
        String newRegion = null;
        String tableName = null;
        Set<String> regionTables = new HashSet<String>(Arrays.asList(tableName));
        String cloudWatchEndpoint = null;
        String dynamoDBEndpoint = null;
        String dynamoDBStreamsEndpoint = null;
        AWSCredentialsProvider credentialsProvider = new InstanceProfileCredentialsProvider();
        coordinator.addRegion(newRegion, regionTables, cloudWatchEndpoint, dynamoDBEndpoint, dynamoDBStreamsEndpoint,
            credentialsProvider, credentialsProvider, credentialsProvider);
    }

    private void setHelpResponse(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) {
        sendError(response, HttpServletResponse.SC_BAD_REQUEST, target + " is an invalid target");

    }

    private static ReplicationTableDescription convertConfigToExternalDescription(TableConfiguration config) {
        ReplicationTableDescription description = new ReplicationTableDescription();
        description.isMaster = config.hasStreams();
        if (config.hasStreams()) {
            description.kinesisApplicationName = config.getKinesisApplicationName();
        }
        description.region = config.getRegion();
        description.tableName = config.getTable();
        description.tableStatus = config.getStatus().toString();
        // TODO description.accountId = ????
        return description;
    }

    private static Map<String, List<ReplicationTableDescription>> getGroupDescription(
        Collection<TableConfiguration> configs) {
        Map<String, List<ReplicationTableDescription>> rgDescription = new HashMap<String, List<ReplicationTableDescription>>();
        for (TableConfiguration config : configs) {
            String region = config.getRegion();
            if (!rgDescription.containsKey(region)) {
                rgDescription.put(region, new LinkedList<ReplicationTableDescription>());
            }
            rgDescription.get(region).add(convertConfigToExternalDescription(config));
        }
        return rgDescription;
    }

    private static void sendError(HttpServletResponse response, int sc, String msg) {
        try {
            response.sendError(sc, msg);
        } catch (IOException e) {
            LOGGER.error("Error writing error response", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
}
