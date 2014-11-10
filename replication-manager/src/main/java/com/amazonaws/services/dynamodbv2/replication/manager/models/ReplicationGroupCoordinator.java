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
package com.amazonaws.services.dynamodbv2.replication.manager.models;

import java.io.IOException;
import java.util.logging.Logger;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.client.ClientProperties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class ReplicationGroupCoordinator {
	public static final String DESCRIBE_REPLICATION_GROUP_API = "DescribeReplicationGroup";
	public static final String DESCRIBE_REPLICATION_COORDINATOR_API = "DescribeReplicationCoordinator";
	public static final String DESCRIBE_TABLES_API = "DescribeTables";
	public static final String START_REPLICATION_API = "StartReplication";
	public static final String STOP_REPLICATION_API = "StopReplication";	
	public static final String REPLICATION_COORDINATOR_STATUS_FIELD = "ReplicationCoordinatorStatus";
	protected String endpoint;
	protected Client client;
	protected String id;
	protected Logger logger = Logger.getLogger(getClass().getName());
	protected ReplicationGroupStatus status;

	public ReplicationGroupCoordinator(String groupId) {
		client = ClientBuilder.newClient();
		this.id = groupId;
	}
	
	public String getId(){
		return id;
	}

	public Response listTables() throws ReplicationGroupCoordinatorException {
    	WebTarget target = client.target(endpoint).path(DESCRIBE_REPLICATION_GROUP_API);
    	String json = target.request().get(String.class);
    	logger.finest(json);
		try {
	    	JsonNode tables = new ObjectMapper().readTree(json);
			return Response.ok(tables).build();
		} catch (IOException e) {
			throw new ReplicationGroupCoordinatorException("Failed to parse response from coordinator: " + json);
		}
	}


	public Response getTable(String region, String tableName, String accountId) throws ReplicationGroupCoordinatorException {
    	WebTarget target = client.target(endpoint).path(DESCRIBE_TABLES_API);
    	String json = target.request().get(String.class);
    	logger.finest(json);
    	try {
	    	JsonNode tables = new ObjectMapper().readTree(json);
	    	if (tables.isArray() && tables.size() == 1){
	    		return Response.ok(tables.get(0)).build();
	    	} else {
	    		throw new ReplicationGroupCoordinatorException("Unnexpected response from the coordinator. It should be an array with one table object in it, but received: " + json);
	    	}
		} catch (IOException e) {
			throw new ReplicationGroupCoordinatorException("Failed to parse response from the coordinator: " + json);
		}
	}

	public Response addTable(Table table) {
		// TODO 
		return Response.status(Status.NOT_IMPLEMENTED).build();
	}
	
	public Response deleteTable(String region, String tableName, String accountId) {
		// TODO 
		return Response.status(Status.NOT_IMPLEMENTED).build();
	}
	
	public Response startReplication() {
		ReplicationGroupStatus status = getStatus();
		if(status == ReplicationGroupStatus.STOPPED){
			logger.info("Starting replication: " + this.getId());
			WebTarget target = client.target(endpoint).path(START_REPLICATION_API);
			target.request().post(Entity.entity("", MediaType.APPLICATION_JSON_TYPE));
		}
		return Response.ok(getStatus()).build();
		
	}


	public Response stopReplication() {
		ReplicationGroupStatus status = getStatus();
		if(status == ReplicationGroupStatus.RUNNING){
			logger.info("Stopping replication: " + this.getId());
			WebTarget target = client.target(endpoint).path(STOP_REPLICATION_API);
			target.request().post(Entity.entity("", MediaType.APPLICATION_JSON_TYPE));
		} 
		return Response.ok(getStatus()).build();
    }

	public ReplicationGroupStatus getStatus(){
		Client client = ClientBuilder.newClient();

	    client.property(ClientProperties.CONNECT_TIMEOUT, 1000);
	    client.property(ClientProperties.READ_TIMEOUT,    1000);

	    WebTarget target = client.target(endpoint).path(DESCRIBE_REPLICATION_COORDINATOR_API);
	    try {
	    	JsonNode json = target.request().get(JsonNode.class);
	    	logger.fine("Received " + json);
	    	JsonNode statusField = json.get(REPLICATION_COORDINATOR_STATUS_FIELD);
	    	if (statusField != null ){
	    		return ReplicationGroupStatus.valueOf(statusField.textValue());
	    	} else {
	    		return ReplicationGroupStatus.UNKNOWN;
	    	}
	    } catch (ProcessingException pe) {
	    	if (this.status == ReplicationGroupStatus.BOOTSTRAPPING) {
	    		return ReplicationGroupStatus.BOOTSTRAPPING;
 	    	} else {
 	    		return ReplicationGroupStatus.COORDINATOR_UNREACHABLE;
 	    	}
	    }
	};

	public abstract void launch();
	
	public abstract void terminate();

}
