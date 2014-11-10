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
package com.amazonaws.services.dynamodbv2.replication.manager.resources;

import java.net.URI;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import com.amazonaws.services.dynamodbv2.replication.manager.models.ReplicationGroup;
import com.amazonaws.services.dynamodbv2.replication.manager.models.ReplicationGroupStatus;
import com.amazonaws.services.dynamodbv2.replication.manager.models.Table;

/**
 * Root resource (exposed at "/" path)
 */
@Path(ReplicationGroupResource.PATH)
public class ReplicationGroupResource {
	public static final String PATH = "/groups";
	
    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getReplicationGroups() {
        return Response.ok(ReplicationGroup.list()).build();
    }

    @POST
    @Path("/")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createReplicationGroup(@Context UriInfo requestUri, ReplicationGroup group) {
    	group.setId(UUID.randomUUID().toString());
    	group.getCoordinator().launch();
    	group.save();
    	URI uri = URI.create(requestUri.getBaseUri() + PATH + "/" + group.getId());
    	return Response.created(uri).build();
    }
	
    @GET
    @Path("/{groupId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getReplicationGroup(@PathParam(value = "groupId") String groupId) {
    	return Response.ok(ReplicationGroup.get(groupId)).build();
    }
    
    @DELETE
    @Path("/{groupId}")
    public Response deleteReplicationGroup(@PathParam(value = "groupId") String groupId) {
    	ReplicationGroup group = ReplicationGroup.get(groupId);
    	if (group.getStatus() == ReplicationGroupStatus.STOPPED){
    		group.getCoordinator().terminate();
    		group.delete();
        	return Response.noContent().build();
    	} else {
    		return Response.status(Status.BAD_REQUEST).entity("Cannot delete running replication group. Stop replication first").build();
    	}
    }
    
    @GET
    @Path("/{groupId}/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getReplicationGroupStatus(
    			@PathParam("groupId") String groupId) {
    	ReplicationGroup group = ReplicationGroup.get(groupId);
    	return Response.ok(group.getStatus()).build();
    }

    	
    @PUT
    @Path("/{groupId}/status/{update}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateReplicationGroupStatus(
    			@PathParam("groupId") String groupId,
    			@PathParam("update") String update) {
    	ReplicationGroup group = ReplicationGroup.get(groupId);
    	ReplicationGroupStatus status = group.getStatus();
    	switch (status) {
    	case RUNNING:
    	case STOPPED:
    			if (ReplicationGroup.START.equals(update)){
    				return group.getCoordinator().startReplication();
    			} if (ReplicationGroup.STOP.equals(update)){
    				return group.getCoordinator().stopReplication();
    			} else {
        			return Response.status(Status.BAD_REQUEST).entity("Unknown operation: " + update).build();
    			}
    	case BOOTSTRAPPING:
			return Response.status(Status.BAD_REQUEST).entity("Cannot change status during bootstrapping").build();
    	default:
    		return Response.status(Status.INTERNAL_SERVER_ERROR).entity("Unknown status: " + status).build();
    	}
    }
   
    @GET
    @Path("/{groupId}/tables")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTablesInReplicationGroup(@PathParam(value = "groupId") String groupId) {    
    	ReplicationGroup group = ReplicationGroup.get(groupId);    	
    	return group.getCoordinator().listTables();
    }

    @POST
    @Path("/{groupId}/tables")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addTable(@PathParam(value = "groupId") String groupId, Table table) {
    	ReplicationGroup group = ReplicationGroup.get(groupId);
    	return group.getCoordinator().addTable(table);
    }

    @GET
    @Path("/{groupId}/tables/{region}/{tableName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTable(
    		@PathParam("groupId") String groupId,
    		@PathParam("region") String region,
    		@PathParam("tableName") String tableName,
    		@QueryParam("accountId") String accountId
    		) {
    	ReplicationGroup group = ReplicationGroup.get(groupId);
    	return group.getCoordinator().getTable(region, tableName, accountId);
    }
    
    @DELETE
    @Path("/{groupId}/tables/{region}/{tableName}")
    public Response deleteTable(
    		@PathParam(value = "groupId") String groupId,
    		@PathParam(value = "region") String region,
    		@PathParam(value = "tableName") String tableName,
    		@QueryParam("accountId") String accountId
    		) {
    	ReplicationGroup group = ReplicationGroup.get(groupId);
    	return group.getCoordinator().deleteTable(region, tableName, accountId);
    }
}
