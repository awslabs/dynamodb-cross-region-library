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

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.client.ClientProperties;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.replication.ApplierProxy;
import com.amazonaws.services.dynamodbv2.replication.RegionReplicationCoordinator;
import com.amazonaws.services.dynamodbv2.replication.ReplicationConfiguration;
import com.amazonaws.services.dynamodbv2.replication.ReplicationPolicy;
import com.amazonaws.services.dynamodbv2.replication.ShardSubscriberProxy;
import com.amazonaws.services.dynamodbv2.replication.ReplicationPolicy.PolicyViolationBehavior;
import com.amazonaws.services.dynamodbv2.replication.impl.BasicTimestampReplicationPolicy;
import com.amazonaws.services.dynamodbv2.replication.impl.ReplicationConfigurationImpl;
import com.amazonaws.services.dynamodbv2.replication.impl.local.LocalRegionReplicationCoordinator;
import com.amazonaws.services.dynamodbv2.replication.impl.local.LocalShardSubscriberProxy;
import com.amazonaws.services.dynamodbv2.replication.impl.local.LocalTableApplierProxy;
import com.amazonaws.services.dynamodbv2.replication.manager.Demo;
import com.amazonaws.services.dynamodbv2.replication.server.ReplicationCoordinatorServer;


public class ReplicationGroupCoordinatorLocal extends
		ReplicationGroupCoordinator {
	private ExecutorService service;
	private RegionReplicationCoordinator coordinator;
	
	public ReplicationGroupCoordinatorLocal(String id) {
		super(id);
    	this.endpoint = "http://localhost:8081";    	
   	}

	
	public Response addTable(Table table) {
		logger.info("Received addTable request");
		logger.info(table.getRegion());
		logger.info(table.getEndpoint());
		logger.info(table.getTableName());

		if (table.getEndpoint() != null){
			logger.fine("Checking if endpoint is reachable");
			Client client = ClientBuilder.newClient();
		    client.property(ClientProperties.CONNECT_TIMEOUT, 1000);
		    client.property(ClientProperties.READ_TIMEOUT,    1000);
		    WebTarget target = client.target(table.getEndpoint()).path("/");
		    try {
		    	target.request().get();
		    } catch (ProcessingException pe) {
		    	return Response.status(Status.BAD_REQUEST).entity("Endpoint is not reachable: " + table.getEndpoint()).build();
		    }
			// TODO Demo purpose. Remove.		   
		    if (table.getEndpoint().startsWith("http://localhost")){
		    	try {
		    		Demo.createTable(table.getTableName(), table.getRegion(), table.getEndpoint(), table.isMaster());
		    	} catch (ResourceInUseException e) {
		    		logger.fine("Table already exists");
		    	}
		    }
		}
		
        AWSCredentialsProvider creds = new DefaultAWSCredentialsProviderChain() ;
		coordinator.addRegion(table.getRegion(),
				new HashSet<String>(Arrays.asList(table.getTableName())),
	            "http://monitoring." + table.getRegion() + ".amazonaws.com", 
	            table.getEndpoint(),
	            table.getEndpoint(), 
	            creds, creds, creds);
		return Response.ok().build();
	}
	

	@Override
	public void launch() {
		
		new Thread(new Runnable(){
			@Override
			public void run() {
		        ReplicationConfiguration configuration = new ReplicationConfigurationImpl();
		        ReplicationPolicy replicationPolicy = new BasicTimestampReplicationPolicy(PolicyViolationBehavior.IGNORE);
		        ShardSubscriberProxy subscriberProxy = new LocalShardSubscriberProxy();
		        ApplierProxy applierProxy = new LocalTableApplierProxy();
		        coordinator = new LocalRegionReplicationCoordinator(configuration,
		            replicationPolicy, subscriberProxy, applierProxy);

		        Table masterTable = new Table();
		        masterTable.setTableName("master");
		        masterTable.setRegion("us-east-1");
		        masterTable.setEndpoint("http://localhost:8000");
		        masterTable.setMaster(true);
		        
		        addTable(masterTable);
		        
		        Table replicaTable = new Table();
		        replicaTable.setTableName("replica");
		        replicaTable.setRegion("ap-northeast-1");
		        replicaTable.setEndpoint("http://localhost:8001");
		        replicaTable.setMaster(false);
		        
		        addTable(replicaTable);

		        	
		        ReplicationCoordinatorServer runner = new ReplicationCoordinatorServer(coordinator, 8081);
		        service = Executors.newSingleThreadExecutor();
		        service.submit(runner);		        
	
			}
		}).start();
    	this.status = ReplicationGroupStatus.BOOTSTRAPPING;
      }

	@Override
	public void terminate() {
        service.shutdown();	
        coordinator = null;
        service = null;
	}
}
