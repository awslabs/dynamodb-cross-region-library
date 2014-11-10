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

import java.util.List;
import java.util.logging.Logger;

import com.amazonaws.services.dynamodbv2.replication.manager.Util;
import com.amazonaws.services.dynamodbv2.replication.manager.data.DataStoreException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ReplicationGroup {
	public static final String STOP = "stop";
	public static final String START = "start";	
	private String id;
	private String name;
	private ReplicationGroupCoordinatorType coordinatorType;
	private String coordinatorId;
	private ReplicationGroupCoordinator coordinator;
	private Logger logger = Logger.getLogger(getClass().getName());

	@JsonProperty("ID")
	public String getId() {
		return id;
	}
	
	@JsonProperty("ID")
	public void setId(String id) {
		this.id = id;
	}

	
	@JsonProperty("Name")
	public String getName(){
		return name;
	}

	@JsonProperty("Name")
	public void setName(String name) {
		this.name = name;
	}
	
	@JsonProperty("Status")
	public ReplicationGroupStatus getStatus() {
		if (coordinator == null){
			return ReplicationGroupStatus.STOPPED;
		} else {
			return coordinator.getStatus();
		}
	}
	
	@JsonProperty("CoordinatorType")
	public ReplicationGroupCoordinatorType getCoordinatorType() {
		if (coordinatorType == null){
			setCoordinatorType(ReplicationGroupCoordinatorType.LOCAL);
		}
		return coordinatorType;
	}

	@JsonProperty("CoordinatorType")
	public void setCoordinatorType(ReplicationGroupCoordinatorType coordinatorType) {
		this.coordinatorType = coordinatorType;
	}

	@JsonProperty("CoordinatorID")
	public String getCoordinatorId() {
		return coordinatorId;
	}

	@JsonProperty("CoordinatorID")
	public void setCoordinatorId(String coordinatorId) {
		this.coordinatorId = coordinatorId;
	}

	public static List<ReplicationGroup> list() throws DataStoreException {
		return Util.getDataStore().listReplicationGroups();
	}
	

	public static ReplicationGroup get(String groupId) throws ReplicationGroupNotFoundException, DataStoreException {
		ReplicationGroup group = Util.getDataStore().getReplicationGroup(groupId);
    	if (group == null){
    		throw new ReplicationGroupNotFoundException(groupId);
    	}

		return group;
	}
	
	public void save() throws DataStoreException{
		//TODO
		logger.info("Saving replication group: " + this.getId());
		Util.getDataStore().putReplicationGroup(this);
	}


	public void delete() throws DataStoreException {
		logger.info("Deleting replication group: " + this.getId());
		// TODO Auto-generated method stub
		Util.getDataStore().deleteReplicationGroup(this.getId());
	}
	
	
	@JsonIgnore
	public ReplicationGroupCoordinator getCoordinator() {
		if(coordinator == null) {
			switch (getCoordinatorType()) {
	    	case LOCAL:
	    		coordinator = new ReplicationGroupCoordinatorLocal(getCoordinatorId());
	    		break;
	    	case REMOTE:
	    	}
			this.coordinatorId = coordinator.getId();
		}
		return coordinator;
	}
}
