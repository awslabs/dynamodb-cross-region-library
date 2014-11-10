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
package com.amazonaws.services.dynamodbv2.replication.manager.data;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.replication.manager.models.ReplicationGroup;

public class DataStoreInMemory implements DataStore {
	private static DataStoreInMemory instance;
	private HashMap<String, ReplicationGroup> groupMap;
	
	private DataStoreInMemory (){
		groupMap = new HashMap<String, ReplicationGroup>();

	}
	
	public static DataStore getInstance(){
		if (instance == null) {
			instance = new DataStoreInMemory();
		}
		return instance;
	}

	@Override
	public void putReplicationGroup(ReplicationGroup group) throws DataStoreException {
		if (group.getId() == null) {
			throw new DataStoreException("ID cannot be null");
		}
		groupMap.put(group.getId(), group);
	}

	@Override
	public ReplicationGroup getReplicationGroup(String id) throws DataStoreException {
		return groupMap.get(id);
	}

	@Override
	public List<ReplicationGroup> listReplicationGroups() throws DataStoreException {
		LinkedList<ReplicationGroup> list = new LinkedList<ReplicationGroup>();
		for (ReplicationGroup group : groupMap.values()){
			list.add(group);
		}
		return list;
	}

	@Override
	public void deleteReplicationGroup(String id) throws DataStoreException {
		if (groupMap.containsKey(id)){
			groupMap.remove(id);
		}
		throw new DataStoreException("No such grouop found: " + id);
	}

}
