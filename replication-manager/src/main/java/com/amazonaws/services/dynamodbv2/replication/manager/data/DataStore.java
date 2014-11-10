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

import java.util.List;

import com.amazonaws.services.dynamodbv2.replication.manager.models.ReplicationGroup;

public interface DataStore {

	public void putReplicationGroup (ReplicationGroup group) throws DataStoreException;
	
	public ReplicationGroup getReplicationGroup (String id) throws DataStoreException;
	
	public List<ReplicationGroup> listReplicationGroups() throws DataStoreException;
	
	public void deleteReplicationGroup(String id) throws DataStoreException;
	
}
