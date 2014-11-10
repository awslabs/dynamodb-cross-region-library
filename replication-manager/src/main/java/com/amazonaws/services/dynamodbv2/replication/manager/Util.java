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
package com.amazonaws.services.dynamodbv2.replication.manager;

import java.util.HashMap;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.replication.manager.data.DataStore;
import com.amazonaws.services.dynamodbv2.replication.manager.data.DataStoreInMemory;

public class Util {
	private static Util self;
	HashMap<String, AmazonCloudWatchClient> cwClientMap;
    
	private Util(){
		 cwClientMap = new HashMap<String, AmazonCloudWatchClient>();    
	}
	
	public static Util getInstance(){
		if (self == null) {
			self = new Util();
		}
		return self;
	}
	
    public AmazonCloudWatchClient getCloudWatchClient(String region) {
    	if (cwClientMap.containsKey(region)){
    		return cwClientMap.get(region);
    	} else {
    		AmazonCloudWatchClient cwClient = new AmazonCloudWatchClient();
    		cwClient.setRegion(Region.getRegion(Regions.fromName(region)));
    		cwClientMap.put(region, cwClient);
    		return cwClient;
    	}
    }

	public static DataStore getDataStore() {
		// TODO
		return DataStoreInMemory.getInstance();
	}


}
