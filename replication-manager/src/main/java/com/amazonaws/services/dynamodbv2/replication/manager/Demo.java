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

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;

public class Demo {

	public static final String DEMO = "demo";
	
	public static void main(String[] args) {
		createTable("master", "us-east-1", "http://localhost:8000",  true);
		createTable("replica", "ap-northeast-1", "http://localhost:8001", false);
	}

	public static void createTable(String name, String region, String endpoint, boolean streamEnabled) {
        CreateTableRequest ctr = new CreateTableRequest()
    	.withTableName(name)
    	.withAttributeDefinitions(
    		new AttributeDefinition().withAttributeName("device_id").withAttributeType("S"),
    		new AttributeDefinition().withAttributeName("time").withAttributeType("S")        		
    		)
    	.withKeySchema(
    		new KeySchemaElement().withAttributeName("device_id").withKeyType(KeyType.HASH),
    		new KeySchemaElement().withAttributeName("time").withKeyType(KeyType.RANGE)    		
    		)
    	.withProvisionedThroughput(new ProvisionedThroughput()
    		.withReadCapacityUnits(10L)
    		.withWriteCapacityUnits(10L));
        if (streamEnabled){
        	ctr.withStreamSpecification(new StreamSpecification()
    		.withStreamEnabled(true)
    		.withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES));
        }
    
        AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient();
        dynamoDBClient.setRegion(Region.getRegion(Regions.fromName(region)));
        dynamoDBClient.setEndpoint(endpoint);
                
        dynamoDBClient.createTable(ctr);
        System.out.println(dynamoDBClient.describeTable(name));
	}
}
