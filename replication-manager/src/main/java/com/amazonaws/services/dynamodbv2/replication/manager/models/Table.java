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

import com.fasterxml.jackson.annotation.JsonProperty;

public class Table {
	private String accountId;
	private String region;
	private String name;
	private boolean master;
	private String status;
	private String endpoint;
	private String kinesisApplicationName;
	
	@JsonProperty("AccountId")
	public String getAccountId() {
		return accountId;
	}
	
	@JsonProperty("AccountId")
	public void setAccountId(String accountId) {
		this.accountId = accountId;
	}
	
	@JsonProperty(value = "Region")
	public String getRegion() {
		return region;
	}
	
	@JsonProperty(value = "Region")
	public void setRegion(String region) {
		this.region = region;
	}
	
	@JsonProperty(value = "TableName")
	public String getTableName() {
		return name;
	}
	
	@JsonProperty(value = "TableName")
	public void setTableName(String name) {
		this.name = name;
	}
	
	@JsonProperty(value = "Master")
	public boolean isMaster() {
		return master;
	}
	
	@JsonProperty(value = "Master")
	public void setMaster(boolean master) {
		this.master = master;
	}
	
	@JsonProperty(value = "KinesisApplicationName")
	public String getKinesisApplicationName() {
		return kinesisApplicationName;
	}
	
	@JsonProperty(value = "KinesisApplicationName")
	public void setKinesisApplicationName(String name) {
		this.kinesisApplicationName = name;
	}
	
	@JsonProperty(value = "TableStatus")
	public String getStatus() {
		return status;
	}
	
	@JsonProperty(value = "TableStatus")	
	public void setStatus(String status) {
		this.status = status;
	}

	@JsonProperty(value = "Endpoint")
	public String getEndpoint() {
		return endpoint;
	}

	@JsonProperty(value = "Endpoint")
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

}
