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

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

public class ReplicationGroupNotFoundException extends WebApplicationException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1317731628780183775L;

	public ReplicationGroupNotFoundException(String groupId) {
		super("No such group exists " + groupId, Response.Status.NOT_FOUND);
	}
}
