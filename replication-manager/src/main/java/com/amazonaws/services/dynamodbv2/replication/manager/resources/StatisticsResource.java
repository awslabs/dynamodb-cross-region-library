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

import java.io.IOException;
import java.util.logging.Logger;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.dynamodbv2.replication.manager.Util;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path(StatisticsResource.PATH)
public class StatisticsResource {
	public static final String PATH = "/statistics";
	private Logger logger = Logger.getLogger(getClass().getName());

    @POST
    @Path("/{region}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response getMetricStatistics (@PathParam("region") String region,
    										String json) throws JsonProcessingException, IOException {
    	ObjectMapper mapper = new ObjectMapper();
    	
    	// Mixin IgnoreSetUnitMixIn to ignore one of the conflicting setUnit methods.
        mapper.addMixInAnnotations(GetMetricStatisticsRequest.class, IgnoreSetUnitMixIn.class);
        
    	GetMetricStatisticsRequest req = mapper.readValue(json, GetMetricStatisticsRequest.class);
    	logger.fine("Getting " + req.getMetricName() + " for " + req.getNamespace() + " from " + req.getStartTime() + " to " + req.getEndTime());
    	GetMetricStatisticsResult result;
    	try {
    		result = Util.getInstance().getCloudWatchClient(region).getMetricStatistics(req);
    	} catch (AmazonServiceException e){
    		return Response.status(Status.FORBIDDEN).entity("Accessing CloudWatch failed. Please make sure you have provided AWS credentials via environment variables or used IAM role if it is running on an EC2 instance").build();
    	}
    	return Response.ok(result).build();
    }
    
}

/*
 * Class to mixin to GetMetricStatisticsRequest so that one of the conflicting setUnit methods can be ignored.
 */
abstract class IgnoreSetUnitMixIn {
  @JsonIgnore public abstract void setUnit(StandardUnit unit);
}
