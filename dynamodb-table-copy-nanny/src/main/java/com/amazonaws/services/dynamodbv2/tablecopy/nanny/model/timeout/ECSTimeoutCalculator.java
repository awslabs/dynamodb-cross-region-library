/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 * http://aws.amazon.com/asl/
 * 
 * or in the "LICENSE.txt" file accompanying this file.
 * 
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.timeout;

import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.TableCopyConstants;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.Deployment;
import com.amazonaws.services.ecs.model.DescribeServicesRequest;
import com.amazonaws.services.ecs.model.DescribeServicesResult;
import com.amazonaws.services.ecs.model.Service;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.List;

public class ECSTimeoutCalculator implements TimeoutCalculator {

    private static final Logger LOG = Logger.getLogger(ECSTimeoutCalculator.class);

    protected String ecsServiceName;
    protected AmazonECS ecs;

    public ECSTimeoutCalculator(AmazonECS ecs, String ecsServiceName) {
        this.ecs = ecs;
        this.ecsServiceName = ecsServiceName;
    }

    @Override
    public long calculateTimeoutInMillis() {
        DescribeServicesRequest request = new DescribeServicesRequest()
                                        .withCluster(TableCopyConstants.ECS_CLUSTER_NAME)
                                        .withServices(ecsServiceName);

        DescribeServicesResult result = ecs.describeServices(request);

        List<Service> services = result.getServices();
        if (services.size() == 0) {
            throw new IllegalStateException("Unable to find ecs service: " + ecsServiceName);
        }

        Date containerStartTime = null;
        for (Service service : services) {
            for (Deployment deployment : service.getDeployments()) {
                if (TableCopyConstants.ECS_PRIMARY.equals(deployment.getStatus())) {
                    containerStartTime = deployment.getCreatedAt();
                    LOG.debug("ECS Primary Start time: " + containerStartTime);
                }
            }
            break;
        }

        long timeToWait = TableCopyConstants.DAY_IN_MILLIS;
        if (containerStartTime != null) {
            Date now = new Date();
            LOG.debug("Now for ECSTimeoutCalculator" + now.toString());
            long elapsedTime = now.getTime() - containerStartTime.getTime();
            timeToWait -= elapsedTime;
            LOG.info("Elapsed Time:" + elapsedTime);
        }
        timeToWait = Math.max(timeToWait, 0L);
        LOG.info("Time to wait:" + timeToWait);
        return timeToWait;
    }
}
