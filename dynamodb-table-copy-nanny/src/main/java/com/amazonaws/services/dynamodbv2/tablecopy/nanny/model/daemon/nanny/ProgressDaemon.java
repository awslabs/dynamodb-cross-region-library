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
package com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.daemon.nanny;

import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTracker;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.DynamoDBTableCopyNanny;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.config.TableCopyConfigs;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.TableCopyConstants;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationUtilities;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ProgressDaemon extends NannyDaemon {

    private static final Logger LOG = Logger.getLogger(ProgressDaemon.class);

    protected final AmazonCloudWatch cloudWatch;
    protected final double totalSegments;
    protected final String tableCopyDimensionValue;
    protected final String dynamoDBCRRNameSpace;

    protected static volatile int completedSegments;

    public ProgressDaemon(AmazonCloudWatch cloudWatch, TableCopyConfigs configs) {
        this.cloudWatch = cloudWatch;
        this.totalSegments = Integer.valueOf(configs.numOfSegments);
        this.completedSegments = 0;

        String sourceRegion = DynamoDBReplicationUtilities.getRegionFromEndpoint(configs.sourceEndpoint);
        String destinationRegion = DynamoDBReplicationUtilities.getRegionFromEndpoint(configs.destinationEndpoint);
        this.tableCopyDimensionValue = sourceRegion + ":" + configs.sourceTable + "-" + destinationRegion
                + ":" + configs.destinationTable;

        LOG.info("Dimension Info: " + TableCopyConstants.TABLECOPY_DIMENSION + " > " + this.tableCopyDimensionValue);

        try {
            this.dynamoDBCRRNameSpace = DynamoDBReplicationUtilities.getHashedServiceName(sourceRegion,
                    configs.sourceTable, destinationRegion, configs.destinationTable, TableCopyConstants.ECS_CLUSTER_NAME);
            LOG.info("CloudWatch Metric Namespace: " + this.dynamoDBCRRNameSpace);
        } catch (Exception e) {
            throw new RuntimeException("Couldn't calculate the namespace for CloudWatch Metrics");
        }
    }

    public static void incrementProgress() {
        completedSegments++;
    }

    @Override
    public void run() {
        if (isAlive) {
            emitProgressMetric();
            try {
                DynamoDBTableCopyNanny.threadpool.schedule(this, TableCopyConstants.MINUTE_IN_MILLIS, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException ree) {
                LOG.warn("Threadpool has been shutdown, cannot run.", ree);
            }
        }
    }

    public void emitProgressMetric() {
        Dimension dim = new Dimension()
                .withName(TableCopyConstants.TABLECOPY_DIMENSION)
                .withValue(tableCopyDimensionValue);

        Double progressPercentage = new Double((completedSegments / totalSegments) * TableCopyConstants.FRACTION_TO_PERCENT);

        LOG.info("Emitting segment percentage progress: " + progressPercentage);

        MetricDatum datum = new MetricDatum()
                            .withDimensions(dim)
                            .withMetricName(TableCopyConstants.TABLECOPY_PROGRESS_METRIC)
                            .withValue(progressPercentage)
                            .withUnit(StandardUnit.Percent)
                            .withTimestamp(new Date());

        PutMetricDataRequest request = new PutMetricDataRequest()
                                .withNamespace(dynamoDBCRRNameSpace)
                                .withMetricData(datum);

        cloudWatch.putMetricData(request);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        emitProgressMetric();
        cloudWatch.shutdown();
    }

    @Override
    public void callback(TableCopyTracker tracker) {}
}
