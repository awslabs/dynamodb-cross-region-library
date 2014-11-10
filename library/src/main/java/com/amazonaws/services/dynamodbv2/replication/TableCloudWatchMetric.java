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
package com.amazonaws.services.dynamodbv2.replication;

/**
 * The CloudWatch metrics for each table.
 */
public final class TableCloudWatchMetric {
    /**
     * The number of update records successfully replicated to destination tables.
     */
    public static final String NUMBER_CHECKPOINTED_RECORDS = "NumberCheckpointedRecords";
    /**
     * The accumulated end-to-end latency of update records successfully replicated to destination tables. Dividing this
     * number by NUMBER_CHECKPOINTED_RECORDS gives the average end-to-end latency of replication each updates.
     */
    public static final String ACCUMULATED_RECORD_LATENCY = "AccumulatedRecordLatency";
    /**
     * The number of updates users make on the table.
     */
    public static final String NUMBER_USER_WRITES = "NumberUserWrites";
    /**
     * The number of update other tables propagated to the table.
     */
    public static final String NUMBER_REPLICATION_WRITES = "NumberReplicationWrites";

    /**
     * The private constructor.
     */
    private TableCloudWatchMetric() {
        // Not called.
    }
}
