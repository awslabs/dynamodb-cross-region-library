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
package com.amazonaws.services.dynamodbv2.tablecopy.nanny.model;

public class TableCopyConstants {

    //TODO: remove this after integrating coordinator dependency
    public static final String ECS_CLUSTER_NAME = "DynamoDBCrossRegionReplication";
    public static final String ECS_PRIMARY = "PRIMARY";
    public static final String PATH_TO_TABLECOPY_BIN = "/opt/dynamodb-tablecopy/DynamoDBTableCopyUtilities/bin/copy_table";
    public static final String TABLECOPY_DIMENSION = "CRRTableCopy";
    public static final String TABLECOPY_PROGRESS_METRIC = "SegmentsCompletedPercentage";

    public static final long DAY_IN_MILLIS = 24 * 60 * 60 * 1000;
    public static final long MINUTE_IN_MILLIS = 60 * 1000;
    public static final int SEGMENTS_PER_PARTITION = 10;
    public static final double BYTES_PER_PARTITION = 10 * 1024 * 1024 * 1024; // 10 GB ~> 10^9
    public static final double IOPS_PER_PARTITION = 3000;
    public static final double MIN_NUM_OF_PARTITION = 1;
    public static final int WCU_TO_IOPS = 3;

    public static final double FRACTION_TO_PERCENT = 100;

    public static final int NUM_OF_CHECKS_BEFORE_LOG = 3;

    public static final int MAX_METADATA_RETRIES = 4;
}
