/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streams.connectors;

import com.beust.jcommander.Parameter;

import lombok.Getter;

@Getter
public class CommandLineArgs {
    public static final String HELP = "--help";
    @Parameter(names = HELP, description = "Display usage information", help = true)
    private boolean help;

    public static final String SOURCE_SIGNING_REGION = "--sourceRegion";
    @Parameter(names = SOURCE_SIGNING_REGION, required = true, description =
            "Signing region to use for the DynamoDB endpoint containing the source table")
    private String sourceSigningRegion;

    public static final String SOURCE_ENDPOINT = "--sourceEndpoint";
    @Parameter(names = SOURCE_ENDPOINT, description = "DynamoDB endpoint of the source table")
    private String sourceEndpoint;

    public static final String SOURCE_STREAMS_ENDPOINT = "--sourceStreamsEndpoint";
    @Parameter(names = SOURCE_STREAMS_ENDPOINT, description = "DynamoDB Streams endpoint of the source table")
    private String sourceStreamsEndpoint;

    public static final String SOURCE_TABLE = "--sourceTable";
    @Parameter(names = SOURCE_TABLE, description = "Name of the source table", required = true)
    private String sourceTable;

    public static final String KCL_SIGNING_REGION = "--kclRegion";
    @Parameter(names = KCL_SIGNING_REGION, description =
            "Signing region to use for the DynamoDB endpoint containing the KCL table")
    private String kclSigningRegion;

    public static final String KCL_ENDPOINT = "--kclEndpoint";
    @Parameter(names = KCL_ENDPOINT, description = "DynamoDB endpoint for KCL to use")
    private String kclEndpoint;

    public static final String TASK_NAME = "--taskName";
    @Parameter(names = TASK_NAME, description = "Name of task, used to name DynamoDB checkpoint table and identify metrics in CloudWatch")
    private String taskName;

    public static final String DESTINATION_SIGNING_REGION = "--destinationRegion";
    @Parameter(names = DESTINATION_SIGNING_REGION, required = true, description =
            "Signing region to use for the DynamoDB endpoint containing the destination table")
    private String destinationSigningRegion;

    public static final String DESTINATION_ENDPOINT = "--destinationEndpoint";
    @Parameter(names = DESTINATION_ENDPOINT, description = "DynamoDB endpoint of the destination table")
    private String destinationEndpoint;

    public static final String DESTINATION_TABLE = "--destinationTable";
    @Parameter(names = DESTINATION_TABLE, description = "Name of the destination table", required = true)
    private String destinationTable;

    public static final String DONT_PUBLISH_CLOUDWATCH = "--dontPublishCloudwatch";
    @Parameter(names = DONT_PUBLISH_CLOUDWATCH, description = "Have KCL not publish Cloudwatch metrics", hidden = true)
    private boolean dontPublishCloudwatch = false;

    public static final String BATCH_SIZE = "--batchSize";
    @Parameter(names = BATCH_SIZE, description = "Number of records to request in each DynamoDB Streams GetRecords call")
    private Integer batchSize;

    public static final String PARENT_SHARD_POLL_INTERVAL_MILLIS = "--parentShardPollIntervalMillis";
    @Parameter(names = PARENT_SHARD_POLL_INTERVAL_MILLIS,
            description = "Wait for this long between polls to check if parent shards are done",
            hidden = true)
    private Long parentShardPollIntervalMillis;
}
