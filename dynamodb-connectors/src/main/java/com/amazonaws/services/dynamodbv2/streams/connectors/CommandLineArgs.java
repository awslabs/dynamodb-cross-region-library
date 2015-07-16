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
package com.amazonaws.services.dynamodbv2.streams.connectors;

import com.beust.jcommander.Parameter;

public class CommandLineArgs {
    public static final String HELP = "--help";
    @Parameter(names = HELP, description = "Display usage information", help = true)
    private boolean help;

    public boolean getHelp() {
        return help;
    }

    public static final String SOURCE_ENDPOINT = "--sourceEndpoint";
    @Parameter(names = SOURCE_ENDPOINT, description = "Endpoint of the source table", required = true)
    private String sourceEndpoint;

    public String getSourceEndpoint() {
        return sourceEndpoint;
    }

    public static final String SOURCE_TABLE = "--sourceTable";
    @Parameter(names = SOURCE_TABLE, description = "Name of the source table", required = true)
    private String sourceTable;

    public String getSourceTable() {
        return sourceTable;
    }

    public static final String DESTINATION_ENDPOINT = "--destinationEndpoint";
    @Parameter(names = DESTINATION_ENDPOINT, description = "Endpoint of the destination table", required = true)
    private String destinationEndpoint;

    public String getDestinationEndpoint() {
        return destinationEndpoint;
    }

    public static final String DESTINATION_TABLE = "--destinationTable";
    @Parameter(names = DESTINATION_TABLE, description = "Name of the destination table", required = true)
    private String destinationTable;

    public String getDestinationTable() {
        return destinationTable;
    }

    public static final String PIPELINE = "--pipeline";
    @Parameter(names = PIPELINE, description = "Choice of pipeline to run", required = true)
    private String pipeline;

    public String getPipeline() {
        return pipeline;
    }

    public static final String TASK_NAME = "--taskName";
    @Parameter(names = TASK_NAME, description = "Name of task, used to identify metrics in CloudWatch")
    private String taskName;

    public String getTaskName() {
        return taskName;
    }

    public static final String BATCH_SIZE = "--batchSize";
    @Parameter(names = BATCH_SIZE, description = "Number of records to request in each GetRecords call")
    private Integer batchSize;

    public Integer getBatchSize() {
        return null != batchSize ? batchSize : CommandLineInterface.STREAMS_RECORDS_LIMIT;
    }
}
