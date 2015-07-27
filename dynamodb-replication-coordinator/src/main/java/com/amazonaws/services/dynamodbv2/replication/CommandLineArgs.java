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

import com.beust.jcommander.Parameter;

public class CommandLineArgs {
    public static final String HELP = "--help";
    @Parameter(names = HELP, description = "Display usage information", help = true)
    private boolean help;

    public boolean getHelp() {
        return help;
    }

    public static final String ACCOUNT_ID = "--accountId";
    @Parameter(names = ACCOUNT_ID, description = "Account ID of the user", required = true)
    private String accountId;

    public String getAccountId() {
        return accountId;
    }

    public static final String METADATA_TABLE_ENDPOINT = "--metadataTableEndpoint";
    @Parameter(names = METADATA_TABLE_ENDPOINT, description = "Endpoint of the metadata table", required = true)
    private String metadataTableEndpoint;

    public String getMetadataTableEndpoint() {
        return metadataTableEndpoint;
    }

    public static final String METADATA_TABLE_NAME = "--metadataTableName";
    @Parameter(names = METADATA_TABLE_NAME, description = "Name of the metadata table", required = true)
    private String metadataTableName;

    public String getMetadataTableName() {
        return metadataTableName;
    }

    public static final String TASK_NAME = "--taskName";
    @Parameter(names = TASK_NAME, description = "Name of task, used to identify metrics in CloudWatch")
    private String taskName;

    public String getTaskName() {
        return taskName;
    }

    public static final String SERVER_HOST_PORT = "--port";
    @Parameter(names = SERVER_HOST_PORT, description = "Port that the coordinator server will be hosted at")
    public static int port = 7000;

    public int getPort() {
        return port;
    }

}
