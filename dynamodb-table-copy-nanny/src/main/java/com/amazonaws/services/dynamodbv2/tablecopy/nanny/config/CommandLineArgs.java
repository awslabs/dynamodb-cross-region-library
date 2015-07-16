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
package com.amazonaws.services.dynamodbv2.tablecopy.nanny.config;

import com.beust.jcommander.Parameter;

/**
 * Defines the command line arguments for the TableCopy Main
 */
public class CommandLineArgs {

    public static final String HELP = "--help";

    @Parameter(names = HELP, description = "Display usage information", help = true)
    private boolean help = false;

    public boolean needUsage() {
       return help;
    }

    public static final String ECS_SERVICE_NAME = "--ecsServiceName";

    @Parameter(names = ECS_SERVICE_NAME, description = "Name of the ECS service launching this task")
    private String ecsServiceName;

    public String getEcsServiceName() {
        return ecsServiceName;
    }

    public static final String METADATA_ENDPOINT = "--metadataEndpoint";
    @Parameter(names = METADATA_ENDPOINT, description = "Endpoint of the coordinator metadata table", required = true)
    private String metadataEndpoint;

    public String getMetadataEndpoint() {
        return metadataEndpoint;
    }

    public static final String METADATA_TABLE = "--metadataTable";
    @Parameter(names = METADATA_TABLE, description = "Name of the coordinator metadata table", required = true)
    private String metadataTable;

    public String getMetadataTable() {
        return metadataTable;
    }

    public static final String REPLICATION_GROUP_ID = "--replicationGroupId";
    @Parameter(names = REPLICATION_GROUP_ID, description = "Replication Group Id referred to in metadata", required = true)
    private String replicationGroupId;

    public String getReplicationGroupId() {
        return replicationGroupId;
    }

    public static final String REPLICATION_GROUP_MEMBER = "--replicationGroupMember";
    @Parameter(names = REPLICATION_GROUP_MEMBER, description = "Replication Group Member referred to in metadata", required = true)
    private String replicationGroupMember;

    public String getReplicationGroupMember() {
        return replicationGroupMember;
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

    public static final String READ_FRACTION = "--readFraction";

    @Parameter(names = READ_FRACTION, description = "Fraction of read capacity", required = true)
    private String readFraction;

    public String getReadFraction() {
        return readFraction;
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

    public static final String WRITE_FRACTION = "--writeFraction";

    @Parameter(names = WRITE_FRACTION, description = "Fraction of write capacity", required = true)
    private String writeFraction;

    public String getWriteFraction() {
        return writeFraction;
    }

    public static final String CUSTOM_TIMEOUT = "--timeout";

    @Parameter(names = CUSTOM_TIMEOUT, description = "Custom timeout for Nanny in milliseconds", required = false)
    private String customTimeout;

    public String getCustomTimeout() {
        return customTimeout;
    }
}
