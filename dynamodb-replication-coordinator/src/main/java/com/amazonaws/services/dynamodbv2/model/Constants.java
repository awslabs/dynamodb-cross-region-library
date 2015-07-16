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
package com.amazonaws.services.dynamodbv2.model;

import java.nio.charset.Charset;

/**
 * Constants for JSON and DynamoDB Mapper members.
 */
public final class Constants {
    public static final String ARGUMENTS = "Arguments";
    public static final String ARN = "ARN";
    public static final String ATTRIBUTE_DEFINITIONS = "AttributeDefinitions";
    public static final String ATTRIBUTE_NAME = "AttributeName";
    public static final String ATTRIBUTE_TYPE = "AttributeType";
    public static final String TABLE_COPY_TASK = "TableCopyTask";
    public static final Charset ENCODING = Charset.forName("UTF-8");
    public static final String COMMAND = "Command";
    public static final String CONFIGURATION = "Configuration";
    public static final String CONNECTOR_TYPE = "ConnectorType";
    public static final String CONNECTORS = "Connectors";
    public static final String DEFAULT_CLUSTER_NAME = "DynamoDBCrossRegionReplication";
    public static final String ENDPOINT = "Endpoint";
    public static final String EXCLUSIVE_START_REPLICATION_GROUP_NAME = "ExclusiveStartReplicationGroupName";
    public static final String GLOBAL_SECONDARY_INDEXES = "GlobalSecondaryIndexes";
    public static final String INDEX_NAME = "IndexName";
    public static final String KEY_SCHEMA = "KeySchema";
    public static final String KEY_TYPE = "KeyType";
    public static final String LIMIT = "Limit";
    public static final String LOCAL_SECONDARY_INDEXES = "LocalSecondaryIndexes";
    public static final String MEMBER_TABLE_NAME = "MemberTableName";
    public static final String NON_KEY_ATTRIBUTES = "NonKeyAttributes";
    public static final String PROJECTION = "Projection";
    public static final String PROJECTION_TYPE = "ProjectionType";
    public static final String PROVISIONED_THROUGHPUT = "ProvisionedThroughput";
    public static final String READ_CAPACITY_UNITS = "ReadCapacityUnits";
    public static final String REPLICATION_GROUP_MEMBER_STATUS = "ReplicationGroupMemberStatus";
    public static final String REPLICATION_GROUP_MEMBERS = "ReplicationGroupMembers";
    public static final String REPLICATION_GROUP_NAME = "ReplicationGroupName";
    public static final String REPLICATION_GROUP_STATUS = "ReplicationGroupStatus";
    public static final String REPLICATION_GROUP_UUID = "ReplicationGroupUUID";
    public static final String REPLICATION_GROUPS = "ReplicationGroups";
    public static final String SOURCE_TABLE_ENDPOINT = "SourceTableEndpoint";
    public static final String SOURCE_TABLE_ARN = "SourceTableArn";
    public static final String STREAM_ENABLED = "StreamEnabled";
    public static final String VERSION = "Version";
    public static final String WRITE_CAPACITY_UNITS = "WriteCapacityUnits";
    public static final String USER_AGENT = "DynamoDBReplicationCoordinator-1.0";

    /**
     * Private constructor for a static class.
     */
    private Constants() {

    }
}
