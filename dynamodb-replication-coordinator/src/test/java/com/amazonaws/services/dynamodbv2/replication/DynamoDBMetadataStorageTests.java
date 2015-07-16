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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.local.serverRunner.ServerRunner;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinitionDescription;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DynamoDBArn;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMember;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMemberStatus;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupStatus;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElementDescription;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.streams.connectors.DynamoDBConnectorType;

public class DynamoDBMetadataStorageTests {
    private static final String DYNAMODB_LOCAL_ENDPOINT = "http://localhost:8000";
    private static final Logger LOG = Logger.getLogger(DynamoDBMetadataStorageTests.class);
    // FIXME Replace with the new in process interface when invoking DDB Local
    static final String[] localArgs = {"-inMemory", "-port", "8000"};
    static final ExecutorService local = Executors.newSingleThreadExecutor();
    static final String MD_TABLE_NAME = "table1";
    static final AWSCredentials CREDS = new BasicAWSCredentials("Testing", "NoSecretsHere");
    static final AWSCredentialsProvider CRED_PROV = new StaticCredentialsProvider(CREDS);
    static {
        DynamoDBMetadataStorage.init(CRED_PROV, DYNAMODB_LOCAL_ENDPOINT, MD_TABLE_NAME);
    }
    private static final DynamoDBMetadataStorage MD = DynamoDBMetadataStorage.getInstance();

    @BeforeClass
    public static void startLocal() {
        local.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.info("Starting DynamoDB Local");
                    ServerRunner.main(localArgs);
                } catch (Exception e) {
                    LOG.error(e);
                }
            }
        });
    }

    @AfterClass
    public static void stopLocal() {
        local.shutdownNow();
        LOG.info("Stopping DynamoDB Local");
    }

    @Before
    public void setupMetadataTable() {
        AmazonDynamoDBClient ddbClient = new AmazonDynamoDBClient(CRED_PROV);
        ddbClient.setEndpoint(DYNAMODB_LOCAL_ENDPOINT);
        DynamoDB ddb = new DynamoDB(ddbClient);
        ddb.createTable(new CreateTableRequest().withAttributeDefinitions(new AttributeDefinition("ReplicationGroupUUID", ScalarAttributeType.S))
            .withKeySchema(new KeySchemaElement("ReplicationGroupUUID", KeyType.HASH)).withProvisionedThroughput(new ProvisionedThroughput(1l, 1l))
            .withTableName(MD_TABLE_NAME));
    }

    @After
    public void dropMetadataTable() {
        AmazonDynamoDBClient ddbClient = new AmazonDynamoDBClient(CRED_PROV);
        ddbClient.setEndpoint(DYNAMODB_LOCAL_ENDPOINT);
        ddbClient.deleteTable(MD_TABLE_NAME);
    }

    @Test
    public void sanityListTest() {
        try {
            List<String> groups = MD.readReplicationGroups();
            assertFalse(groups == null);
            assertEquals(0, groups.size());
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testCreateReplicationGroup() {
        DynamoDBReplicationGroupMember member = new DynamoDBReplicationGroupMember().withEndpoint("https://dynamodb.us-east-1.amazonaws.com")
            .withARN(new DynamoDBArn().withAccountNumber("123456654321").withRegion("us-east-1").withTableName("testTable").getArnString())
            .withReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.CREATING).withStreamsEnabled(true);
        Map<String, DynamoDBReplicationGroupMember> members = new HashMap<String, DynamoDBReplicationGroupMember>();
        members.put(member.getArn(), member);
        DynamoDBReplicationGroup group = new DynamoDBReplicationGroup()
            .withAttributeDefinitions(Arrays.asList(new AttributeDefinitionDescription().withAttributeName("hashKey").withAttributeType("S")))
            .withKeySchema(Arrays.asList(new KeySchemaElementDescription().withAttributeName("hashKey").withKeyType("HASH")))
            .withReplicationGroupMembers(members).withReplicationGroupUUID("Group1UUID").withReplicationGroupStatus(DynamoDBReplicationGroupStatus.CREATING)
            .withConnectorType(DynamoDBConnectorType.SINGLE_MASTER_TO_READ_REPLICA);
        try {
            DynamoDBReplicationGroup result = MD.compareAndWriteReplicationGroup(null, group);
            assertEquals(result, group);
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testDeleteReplicationGroup() {
        try {
            testCreateReplicationGroup();
            List<String> groups = MD.readReplicationGroups();
            assertFalse(groups == null);
            assertEquals(1, groups.size());
            String group = groups.get(0);
            DynamoDBReplicationGroup repGroup = MD.readReplicationGroup(group);
            assertFalse(repGroup == null);
            MD.compareAndWriteReplicationGroup(repGroup, null);
            groups = MD.readReplicationGroups();
            assertFalse(groups == null);
            assertEquals(0, groups.size());
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void updateReplicationGroup() {
        try {
            testCreateReplicationGroup();
            List<String> groups = MD.readReplicationGroups();
            assertFalse(groups == null);
            assertEquals(1, groups.size());
            String group = groups.get(0);
            DynamoDBReplicationGroup repGroup = MD.readReplicationGroup(group);
            assertFalse(repGroup == null);
            assertFalse(repGroup.getAttributeDefinitions() == null);
            DynamoDBReplicationGroup group2 = new DynamoDBReplicationGroup(repGroup);
            repGroup.getAttributeDefinitions().add(new AttributeDefinitionDescription().withAttributeName("newAttribute").withAttributeType("S"));
            assertEquals(group2, MD.compareAndWriteReplicationGroup(repGroup, group2));
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }
}
