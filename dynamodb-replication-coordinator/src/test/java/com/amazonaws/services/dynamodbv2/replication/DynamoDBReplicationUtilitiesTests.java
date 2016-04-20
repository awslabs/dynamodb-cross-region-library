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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.dynamodbv2.local.serverRunner.ServerRunner;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinitionDescription;
import com.amazonaws.services.dynamodbv2.model.DynamoDBArn;
import com.amazonaws.services.dynamodbv2.model.DynamoDBConnectorDescription;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMember;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMemberStatus;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupStatus;
import com.amazonaws.services.dynamodbv2.model.DynamoDBTableCopyDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElementDescription;
import com.amazonaws.services.dynamodbv2.model.ProjectionDescription;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDesc;
import com.amazonaws.services.dynamodbv2.model.SecondaryIndexDesc;

public class DynamoDBReplicationUtilitiesTests {
    private static final String DYNAMODB_LOCAL_ENDPOINT = "http://localhost:8000";
    private static final Logger LOG = Logger.getLogger(DynamoDBReplicationUtilities.class);
    // FIXME Replace with the new in process interface when invoking DDB Local
    private static final String[] localArgs = {"-inMemory", "-port", "8000"};
    private static final ExecutorService local = Executors.newSingleThreadExecutor();
    private static final AWSCredentials CREDS = new BasicAWSCredentials("Testing", "NoSecretsHere");
    private static final AWSCredentialsProvider CRED_PROV = new StaticCredentialsProvider(CREDS);
    private static final AccountMapToAwsAccess accounts = new AccountMapToAwsAccess();
    static {
        accounts.addAwsAccessAccount("123456654321", new AwsAccess(CRED_PROV));
    }

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

    @Test
    public void testCreateTableIfNotExistsWithIndexes() throws Exception {
        // Set up Key Schema
        List<KeySchemaElementDescription> tableKeySchema = new ArrayList<KeySchemaElementDescription>();
        tableKeySchema.add(new KeySchemaElementDescription("hashkey", "HASH"));
        tableKeySchema.add(new KeySchemaElementDescription("rangekey", "RANGE"));

        List<KeySchemaElementDescription> localIndexKeySchema = new ArrayList<KeySchemaElementDescription>();
        localIndexKeySchema.add(new KeySchemaElementDescription("hashkey", "HASH"));
        localIndexKeySchema.add(new KeySchemaElementDescription("indexRangeKey", "RANGE"));

        List<KeySchemaElementDescription> globalIndexKeySchema = new ArrayList<KeySchemaElementDescription>();
        globalIndexKeySchema.add(new KeySchemaElementDescription("indexHashKey", "HASH"));
        globalIndexKeySchema.add(new KeySchemaElementDescription("indexRangeKey", "RANGE"));

        // Set up Attribute Definitions
        List<AttributeDefinitionDescription> attributeDefinitions = new ArrayList<AttributeDefinitionDescription>();
        attributeDefinitions.add(new AttributeDefinitionDescription("hashkey", "S"));
        attributeDefinitions.add(new AttributeDefinitionDescription("rangekey", "S"));
        attributeDefinitions.add(new AttributeDefinitionDescription("indexHashKey", "S"));
        attributeDefinitions.add(new AttributeDefinitionDescription("indexRangeKey", "S"));

        // Set up ARN object
        DynamoDBArn arn = new DynamoDBArn().withAccountNumber("123456654321").withRegion("us-east-1").withTableName("testTable");

        // Set up Provisioned Throughput
        ProvisionedThroughputDesc provisionedThroughput = new ProvisionedThroughputDesc(1l, 1l);

        // Set up LSIs
        List<SecondaryIndexDesc> LSIs = new ArrayList<SecondaryIndexDesc>();
        LSIs.add(new SecondaryIndexDesc().withIndexName("lsi").withKeySchema(localIndexKeySchema)
            .withProjection(new ProjectionDescription().withProjectionType("ALL")).withProvisionedThroughput(provisionedThroughput));

        // Set up GSIs
        List<SecondaryIndexDesc> GSIs = new ArrayList<SecondaryIndexDesc>();
        GSIs.add(new SecondaryIndexDesc().withIndexName("gsi").withKeySchema(globalIndexKeySchema)
            .withProjection(new ProjectionDescription().withProjectionType("ALL")).withProvisionedThroughput(provisionedThroughput));

        // Set up the replication group member
        DynamoDBReplicationGroupMember member = new DynamoDBReplicationGroupMember().withARN(arn.getArnString()).withEndpoint(DYNAMODB_LOCAL_ENDPOINT)
            .withGlobalSecondaryIndexes(GSIs).withLocalSecondaryIndexes(LSIs).withProvisionedThroughput(provisionedThroughput).withStreamsEnabled(true);

        // Set up the replication group
        DynamoDBReplicationGroup group = new DynamoDBReplicationGroup().withAttributeDefinitions(attributeDefinitions).withKeySchema(tableKeySchema)
            .withReplicationGroupStatus(DynamoDBReplicationGroupStatus.CREATING).withReplicationGroupUUID("ReplicationGroupUUID")
            .withReplicationGroupMembers(new HashMap<String, DynamoDBReplicationGroupMember>());
        group.addReplicationGroupMember(member);

        // Test create table when it does not exist
        DynamoDBReplicationUtilities.createTableIfNotExists(group, member, accounts);
    }

    @Test
    public void testCreateTableIfNotExists() throws Exception {
        // Set up Key Schema
        List<KeySchemaElementDescription> tableKeySchema = new ArrayList<KeySchemaElementDescription>();
        tableKeySchema.add(new KeySchemaElementDescription("hashkey", "HASH"));
        tableKeySchema.add(new KeySchemaElementDescription("rangekey", "RANGE"));

        // Set up Attribute Definitions assuming base table has an index
        List<AttributeDefinitionDescription> attributeDefinitions = new ArrayList<AttributeDefinitionDescription>();
        attributeDefinitions.add(new AttributeDefinitionDescription("hashkey", "S"));
        attributeDefinitions.add(new AttributeDefinitionDescription("rangekey", "S"));
        attributeDefinitions.add(new AttributeDefinitionDescription("indexHashKey", "S"));
        attributeDefinitions.add(new AttributeDefinitionDescription("indexRangeKey", "S"));

        // Set up ARN object
        DynamoDBArn arn = new DynamoDBArn().withAccountNumber("123456654321").withRegion("us-east-1").withTableName("testTable");

        // Set up Provisioned Throughput
        ProvisionedThroughputDesc provisionedThroughput = new ProvisionedThroughputDesc(1l, 1l);

        // Set up the replication group member
        DynamoDBReplicationGroupMember member = new DynamoDBReplicationGroupMember().withARN(arn.getArnString()).withEndpoint(DYNAMODB_LOCAL_ENDPOINT)
            .withProvisionedThroughput(provisionedThroughput).withStreamsEnabled(true);

        // Set up the replication group
        DynamoDBReplicationGroup group = new DynamoDBReplicationGroup().withAttributeDefinitions(attributeDefinitions).withKeySchema(tableKeySchema)
            .withReplicationGroupStatus(DynamoDBReplicationGroupStatus.CREATING).withReplicationGroupUUID("ReplicationGroupUUID")
            .withReplicationGroupMembers(new HashMap<String, DynamoDBReplicationGroupMember>());
        group.addReplicationGroupMember(member);

        // Test create table when it does not exist
        DynamoDBReplicationUtilities.createTableIfNotExists(group, member, accounts);
    }

    @Test
    public void testIsInEnum() {
        assertTrue(DynamoDBReplicationUtilities.isInEnum("CREATING", DynamoDBReplicationGroupMemberStatus.class));
        assertFalse(DynamoDBReplicationUtilities.isInEnum("INVALID", DynamoDBReplicationGroupMemberStatus.class));
    }

    @Test
    public void testCompareSameGroupMember() {
        DynamoDBReplicationGroupMember member1 = new DynamoDBReplicationGroupMember().withARN("arn:aws:dynamodb:us-east-1:123456654321:table/testTable");
        DynamoDBReplicationGroupMember member2 = new DynamoDBReplicationGroupMember().withARN("arn:aws:dynamodb:us-east-1:123456654321:table/testTable");

        // Check the 2 members are the same if they have the same ARN
        assertEquals(DynamoDBReplicationUtilities.SAME_GROUP_MEMBER_COMPARATOR.compare(member1, member2), 0);

        // Check result if ARNs are different
        member2.setArn("arn:aws:dynamodb:us-east-1:123456654321:table/testTable2");
        assertTrue(DynamoDBReplicationUtilities.SAME_GROUP_MEMBER_COMPARATOR.compare(member1, member2) < 0);
    }

    @Test
    public void testsComparePriorityGroupMember() {
        DynamoDBReplicationGroupMember member1 = new DynamoDBReplicationGroupMember().withARN("arn:aws:dynamodb:us-east-1:654321123456:table/testTable");
        DynamoDBReplicationGroupMember member2 = new DynamoDBReplicationGroupMember().withARN("arn:aws:dynamodb:us-east-1:123456654321:table/testTable");

        // Check compare results when both does not have a bootstrap task
        assertTrue(DynamoDBReplicationUtilities.CREATING_PRIORITY_GROUP_MEMBER_COMPARATOR.compare(member1, member2) > 0);

        // Check compare results when one member does not have a bootstrap task and the other one does
        member2.setTableCopyTask(new DynamoDBTableCopyDescription());
        assertTrue(DynamoDBReplicationUtilities.CREATING_PRIORITY_GROUP_MEMBER_COMPARATOR.compare(member1, member2) < 0);

        // Check compare results when both have bootstrap tasks
        member1.setTableCopyTask(new DynamoDBTableCopyDescription());
        assertTrue(DynamoDBReplicationUtilities.CREATING_PRIORITY_GROUP_MEMBER_COMPARATOR.compare(member1, member2) > 0);
    }

    @Test
    public void testGetStackName() throws Exception {
        DynamoDBReplicationGroupMember member = new DynamoDBReplicationGroupMember().withARN(
            new DynamoDBArn().withAccountNumber("654321123456").withRegion("us-east-1").withTableName("dym-xrr-destination").getArnString()).withEndpoint(
            "dynamodb.us-east-1.amazonaws.com");
        DynamoDBConnectorDescription connector = new DynamoDBConnectorDescription().withSourceTableEndpoint("https://preview-dynamodb.eu-west-1.amazonaws.com")
            .withSourceTableArn(new DynamoDBArn().withAccountNumber("654321123456").withRegion("eu-west-1").withTableName("dym-xrr-source").getArnString());
        String hashedStackName = DynamoDBReplicationUtilities.getHashedServiceName(connector.getSourceTableArn(), member.getArn(),
            "DynamoDBReplicationConnector");
        assertEquals("DynamoDBReplicationConnectorcf59eba954739791c31f83b2228161ed", hashedStackName);
    }
}
