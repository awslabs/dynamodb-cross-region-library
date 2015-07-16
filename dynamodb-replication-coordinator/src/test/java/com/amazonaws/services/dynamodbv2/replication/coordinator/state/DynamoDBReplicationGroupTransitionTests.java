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
package com.amazonaws.services.dynamodbv2.replication.coordinator.state;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.datamodeling.ConversionSchema;
import com.amazonaws.services.dynamodbv2.datamodeling.ConversionSchemas;
import com.amazonaws.services.dynamodbv2.datamodeling.ItemConverter;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinitionDescription;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DynamoDBArn;
import com.amazonaws.services.dynamodbv2.model.DynamoDBConnectorDescription;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMember;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMemberStatus;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupStatus;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElementDescription;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.streams.connectors.DynamoDBConnectorType;

public class DynamoDBReplicationGroupTransitionTests {

    private static final int ACCOUNT_NUMBER_LENGTH = 12;
    private static final String ALTERNATE_TABLE_NAME = "testTable2";
    private static final String DEFAULT_TABLE_NAME = "testTable1";
    private static final ItemConverter ITEM_CONVERTER = ConversionSchemas.V2_COMPATIBLE.getConverter((new ConversionSchema.Dependencies()));

    private static String generateNumber(int length) {
        Random random = new Random();
        String generated = "";
        for (int i = 0; i < length; i++) {
            generated += random.nextInt(10 /* single digit generation */);
        }
        return generated;
    }

    private static Map<String, AttributeValue> getGroupImage(DynamoDBReplicationGroupStatus status) {
        DynamoDBReplicationGroup group = new DynamoDBReplicationGroup();

        // Key schema
        List<KeySchemaElementDescription> keySchema = new ArrayList<KeySchemaElementDescription>();
        keySchema.add(new KeySchemaElementDescription().withAttributeName("attribute0").withKeyType(KeyType.HASH.toString()));
        group.setKeySchema(keySchema);

        // Attribute Definition
        List<AttributeDefinitionDescription> attributeDefinitions = new ArrayList<AttributeDefinitionDescription>();
        attributeDefinitions.add(new AttributeDefinitionDescription().withAttributeName("attribute0").withAttributeType(ScalarAttributeType.S.toString()));
        group.setAttributeDefinitions(attributeDefinitions);

        // Replication group name
        group.setReplicationGroupUUID("Replication Group UUID");

        // Replication group status
        group.setReplicationGroupStatus(status);

        // Replication group members
        group.setReplicationGroupMembers(new HashMap<String, DynamoDBReplicationGroupMember>());

        // Replication group connector type
        group.setConnectorType(DynamoDBConnectorType.SINGLE_MASTER_TO_READ_REPLICA);

        return ITEM_CONVERTER.convert(group);
    }

    private static DynamoDBReplicationGroupMember getMember(String tableName, DynamoDBReplicationGroupMemberStatus status) {
        DynamoDBReplicationGroupMember member = new DynamoDBReplicationGroupMember();

        // ARN
        DynamoDBArn arn = new DynamoDBArn().withAccountNumber(generateNumber(ACCOUNT_NUMBER_LENGTH)).withRegion("us-east-1").withTableName(tableName);
        member.setArn(arn.getArnString());

        // Endpoint
        String endpoint = "dynamodb.us-east-1.amazonaws.com";
        member.setEndpoint(endpoint);

        // Status
        member.setReplicationGroupMemberStatus(status);

        // Streams Enabled
        member.setStreamsEnabled(true);

        return member;
    }

    @Test(expected = IllegalStateException.class)
    public void testNullStreamRecord() {
        DynamoDBReplicationGroupTransition.getTransition(null /* null Stream record */);
    }

    @Test(expected = IllegalStateException.class)
    public void testOnlyAttributeDefinitionChanged() {
        Map<String, AttributeValue> oldGroupImage = getGroupImage(DynamoDBReplicationGroupStatus.ACTIVE);
        Map<String, AttributeValue> newGroupImage = getGroupImage(DynamoDBReplicationGroupStatus.ACTIVE);
        DynamoDBReplicationGroup newGroup = ITEM_CONVERTER.unconvert(DynamoDBReplicationGroup.class, newGroupImage);
        newGroup.setAttributeDefinitions(new ArrayList<AttributeDefinitionDescription>());
        newGroupImage = ITEM_CONVERTER.convert(newGroup);

        // Make Stream record
        StreamRecord streamRecord = new StreamRecord().withOldImage(oldGroupImage).withNewImage(newGroupImage);
        DynamoDBReplicationGroupTransition.getTransition(streamRecord);
    }

    @Test
    public void testCreatingGroup() {
        StreamRecord streamRecord = new StreamRecord().withNewImage(getGroupImage(DynamoDBReplicationGroupStatus.CREATING));
        assertTrue(DynamoDBReplicationGroupTransition.getTransition(streamRecord) instanceof DynamoDBReplicationGroupCreationStarted);
    }

    @Test
    public void testDeletingGroup() {
        StreamRecord streamRecord = new StreamRecord().withOldImage(getGroupImage(DynamoDBReplicationGroupStatus.DELETING));
        assertTrue(DynamoDBReplicationGroupTransition.getTransition(streamRecord) instanceof DynamoDBReplicationGroupDeletionCompleted);
    }

    @Test(expected = IllegalStateException.class)
    public void testGroupTransitionFromDeleting() {
        StreamRecord streamRecord = new StreamRecord().withOldImage(getGroupImage(DynamoDBReplicationGroupStatus.DELETING)).withNewImage(
            getGroupImage(DynamoDBReplicationGroupStatus.ACTIVE));
        DynamoDBReplicationGroupTransition.getTransition(streamRecord);
    }

    @Test(expected = IllegalStateException.class)
    public void testGroupTransitionFromActive() {
        StreamRecord streamRecord = new StreamRecord().withOldImage(getGroupImage(DynamoDBReplicationGroupStatus.ACTIVE));

        // ACTIVE to DELETING
        streamRecord.setNewImage(getGroupImage(DynamoDBReplicationGroupStatus.DELETING));
        assertTrue(DynamoDBReplicationGroupTransition.getTransition(streamRecord) instanceof DynamoDBReplicationGroupDeletionStarted);

        // ACTIVE to UPDATING
        Map<String, AttributeValue> newGroupImage = getGroupImage(DynamoDBReplicationGroupStatus.UPDATING);
        DynamoDBReplicationGroup group = ITEM_CONVERTER.unconvert(DynamoDBReplicationGroup.class, newGroupImage);
        group.addReplicationGroupMember(getMember(DEFAULT_TABLE_NAME, DynamoDBReplicationGroupMemberStatus.UPDATING));
        newGroupImage = ITEM_CONVERTER.convert(group);
        streamRecord.setNewImage(newGroupImage);
        assertTrue(DynamoDBReplicationGroupTransition.getTransition(streamRecord) instanceof DynamoDBReplicationGroupUpdateStarted);

        // ACTIVE to CREATING not valid
        streamRecord.setNewImage(getGroupImage(DynamoDBReplicationGroupStatus.CREATING));
        DynamoDBReplicationGroupTransition.getTransition(streamRecord);
    }

    @Test(expected = IllegalStateException.class)
    public void testGroupTransitionFromUpdating() {
        StreamRecord streamRecord = new StreamRecord().withOldImage(getGroupImage(DynamoDBReplicationGroupStatus.UPDATING));

        // UPDATING to DELETING
        streamRecord.setNewImage(getGroupImage(DynamoDBReplicationGroupStatus.DELETING));
        assertTrue(DynamoDBReplicationGroupTransition.getTransition(streamRecord) instanceof DynamoDBReplicationGroupDeletionStarted);

        // UPDATING to ACTIVE
        streamRecord.setNewImage(getGroupImage(DynamoDBReplicationGroupStatus.ACTIVE));
        assertTrue(DynamoDBReplicationGroupTransition.getTransition(streamRecord) instanceof DynamoDBReplicationGroupUpdateCompleted);

        // UPDATING to CREATING not valid
        streamRecord.setNewImage(getGroupImage(DynamoDBReplicationGroupStatus.CREATING));
        DynamoDBReplicationGroupTransition.getTransition(streamRecord);
    }

    @Test(expected = IllegalStateException.class)
    public void testGroupTransitionFromCreating() {
        StreamRecord streamRecord = new StreamRecord().withOldImage(getGroupImage(DynamoDBReplicationGroupStatus.CREATING));

        // CREATING to DELETING
        streamRecord.setNewImage(getGroupImage(DynamoDBReplicationGroupStatus.DELETING));
        assertTrue(DynamoDBReplicationGroupTransition.getTransition(streamRecord) instanceof DynamoDBReplicationGroupDeletionStarted);

        // CREATING to ACTIVE
        streamRecord.setNewImage(getGroupImage(DynamoDBReplicationGroupStatus.ACTIVE));
        assertTrue(DynamoDBReplicationGroupTransition.getTransition(streamRecord) instanceof DynamoDBReplicationGroupCreationCompleted);

        // CREATING to UPDATING not valid
        streamRecord.setNewImage(getGroupImage(DynamoDBReplicationGroupStatus.UPDATING));
        DynamoDBReplicationGroupTransition.getTransition(streamRecord);
    }

    @Test(expected = IllegalStateException.class)
    public void testMultipleMemberCreation() {
        StreamRecord streamRecord = new StreamRecord();
        Map<String, AttributeValue> oldGroupImage = getGroupImage(DynamoDBReplicationGroupStatus.ACTIVE);

        Map<String, AttributeValue> newGroupImage = getGroupImage(DynamoDBReplicationGroupStatus.UPDATING);

        // add group members to new group
        DynamoDBReplicationGroup newGroup = ITEM_CONVERTER.unconvert(DynamoDBReplicationGroup.class, newGroupImage);
        newGroup.addReplicationGroupMember(getMember(DEFAULT_TABLE_NAME, DynamoDBReplicationGroupMemberStatus.CREATING));
        newGroup.addReplicationGroupMember(getMember(ALTERNATE_TABLE_NAME, DynamoDBReplicationGroupMemberStatus.CREATING));
        newGroupImage = ITEM_CONVERTER.convert(newGroup);

        // add to stream record
        streamRecord.setOldImage(oldGroupImage);
        streamRecord.setNewImage(newGroupImage);

        DynamoDBReplicationGroupTransition.getTransition(streamRecord).transition(null /* not needed for testing */, null /* not needed for testing */);
    }

    @Test(expected = IllegalStateException.class)
    public void testMultipleMemberDeletion() {
        StreamRecord streamRecord = new StreamRecord();
        Map<String, AttributeValue> oldGroupImage = getGroupImage(DynamoDBReplicationGroupStatus.ACTIVE);

        // add group members to old group
        DynamoDBReplicationGroup oldGroup = ITEM_CONVERTER.unconvert(DynamoDBReplicationGroup.class, oldGroupImage);
        oldGroup.addReplicationGroupMember(getMember(DEFAULT_TABLE_NAME, DynamoDBReplicationGroupMemberStatus.DELETING));
        oldGroup.addReplicationGroupMember(getMember(ALTERNATE_TABLE_NAME, DynamoDBReplicationGroupMemberStatus.DELETING));
        oldGroupImage = ITEM_CONVERTER.convert(oldGroup);

        Map<String, AttributeValue> newGroupImage = getGroupImage(DynamoDBReplicationGroupStatus.UPDATING);

        // add to stream record
        streamRecord.setOldImage(oldGroupImage);
        streamRecord.setNewImage(newGroupImage);

        DynamoDBReplicationGroupTransition.getTransition(streamRecord).transition(null /* not needed for testing */, null /* not needed for testing */);
    }

    @Test(expected = IllegalStateException.class)
    public void testMultipleMemberModification() {
        StreamRecord streamRecord = new StreamRecord();
        Map<String, AttributeValue> oldGroupImage = getGroupImage(DynamoDBReplicationGroupStatus.ACTIVE);

        // add group members to old group
        DynamoDBReplicationGroup oldGroup = ITEM_CONVERTER.unconvert(DynamoDBReplicationGroup.class, oldGroupImage);
        oldGroup.addReplicationGroupMember(getMember(DEFAULT_TABLE_NAME, DynamoDBReplicationGroupMemberStatus.ACTIVE));
        oldGroup.addReplicationGroupMember(getMember(ALTERNATE_TABLE_NAME, DynamoDBReplicationGroupMemberStatus.WAITING));
        oldGroupImage = ITEM_CONVERTER.convert(oldGroup);

        Map<String, AttributeValue> newGroupImage = getGroupImage(DynamoDBReplicationGroupStatus.UPDATING);

        // add group members to new group
        DynamoDBReplicationGroup newGroup = ITEM_CONVERTER.unconvert(DynamoDBReplicationGroup.class, newGroupImage);
        newGroup.addReplicationGroupMember(getMember(DEFAULT_TABLE_NAME, DynamoDBReplicationGroupMemberStatus.UPDATING));
        newGroup.addReplicationGroupMember(getMember(ALTERNATE_TABLE_NAME, DynamoDBReplicationGroupMemberStatus.ACTIVE));

        // add to stream record
        streamRecord.setOldImage(oldGroupImage);
        streamRecord.setNewImage(newGroupImage);

        DynamoDBReplicationGroupTransition.getTransition(streamRecord).transition(null /* not needed for testing */, null /* not needed for testing */);
    }

    @Test(expected = IllegalStateException.class)
    public void testMemberDirectTransition() {
        /*
         * Test single member deletion
         */
        StreamRecord streamRecord = new StreamRecord();
        Map<String, AttributeValue> oldGroupImage = getGroupImage(DynamoDBReplicationGroupStatus.CREATING);

        // add group members to old group
        DynamoDBReplicationGroup oldGroup = ITEM_CONVERTER.unconvert(DynamoDBReplicationGroup.class, oldGroupImage);
        DynamoDBReplicationGroupMember deletingMember = getMember(DEFAULT_TABLE_NAME, DynamoDBReplicationGroupMemberStatus.DELETING);
        oldGroup.addReplicationGroupMember(deletingMember);
        oldGroupImage = ITEM_CONVERTER.convert(oldGroup);

        Map<String, AttributeValue> newGroupImage = getGroupImage(DynamoDBReplicationGroupStatus.CREATING);

        // add to stream record
        streamRecord.setOldImage(oldGroupImage);
        streamRecord.setNewImage(newGroupImage);

        // check that direct transition class is invoked when processing the Streams record
        assertTrue(DynamoDBReplicationGroupTransition.getTransition(streamRecord) instanceof DynamoDBReplicationGroupMemberDirectTransition);

        /*
         * Test single member starts deleting
         */
        DynamoDBReplicationGroupMember creatingMember = new DynamoDBReplicationGroupMember(deletingMember);
        creatingMember.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.CREATING);

        // reset old group
        oldGroup.removeReplicationGroupMember(deletingMember.getArn());
        oldGroup.addReplicationGroupMember(creatingMember);
        oldGroupImage = ITEM_CONVERTER.convert(oldGroup);

        // reset new group
        DynamoDBReplicationGroup newGroup = ITEM_CONVERTER.unconvert(DynamoDBReplicationGroup.class, newGroupImage);
        newGroup.addReplicationGroupMember(deletingMember);
        newGroupImage = ITEM_CONVERTER.convert(newGroup);

        // add to stream record
        streamRecord.setOldImage(oldGroupImage);
        streamRecord.setNewImage(newGroupImage);

        // check that direct transition class is invoked when processing the Streams record
        assertTrue(DynamoDBReplicationGroupTransition.getTransition(streamRecord) instanceof DynamoDBReplicationGroupMemberDirectTransition);

        /*
         * Test invalid single member change
         */
        DynamoDBReplicationGroupMember updatingMember = new DynamoDBReplicationGroupMember(creatingMember);
        updatingMember.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.UPDATING);

        // reset oldGroup
        oldGroup.removeReplicationGroupMember(creatingMember.getArn());
        oldGroup.addReplicationGroupMember(updatingMember);
        oldGroupImage = ITEM_CONVERTER.convert(oldGroup);

        // reset newGroup
        newGroup.removeReplicationGroupMember(deletingMember.getArn());
        updatingMember.setConnectors(new ArrayList<DynamoDBConnectorDescription>());
        newGroup.addReplicationGroupMember(updatingMember);
        newGroupImage = ITEM_CONVERTER.convert(newGroup);

        // add to stream record
        streamRecord.setOldImage(oldGroupImage);
        streamRecord.setNewImage(newGroupImage);

        // try to get the transition, should throw an exception as we do not support adding a bootstrap task to a member
        DynamoDBReplicationGroupTransition.getTransition(streamRecord);
    }

    @Test
    public void testMemberPriorityTransition() {
        /*
         * Test single member creation
         */
        StreamRecord streamRecord = new StreamRecord();
        Map<String, AttributeValue> oldGroupImage = getGroupImage(DynamoDBReplicationGroupStatus.CREATING);
        Map<String, AttributeValue> newGroupImage = getGroupImage(DynamoDBReplicationGroupStatus.CREATING);

        // add group member to new group
        DynamoDBReplicationGroup newGroup = ITEM_CONVERTER.unconvert(DynamoDBReplicationGroup.class, newGroupImage);
        DynamoDBReplicationGroupMember creatingMember = getMember(DEFAULT_TABLE_NAME, DynamoDBReplicationGroupMemberStatus.CREATING);
        newGroup.addReplicationGroupMember(creatingMember);
        newGroupImage = ITEM_CONVERTER.convert(newGroup);

        // add to stream record
        streamRecord.setOldImage(oldGroupImage);
        streamRecord.setNewImage(newGroupImage);

        // check that priority transition class is invoked when processing the Streams record
        assertTrue(DynamoDBReplicationGroupTransition.getTransition(streamRecord) instanceof DynamoDBReplicationGroupMemberPriorityTransition);

        /*
         * Test single member started creation, moving to waiting
         */
        DynamoDBReplicationGroupMember waitingMember = new DynamoDBReplicationGroupMember(creatingMember);
        waitingMember.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.WAITING);

        // reset old group
        DynamoDBReplicationGroup oldGroup = ITEM_CONVERTER.unconvert(DynamoDBReplicationGroup.class, oldGroupImage);
        oldGroup.addReplicationGroupMember(creatingMember);
        oldGroupImage = ITEM_CONVERTER.convert(oldGroup);

        // reset new group
        newGroup.removeReplicationGroupMember(creatingMember.getArn());
        newGroup.addReplicationGroupMember(waitingMember);
        newGroupImage = ITEM_CONVERTER.convert(newGroup);

        // add to stream record
        streamRecord.setOldImage(oldGroupImage);
        streamRecord.setNewImage(newGroupImage);

        // check that priority transition class is invoked when processing the Streams record
        assertTrue(DynamoDBReplicationGroupTransition.getTransition(streamRecord) instanceof DynamoDBReplicationGroupMemberPriorityTransition);
    }
}
