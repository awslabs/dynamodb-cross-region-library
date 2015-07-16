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
package com.amazonaws.services.dynamodbv2.replication.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinitionDescription;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMember;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupStatus;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElementDescription;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.streams.connectors.DynamoDBConnectorType;

public class DynamoDBReplicationGroupTests {

    private static void addAttributeDefintions(DynamoDBReplicationGroup group) {
        List<AttributeDefinitionDescription> attributeDefinitions = new ArrayList<AttributeDefinitionDescription>();
        attributeDefinitions.add(new AttributeDefinitionDescription().withAttributeName("attribute0").withAttributeType(
            ScalarAttributeType.S.toString()));
        group.setAttributeDefinitions(attributeDefinitions);
    }

    private static void addKeySchema(DynamoDBReplicationGroup group) {
        List<KeySchemaElementDescription> keySchema = new ArrayList<KeySchemaElementDescription>();
        keySchema.add(new KeySchemaElementDescription().withAttributeName("attribute0").withKeyType(KeyType.HASH.toString()));
        group.setKeySchema(keySchema);
    }

    private static void addGroupMembers(DynamoDBReplicationGroup group){
        Map<String, DynamoDBReplicationGroupMember> members = new HashMap<String, DynamoDBReplicationGroupMember>();
        group.setReplicationGroupMembers(members);
    }

    private static void addGroupName(DynamoDBReplicationGroup group){
        group.setReplicationGroupUUID("ReplicationGroupUUID");
    }

    private static void addGroupStatus(DynamoDBReplicationGroup group){
        group.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.CREATING);
    }

    private static void addConnectorType(DynamoDBReplicationGroup group) {
        group.setConnectorType(DynamoDBConnectorType.SINGLE_MASTER_TO_READ_REPLICA);
    }

    @Test
    public void testIsValid(){
        DynamoDBReplicationGroup group = new DynamoDBReplicationGroup();

        // Add all required fields
        addAttributeDefintions(group);
        addGroupMembers(group);
        addGroupName(group);
        addGroupStatus(group);
        addKeySchema(group);
        addConnectorType(group);

        // Make sure attribute defintions are required
        group.setAttributeDefinitions(null);
        assertFalse(group.isValid());
        addAttributeDefintions(group);

        // Make sure key schema is required
        group.setKeySchema(null);
        assertFalse(group.isValid());
        addKeySchema(group);

        // Make sure group members are required
        group.setReplicationGroupMembers(null);
        assertFalse(group.isValid());
        addGroupMembers(group);

        // Make sure group UUID is required
        group.setReplicationGroupUUID(null);
        assertFalse(group.isValid());
        addGroupName(group);

        // Make sure group status is required
        group.setReplicationGroupStatus(null);
        assertFalse(group.isValid());
        addGroupStatus(group);

        // Make sure group connector type is required
        group.setConnectorType(null);
        assertFalse(group.isValid());
        addConnectorType(group);

        // Verify isValid with all required fields
        assertTrue(group.isValid());
    }

    @Test
    public void testVersionNotInEqualsOrHashCode() {
        DynamoDBReplicationGroup group1 = new DynamoDBReplicationGroup().withVersion(0l);
        DynamoDBReplicationGroup group2 = new DynamoDBReplicationGroup().withVersion(1l);
        assertEquals(group1, group2);
        assertEquals(group1.hashCode(), group2.hashCode());
    }

}
