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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.DynamoDBArn;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMember;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMemberStatus;

public class DynamoDBReplicationGroupMemberTests {

    private static final String DEFAULT_DYNAMODB_ENDPOINT = "dynamodb.us-east-1.amazonaws.com";

    private static void addEndpoint(DynamoDBReplicationGroupMember member) {
        String endpoint = DEFAULT_DYNAMODB_ENDPOINT;
        member.setEndpoint(endpoint);
    }

    private static void addArn(DynamoDBReplicationGroupMember member) {
        DynamoDBArn arn = new DynamoDBArn().withAccountNumber("123456654321").withRegion("us-east-1").withTableName("testTable");
        member.setArn(arn.getArnString());
    }

    private static void addMemberStatus(DynamoDBReplicationGroupMember member) {
        member.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.ACTIVE);
    }

    private void setStreamsEnabled(DynamoDBReplicationGroupMember member) {
        member.setStreamsEnabled(true);
    }

    @Test
    public void testIsValid() {
        DynamoDBReplicationGroupMember member = new DynamoDBReplicationGroupMember();

        // Add all required fields
        addEndpoint(member);
        addArn(member);
        addMemberStatus(member);
        setStreamsEnabled(member);

        // Make sure endpoint is required
        member.setEndpoint(null);
        assertFalse(member.isValid());
        addEndpoint(member);

        // Make sure arn is required
        member.setArn(null);
        assertFalse(member.isValid());
        addArn(member);

        // Make sure member status is required
        member.setReplicationGroupMemberStatus(null);
        assertFalse(member.isValid());
        addMemberStatus(member);

        // Verify isValid with all required fields
        assertTrue(member.isValid());
    }


}
