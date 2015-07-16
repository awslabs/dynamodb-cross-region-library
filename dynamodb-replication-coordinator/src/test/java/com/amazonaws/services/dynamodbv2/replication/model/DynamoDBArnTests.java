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
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.DynamoDBArn;

public class DynamoDBArnTests {

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidArn() {
        DynamoDBArn arn = new DynamoDBArn();

        // Set invalid ARN string
        arn.setArnString("test");
    }

    @Test
    public void testValidArn() {
        DynamoDBArn arn = new DynamoDBArn();
        // Set all required fields
        arn.setAccountNumber("123456654321");
        arn.setRegion("us-east-1");
        arn.setTableName("testTable");

        // Check the arn string is generated correctly
        assertEquals(arn.getArnString(), "arn:aws:dynamodb:us-east-1:123456654321:table/testTable");
    }

    @Test
    public void testCompareArn() {
        DynamoDBArn arn1 = new DynamoDBArn();
        DynamoDBArn arn2 = new DynamoDBArn();

        // Set all required fields for arn1
        arn1.setAccountNumber("123456654321");
        arn1.setRegion("us-east-1");
        arn1.setTableName("table1");

        // Set all required fields for arn2
        arn2.setAccountNumber("123456654321");
        arn2.setRegion("us-east-1");
        arn2.setTableName("table1");

        // Check comparison results of the 2 arns
        assertEquals(arn1.compareTo(arn2), 0);

        // Check account number is compared first
        arn2.setAccountNumber("654321123456");
        assertTrue(arn1.compareTo(arn2) < 0);

        // Check region is compared after account number
        arn2.setAccountNumber("123456654321");
        arn2.setRegion("eu-west-1");
        assertTrue(arn1.compareTo(arn2) > 0);

        // Check table name is compared last after account number and region
        arn2.setRegion("us-east-1");
        arn2.setTableName("table2");
        assertTrue(arn1.compareTo(arn2) < 0);
    }

}
