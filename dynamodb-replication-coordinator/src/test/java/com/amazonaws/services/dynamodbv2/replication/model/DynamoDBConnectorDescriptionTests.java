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

import com.amazonaws.services.dynamodbv2.model.DynamoDBConnectorDescription;

public class DynamoDBConnectorDescriptionTests {

    private static void addSourceTableEndpoint(DynamoDBConnectorDescription connector) {
        connector.setSourceTableEndpoint("dynamodb.us-east-1.amazonaws.com");
    }

    private static void addSourceTableArn(DynamoDBConnectorDescription connector) {
        connector.setSourceTableArn("arn:aws:dynamodb:us-east-1:123456654321:table/testTable");
    }

    @Test
    public void testIsValid() {
        DynamoDBConnectorDescription connector = new DynamoDBConnectorDescription();

        // Add all required fields
        addSourceTableEndpoint(connector);
        addSourceTableArn(connector);

        // Make sure source table endpoint is required
        connector.setSourceTableEndpoint(null);
        assertFalse(connector.isValid());
        addSourceTableEndpoint(connector);

        // Make sure source table arn is required
        connector.setSourceTableArn(null);
        assertFalse(connector.isValid());
        addSourceTableArn(connector);

        // Verify isValid with all required fields
        assertTrue(connector.isValid());
    }
}
