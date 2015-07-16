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
package com.amazonaws.services.dynamodbv2.tablecopy.client.metadataaccess.impl;

import com.amazonaws.services.dynamodbv2.tablecopy.client.metadataaccess.TableCopyMetadataAccess;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.request.TableCopyRequest;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.workflow.TableCopyStartStep;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.workflow.TableCopyWaitStep;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for RestartMetadataAccess
 */
public class RestartMetadataAccessTest {
    private TableCopyMetadataAccess mdAccess;
    private TableCopyRequest request;

    @Before
    public void setUp() {
        mdAccess = new RestartMetadataAccess();
        request = new TableCopyRequest("source", "sourceEndpoint", 1.0, "destination", "dstEndpoint", 1.0);
    }

    @Test
    public void testGetStep() {
        assertTrue("This should always return a new start step",
                mdAccess.getStep(request) instanceof TableCopyStartStep);
    }

    @Test
    public void testIgnoreWriteStep() {
        mdAccess.writeStep(new TableCopyWaitStep());
        assertTrue("This should always return a new start step",
                mdAccess.getStep(request) instanceof TableCopyStartStep);
    }
}
