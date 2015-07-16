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
package com.amazonaws.services.dynamodbv2.tablecopy.client;

import com.amazonaws.services.dynamodbv2.tablecopy.client.metadataaccess.TableCopyMetadataAccess;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.request.TableCopyCallback;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.request.TableCopyRequest;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTaskHandler;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTaskHandlerFactory;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTracker;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TaskStatus;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for DynamoDBTableCopyClient. Tests specific to how each task is executed are placed in
 * TableCopyTaskRunnableTest.
 */
public class DynamoDBTableCopyClientTest extends EasyMockSupport {
    private TableCopyMetadataAccess metadataAccessMock;
    private TableCopyTaskHandlerFactory taskHandlerFactoryMock;
    private TableCopyTaskHandler taskHandler;
    private ExecutorService tableCopyThreadpool;
    private DynamoDBTableCopyClient client;
    private TableCopyRequest request;
    private TableCopyCallback callback;


    @Before
    public void setUp() {
        metadataAccessMock = createMock(TableCopyMetadataAccess.class);
        taskHandlerFactoryMock = createMock(TableCopyTaskHandlerFactory.class);
        taskHandler = createMock(TableCopyTaskHandler.class);
        tableCopyThreadpool = createMock(ExecutorService.class);
        client = new DynamoDBTableCopyClient(metadataAccessMock, taskHandlerFactoryMock, tableCopyThreadpool);
        request = new TableCopyRequest("source", "sourceEndpoint", 1.0, "destination", "dstEndpoint", 1.0);
        callback = createMock(TableCopyCallback.class);
    }

    @Test
    public void testAsyncTableCopy() {
        expect(taskHandlerFactoryMock.createTaskHandler(request)).andReturn(taskHandler);
        expect(tableCopyThreadpool.submit(anyObject(TableCopyTaskRunnable.class)))
                .andStubReturn(createMock(Future.class));

        replayAll();
        TableCopyTracker tracker = client.launchTableCopy(request, callback);
        assertEquals("Task should be active after launching the table copy",
                tracker.getStatus(), TaskStatus.ACTIVE);
        assertTrue("Task cancellation should be enabled after launching the table copy", tracker.isCancelEnabled());
        verifyAll();
    }
}
