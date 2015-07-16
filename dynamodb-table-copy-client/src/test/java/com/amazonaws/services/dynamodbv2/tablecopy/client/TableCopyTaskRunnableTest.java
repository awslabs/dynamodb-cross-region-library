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
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTracker;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TaskStatus;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.workflow.TableCopyStep;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.workflow.TableCopyStepContext;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for TableCopyTaskRunnable
 */

public class TableCopyTaskRunnableTest extends EasyMockSupport {

    private TableCopyTaskRunnable task;
    private TableCopyTracker tracker;

    private TableCopyRequest request;
    private TableCopyMetadataAccess metadataAccess;
    private TableCopyCallback callback;
    private TableCopyTaskHandler handler;

    @Before
    public void setupMocks() {
        tracker = new TableCopyTracker();
        request = new TableCopyRequest("source", "sourceEndpoint", 1.0, "destination", "dstEndpoint", 1.0);
        metadataAccess = createMock(TableCopyMetadataAccess.class);
        callback = createMock(TableCopyCallback.class);
        handler = createMock(TableCopyTaskHandler.class);

        task = new TableCopyTaskRunnable(request, metadataAccess, callback, tracker, handler);
    }

    @Test
    public void testHappyRun() {
        /**
         * Start at a non terminal step and execute until completion
         */
        TableCopyStep startStep = createMock(TableCopyStep.class);
        TableCopyStep endStep = createMock(TableCopyStep.class);
        expect(startStep.isTerminal()).andReturn(false);
        expect(startStep.nextStep(anyObject(TableCopyStepContext.class))).andReturn(endStep);
        expect(endStep.isTerminal()).andReturn(true);
        expect(endStep.getTaskStatus()).andReturn(TaskStatus.COMPLETE);
        expect(metadataAccess.getStep(request)).andReturn(startStep);

        replayAll();
        task.run();
        assertEquals("The async tracker's status should end as COMPLETE",
                tracker.getStatus(), TaskStatus.COMPLETE);
        verifyAll();
    }

    @Test
    public void testCanceledRun() {
        handler.shutdown();
        expect(metadataAccess.getStep(request)).andReturn(createMock(TableCopyStep.class));

        replayAll();
        task.cancel();
        assertTrue("Task should be marked as canceled", task.isCanceled());
        assertEquals("Task status should be CANCELED", tracker.getStatus(), TaskStatus.CANCELED);
        task.run();
        assertEquals("Task status should still be CANCELED", tracker.getStatus(), TaskStatus.CANCELED);
        verifyAll();
    }

    @Test
    public void testCancelPrecedence() {
        /**
         * Cancellation should take precedence over task completion (terminal step)
         */
        handler.shutdown();
        expect(metadataAccess.getStep(request)).andReturn(createMock(TableCopyStep.class));

        replayAll();
        task.cancel();
        assertTrue("Task should be marked as canceled", task.isCanceled());
        assertEquals("Task status should be CANCELED", tracker.getStatus(), TaskStatus.CANCELED);
        task.run();
        assertEquals("Task status should still be CANCELED", tracker.getStatus(), TaskStatus.CANCELED);
        verifyAll();
    }

}
