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
package com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.workflow;


import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.request.TableCopyCallback;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TaskStatus;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertTrue;

public class TableCopyCallbackStepTest extends EasyMockSupport {

    static TableCopyCallbackStep callbackStep;
    static TableCopyStepContext context;

    @Before
    public void setup() {
        context = createMock(TableCopyStepContext.class);
    }

    @Test
    public void testNextStepInternalComplete() {
        testNextStepWithStatus(TaskStatus.COMPLETE);
    }

    @Test
    public void testNextStepInternalFailed() {
        testNextStepWithStatus(TaskStatus.FAILED);
    }

    @Test
    public void testNextStepInternalUnretryableFailed() {
        testNextStepWithStatus(TaskStatus.UNRETRYABLE_FAILED);
    }

    @Test
    public void testNextStepInternalCanceled() {
        testNextStepWithStatus(TaskStatus.CANCELED);
    }

    private void testNextStepWithStatus(TaskStatus status) {
        callbackStep = new TableCopyCallbackStep(status);

        TableCopyCallback mockCallback = createMock(TableCopyCallback.class);
        mockCallback.performCallback(status);
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
                @Override
                public Void answer() {
                    return null;
                }
            });

        expect(context.getCallback()).andReturn(mockCallback);

        replayAll();
        TableCopyStep nextStep = callbackStep.nextStepInternal(context);
        if (status == TaskStatus.COMPLETE) {
            assertTrue("We should end on the complete state", nextStep instanceof TableCopyCompleteStep);
        } else {
            assertTrue("We should end on the failed state", nextStep instanceof TableCopyFailedStep);
        }
        verifyAll();
    }
}
