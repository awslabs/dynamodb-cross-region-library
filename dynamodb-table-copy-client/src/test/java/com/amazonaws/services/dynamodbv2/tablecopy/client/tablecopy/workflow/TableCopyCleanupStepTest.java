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

import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTaskHandler;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TaskStatus;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertTrue;

public class TableCopyCleanupStepTest extends EasyMockSupport {

    static final TaskStatus status = TaskStatus.COMPLETE;
    static TableCopyCleanupStep cleanupStep;
    static TableCopyStepContext context;
    static TableCopyTaskHandler handler;

    @Before
    public void setup() {
        cleanupStep = new TableCopyCleanupStep(status);
        context = createMock(TableCopyStepContext.class);
        handler = createMock(TableCopyTaskHandler.class);
    }

    @Test
    public void testNextStep() {
        expect(context.getTableCopyHandler()).andReturn(handler);

        handler.shutdown();
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() {
                return null;
            }
        });

        replayAll();
        TableCopyStep nextStep = cleanupStep.nextStepInternal(context);
        assertTrue(nextStep instanceof TableCopyCallbackStep);
        verifyAll();
    }
}
