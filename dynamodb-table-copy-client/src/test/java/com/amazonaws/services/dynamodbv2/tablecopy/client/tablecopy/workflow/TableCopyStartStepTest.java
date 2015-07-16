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
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTracker;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TaskStatus;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertTrue;

public class TableCopyStartStepTest extends EasyMockSupport {

    static TableCopyStartStep startStep;
    static TableCopyStepContext context;
    static TableCopyTaskHandler handler;
    static TableCopyTracker tracker;
    static InputStream stdout;
    static InputStream stderr;

    @Before
    public void setup() {
        context = createMock(TableCopyStepContext.class);
        handler = createMock(TableCopyTaskHandler.class);
        tracker = createMock(TableCopyTracker.class);
        stdout = createMock(InputStream.class);
        stderr = createMock(InputStream.class);
        startStep = new TableCopyStartStep();
    }

    @Test
    public void testUnknownStatus() {
        expect(context.getTableCopyHandler()).andReturn(handler);
        expect(context.getAsyncTracker()).andReturn(tracker);


        expect(handler.getStatus()).andReturn(TaskStatus.UNKNOWN);
        handler.tableCopy();
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() {
                return null;
            }
        });

        expect(handler.getInputStream()).andReturn(stdout);
        tracker.setStdout(stdout);
        EasyMock.expectLastCall();

        expect(handler.getErrorStream()).andReturn(stderr);
        tracker.setStderr(stderr);
        EasyMock.expectLastCall();

        replayAll();
        TableCopyStep nextStep = startStep.nextStepInternal(context);
        assertTrue(nextStep instanceof TableCopyWaitStep);
        verifyAll();
    }

    @Test
    public void testActiveStatus() {
        expect(context.getTableCopyHandler()).andReturn(handler);

        expect(handler.getStatus()).andReturn(TaskStatus.ACTIVE);

        replayAll();
        TableCopyStep nextStep = startStep.nextStepInternal(context);
        assertTrue(nextStep instanceof TableCopyWaitStep);
        verifyAll();
    }
}
