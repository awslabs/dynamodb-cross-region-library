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
package com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers;

import org.junit.Test;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for TaskTracker
 */
public class TaskTrackerTest {
    @Test
    public void testWaitForCompletion() {
        TaskTracker mockTracker = createMockBuilder(TaskTracker.class).createMock();
        expect(mockTracker.getStatus()).andReturn(TaskStatus.ACTIVE).times(1);
        expect(mockTracker.getStatus()).andReturn(TaskStatus.COMPLETE).times(1);

        replay(mockTracker);
        assertEquals("Task tracker returned an incorrect final status",
                TaskStatus.COMPLETE, mockTracker.waitForCompletion());
        verify(mockTracker);
    }
}
