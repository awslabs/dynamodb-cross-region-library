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

import com.amazonaws.services.dynamodbv2.tablecopy.client.TableCopyTaskRunnable;
import com.amazonaws.services.dynamodbv2.tablecopy.client.exceptions.TableCopyClientException;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for table copy tracker
 */
public class TableCopyTrackerTest extends EasyMockSupport {
    private TableCopyTracker tracker;

    @Before
    public void setUp() {
        tracker = new TableCopyTracker();
    }

    @Test
    public void testCancelEnabled() {
        TableCopyTaskRunnable task = createMock(TableCopyTaskRunnable.class);
        task.cancel();

        replayAll();
        tracker.enableCancellation(task);
        assertTrue("Cancellation should be enabled", tracker.isCancelEnabled());
        tracker.cancel();
        verifyAll();
    }

    @Test(expected = TableCopyClientException.class)
    public void testCancelDisabled() {
        tracker.cancel();
    }

}
