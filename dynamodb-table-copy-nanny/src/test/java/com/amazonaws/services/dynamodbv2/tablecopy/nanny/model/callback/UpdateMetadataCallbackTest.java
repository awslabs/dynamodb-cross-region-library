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
package com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.callback;

import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TaskStatus;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.TableCopyUtils;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMemberStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.easymock.EasyMock.expect;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TableCopyUtils.class)
public class UpdateMetadataCallbackTest {

    static final boolean success = true;
    static final String repGroupId = "MyRandomUUID";
    static final String repGroupMember = "MyRepGroupMember";
    static final String metadataTable = "MetadataTable";

    static UpdateMetadataCallback callback;

    @Before
    public void setup() {
        callback = new UpdateMetadataCallback(repGroupId, repGroupMember);
        PowerMock.mockStatic(TableCopyUtils.class);
    }

    @Test
    public void testPerformCallBackFailed() {
        expect(TableCopyUtils.markReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_FAILED,
                repGroupId, repGroupMember)).andReturn(true);

        PowerMock.replayAll();

        callback.performCallback(TaskStatus.FAILED);

        PowerMock.verifyAll();
    }

    @Test
    public void testPerformCallBackUnretryableFailed() {
        expect(TableCopyUtils.markReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_FAILED,
                repGroupId, repGroupMember)).andReturn(true);

        PowerMock.replayAll();

        callback.performCallback(TaskStatus.UNRETRYABLE_FAILED);

        PowerMock.verifyAll();
    }

    @Test
    public void testPerformCallBackComplete() {
        expect(TableCopyUtils.markReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_COMPLETE,
                repGroupId, repGroupMember)).andReturn(true);

        PowerMock.replayAll();

        callback.performCallback(TaskStatus.COMPLETE);

        PowerMock.verifyAll();
    }

}
