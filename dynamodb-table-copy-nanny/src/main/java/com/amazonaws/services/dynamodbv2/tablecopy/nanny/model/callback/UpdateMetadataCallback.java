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

import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.request.TableCopyCallback;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TaskStatus;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.TableCopyUtils;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.log4j.Logger;

public class UpdateMetadataCallback implements TableCopyCallback {

    private static final Logger LOG = Logger.getLogger(UpdateMetadataCallback.class);

    private final String repGroupId;
    private final String repGroupMember;

    public UpdateMetadataCallback(String repGroupId, String repGroupMember) {
        this.repGroupId = repGroupId;
        this.repGroupMember = repGroupMember;
    }

    /*
     * Method used by the TableCopyClient, which will provide the corresponding replication status given the
     * status of the Table Copy task.
     */
    @Override
    public void performCallback(TaskStatus taskStatus) {
        LOG.info("TableCopy has reached terminal state, running callback for TaskStatus " + taskStatus);
        DynamoDBReplicationGroupMemberStatus updatedStatus;
        switch (taskStatus) {
            case CANCELED:
                updatedStatus = DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_CANCELLED;
                break;
            case FAILED:
            case UNRETRYABLE_FAILED:
                updatedStatus = DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_FAILED;
                break;
            case COMPLETE:
                updatedStatus = DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_COMPLETE;
                break;
            default:
                throw new RuntimeException("Unexpected TaskStatus when performing callback");
        }

        LOG.debug("Updating ReplicationGroup: " + repGroupId + ", ReplicationGroupMember: " + repGroupMember);
        boolean updateSucceeded = TableCopyUtils.markReplicationGroupMemberStatus(updatedStatus, repGroupId, repGroupMember);
        if (!updateSucceeded) {
            LOG.warn("UpdateMetadataCallback " + (updateSucceeded ? "succeeded" : " failed") + " in marking the MetadataTable " + updatedStatus);
        }
    }
}
