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

/**
 * Step: create table copy if it does not already exist
 * Transition: set metadata to Waiting
 * On failure: set metadata to Cleanup(failed)
 */
public class TableCopyStartStep extends TableCopyStep {
    @Override
    protected TableCopyStep nextStepInternal(TableCopyStepContext tableCopyStateContext) {
        TableCopyTaskHandler taskHandler = tableCopyStateContext.getTableCopyHandler();

        if (!(taskHandler.getStatus() == TaskStatus.ACTIVE)) {
            taskHandler.tableCopy();
            TableCopyTracker tracker = tableCopyStateContext.getAsyncTracker();
            tracker.setStdout(taskHandler.getInputStream());
            tracker.setStderr(taskHandler.getErrorStream());
        }
        
        return new TableCopyWaitStep();
    }

    @Override
    protected TableCopyStep getOnFailStep() {
        return new TableCopyCleanupStep(TaskStatus.FAILED);
    }

    @Override
    public boolean isTerminal() {
        return false;
    }

    @Override
    public TaskStatus getTaskStatus() {
        return TaskStatus.ACTIVE;
    }

    public String toString() {
        return TableCopySteps.START;
    }
}
