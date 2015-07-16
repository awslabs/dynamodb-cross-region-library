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

/**
 * Step: clean up tablecopy
 * Transition: set metadata to Callback(status)
 * On failure: set metadata to Callback(unretryable failure)
 */
public class TableCopyCleanupStep extends TableCopyStep {
    private final TaskStatus internalTaskStatus;

    public TableCopyCleanupStep(TaskStatus internalTaskStatus) {
        super();
        this.internalTaskStatus = internalTaskStatus;
    }

    @Override
    protected TableCopyStep nextStepInternal(TableCopyStepContext tableCopyStepContext) {
        TableCopyTaskHandler taskHandler = tableCopyStepContext.getTableCopyHandler();
        taskHandler.shutdown();
        return new TableCopyCallbackStep(internalTaskStatus);
    }

    @Override
    protected TableCopyStep getOnFailStep() {
        return new TableCopyCallbackStep(TaskStatus.UNRETRYABLE_FAILED);
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
        return TableCopySteps.CLEANUP;
    }
}
