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

/**
 * Step: perform callback
 * Transition:
 *   if status == complete, set metadata to Complete
 *   else, set metadata to Failed
 * On failure: set metadata to Failed
 */
public class TableCopyCallbackStep extends TableCopyStep {
    private final TaskStatus internalTaskStatus;

    public TableCopyCallbackStep(TaskStatus internalTaskStatus) {
        super();
        this.internalTaskStatus = internalTaskStatus;
    }

    @Override
    protected TableCopyStep nextStepInternal(TableCopyStepContext tableCopyStepContext) {
        TableCopyCallback callback = tableCopyStepContext.getCallback();
        callback.performCallback(internalTaskStatus);

        if (internalTaskStatus == TaskStatus.COMPLETE) {
            return new TableCopyCompleteStep();
        } else {
            return new TableCopyFailedStep(internalTaskStatus);
        }
    }

    @Override
    protected TableCopyStep getOnFailStep() {
        return new TableCopyFailedStep(internalTaskStatus);
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
        return TableCopySteps.CALLBACK;
    }
}
