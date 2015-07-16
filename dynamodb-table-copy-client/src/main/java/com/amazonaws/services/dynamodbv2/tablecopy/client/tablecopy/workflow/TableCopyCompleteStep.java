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

import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TaskStatus;

/**
 * Terminal complete state
 */
public class TableCopyCompleteStep extends TableCopyStep {
    @Override
    public boolean isTerminal() {
        return true;
    }

    @Override
    public TaskStatus getTaskStatus() {
        return TaskStatus.COMPLETE;
    }

    public String toString() {
        return TableCopySteps.COMPLETE;
    }
}
