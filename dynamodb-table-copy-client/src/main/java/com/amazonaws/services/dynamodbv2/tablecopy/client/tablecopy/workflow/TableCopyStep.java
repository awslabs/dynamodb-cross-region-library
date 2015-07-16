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

import com.amazonaws.services.dynamodbv2.tablecopy.client.exceptions.TableCopyClientException;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TaskStatus;

/**
 * Table copy task steps
 */
public abstract class TableCopyStep {

    public TableCopyStep nextStep(TableCopyStepContext tableCopyStepContext) {
        if (isTerminal()) {
            throw new TableCopyClientException("No more steps");
        }

        TableCopyStep nextStep;
        try {
            nextStep = nextStepInternal(tableCopyStepContext);
        } catch (Exception ex) {
            // TODO: log exception somewhere
            ex.printStackTrace();
            nextStep = getOnFailStep();
        }
        tableCopyStepContext.getMetadataAccess().writeStep(nextStep);
        return nextStep;
    }

    protected TableCopyStep nextStepInternal(TableCopyStepContext tableCopyStepContext) {
        throw new TableCopyClientException("No more steps");
    }

    protected TableCopyStep getOnFailStep() {
        throw new TableCopyClientException("No more steps");
    }

    public abstract boolean isTerminal();

    public abstract TaskStatus getTaskStatus();

}
