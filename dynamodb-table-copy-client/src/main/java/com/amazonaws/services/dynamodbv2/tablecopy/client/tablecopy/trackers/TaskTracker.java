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

/**
 * Abstract class for tracking the status of a tablecopy.
 */
public abstract class TaskTracker {
    private final static long COMPLETION_POLLING_PERIOD_MS = 10000;

    public abstract TaskStatus getStatus();

    /**
     * Polls for the tablecopy status and sleeps until the tablecopy completes
     * @return - the final status
     */
    public TaskStatus waitForCompletion() {
        TaskStatus status = getStatus();
        while (!status.isFinished()) {
            try {
                Thread.sleep(COMPLETION_POLLING_PERIOD_MS);
            } catch (InterruptedException ex) {
                // No need to do anything
            }
            status = getStatus();
        }
        return status;
    }
}
