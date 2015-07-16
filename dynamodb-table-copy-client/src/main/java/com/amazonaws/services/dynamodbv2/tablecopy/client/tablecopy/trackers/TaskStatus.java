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

public enum TaskStatus {

    /**
     * Task state cannot be determined
     */
    UNKNOWN(false, "UNKNOWN"),

    /**
     * Task is running
     */
    ACTIVE(false, "ACTIVE"),

    /**
     * Task was canceled
     */
    CANCELED(true, "CANCELED"),

    /**
     * Task completed unsuccessfully but can be retried
     */
    FAILED(true, "FAILED"),

    /**
     * Task completed unsuccessfully and cannot be retried
     */
    UNRETRYABLE_FAILED(true, "UNRETRYABLE_FAILED"),

    /**
     * Task completed successfully
     */
    COMPLETE(true, "COMPLETE");

    private final boolean isFinished;
    private final String value;

    private TaskStatus(boolean isFinished, String value) {
        this.isFinished = isFinished;
        this.value = value;
    }

    /**
     * Returns true if the status represents a tablecopy that has completed
     */
    public boolean isFinished() {
        return this.isFinished;
    }

    /**
     * Returns the enum associated with its String
     * @param status
     * @return
     */
    public static TaskStatus fromString(String status) {
        if (status == null) {
            return null;
        }

        for (TaskStatus taskStatus : TaskStatus.values()) {
            if (taskStatus.toString().equals(status)) {
                return taskStatus;
            }
        }
        
        throw new IllegalArgumentException("Found an invalid status: " + status);
    }

    /**
     * Returns String representation of the Enum.
     * @return
     */
    public String toString() {
        return this.value;
    }
}
