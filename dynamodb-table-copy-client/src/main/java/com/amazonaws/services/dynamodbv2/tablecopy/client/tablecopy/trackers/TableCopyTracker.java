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

import java.io.InputStream;

/**
 * Tracker for the table copy task. This allows cancellation of the task.
 */
public class TableCopyTracker extends TaskBasicTracker {
    private TableCopyTaskRunnable task;

    protected InputStream stdout;
    protected InputStream stderr;

    /**
     * Passes in the task runnable so that we can cancel the task later.
     * @param task
     */
    public void enableCancellation(TableCopyTaskRunnable task) {
        this.task = task;
    }

    public boolean isCancelEnabled() {
        return task != null;
    }

    /**
     * Cancels the task runnable.
     */
    public void cancel() {
        if (task != null) {
            task.cancel();
        } else {
            throw new TableCopyClientException("Tracker without a task attempted to cancel");
        }
    }

    public void setStdout(InputStream stdout) {
        this.stdout = stdout;
    }

    public void setStderr(InputStream stderr) {
        this.stderr = stderr;
    }

    public InputStream getStdout() {
        return stdout;
    }

    public InputStream getStderr() {
        return stderr;
    }
}
