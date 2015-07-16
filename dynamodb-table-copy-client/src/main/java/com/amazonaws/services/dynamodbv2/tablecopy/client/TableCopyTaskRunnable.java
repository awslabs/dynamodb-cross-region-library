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
package com.amazonaws.services.dynamodbv2.tablecopy.client;

import com.amazonaws.services.dynamodbv2.tablecopy.client.metadataaccess.TableCopyMetadataAccess;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.request.TableCopyCallback;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.request.TableCopyRequest;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTaskHandler;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTracker;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TaskStatus;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.workflow.TableCopyStep;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.workflow.TableCopyStepContext;

/**
 * Runnable for the table copy task. Drives the table copy workflow, cancellations, and updates
 * task status.
 */
public class TableCopyTaskRunnable implements Runnable {

    private final TableCopyRequest request;
    private final TableCopyMetadataAccess metadataAccess;
    private final TableCopyCallback callback;
    private final TableCopyTracker tracker;
    private final TableCopyTaskHandler handler;

    private TableCopyStepContext context;
    private Boolean isCanceled = false;

    public TableCopyTaskRunnable(TableCopyRequest request, TableCopyMetadataAccess metadataAccess,
                                 TableCopyCallback callback, TableCopyTracker tracker, TableCopyTaskHandler handler) {
        this.request = request;
        this.metadataAccess = metadataAccess;
        this.callback = callback;
        this.tracker = tracker;
        this.handler = handler;
    }

    @Override
    public void run() {
        TableCopyStep currentStep = metadataAccess.getStep(request);
        context = new TableCopyStepContext(request, callback, tracker, metadataAccess, handler);

        while (!isCanceled && !currentStep.isTerminal()) {
            currentStep = currentStep.nextStep(context);
        }
        synchronized (isCanceled) {
            if (!isCanceled) {
                tracker.setStatus(currentStep.getTaskStatus());
            }
        }
    }

    public void cancel() {
        synchronized (isCanceled) {
            if (!isCanceled) {
                isCanceled = true;
                handler.shutdown();
                tracker.setStatus(TaskStatus.CANCELED);
            }
        }
    }

    public boolean isCanceled() {
        return isCanceled;
    }
}
