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
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTaskHandlerFactory;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTracker;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TaskStatus;

import java.util.concurrent.ExecutorService;

/**
 * Client for copying a DynamoDB table into a new DynamoDB table. The source and destination table
 * must both already exist in order for the copy to proceed.
 *
 * NOTE: The current client implementation can run into race conditions if multiple threads/processes
 * try launching and canceling a task at the same time.
 */
public class DynamoDBTableCopyClient {
    private final TableCopyMetadataAccess metadataAccess;
    private final TableCopyTaskHandlerFactory taskHandlerFactory;
    private final ExecutorService threadpool;

    /**
     * Constructor for the table copy client
     * @param metadataAccess - persistent metadata storage interface for table copy task
     * @param taskHandlerFactory - factory to create new TaskHandler instances
     */
    public DynamoDBTableCopyClient(TableCopyMetadataAccess metadataAccess,
                                      TableCopyTaskHandlerFactory taskHandlerFactory,
                                      ExecutorService threadpool) {
        this.metadataAccess = metadataAccess;
        this.taskHandlerFactory = taskHandlerFactory;
        this.threadpool = threadpool;
    }

    /**
     * Asynchronously perform the table copy specified in the request. If a table copy task already exists
     * for the request, then returns a tracker for the existing table copy task.
     *
     * @param request - the table copy request
     * @param callback - asynchronous handler that will be called when the table copy completes. It may be
     *                called multiple times.
     * @return - a tracker to monitor the status of the request
     */
    public TableCopyTracker launchTableCopy(final TableCopyRequest request,
                                            final TableCopyCallback callback) {

        final TableCopyTracker asyncTracker = new TableCopyTracker();
        asyncTracker.setStatus(TaskStatus.ACTIVE);

        final TableCopyTaskHandler handler = taskHandlerFactory.createTaskHandler(request);

        TableCopyTaskRunnable task = new TableCopyTaskRunnable(request, metadataAccess, callback, asyncTracker, handler);

        threadpool.submit(task);
        asyncTracker.enableCancellation(task);

        return asyncTracker;
    }

    /*
     * Get TableCopyTaskHandlerFactory
     */
    public TableCopyTaskHandlerFactory getTaskHandlerFactory() {
        return taskHandlerFactory;
    }

}
