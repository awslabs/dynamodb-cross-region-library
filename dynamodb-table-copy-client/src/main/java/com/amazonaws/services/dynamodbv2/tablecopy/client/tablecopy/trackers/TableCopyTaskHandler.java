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

import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.request.TableCopyRequest;

import java.io.InputStream;

/**
 * Handler for the internal table copy task.
 *
 * NOTE: The current implementation only allows for one task handler to exist per table copy task. There could be
 * race conditions if multiple task handlers work on the same task.
 */
public abstract class TableCopyTaskHandler extends TaskTracker {
    protected final TableCopyRequest request;
    private boolean isShutdown = false;

    public TableCopyTaskHandler(TableCopyRequest request) {
        this.request = request;
    }

    /**
     * Asynchronously perform the table copy specified in the request.
     */
    public synchronized void tableCopy() {
        if (!isShutdown) {
            tableCopyImpl();
        }
    }

    /**
     * Shutdown the table copy task and clean up any resources.
     */
    public synchronized void shutdown() {
        if (!isShutdown) {
            shutdownImpl();
            isShutdown = true;
        }
    }

    public InputStream getInputStream() {
        return null;
    }

    public InputStream getErrorStream() {
        return null;
    }

    protected abstract void tableCopyImpl();
    protected abstract void shutdownImpl();
}
