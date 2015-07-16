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

import com.amazonaws.services.dynamodbv2.tablecopy.client.metadataaccess.TableCopyMetadataAccess;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.request.TableCopyCallback;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.request.TableCopyRequest;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTaskHandler;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTracker;

/**
 * Context for a table copy step
 */
public class TableCopyStepContext {
    private final TableCopyRequest request;
    private final TableCopyCallback callback;
    private final TableCopyTracker asyncTracker;
    private final TableCopyMetadataAccess metadataAccess;
    private final TableCopyTaskHandler tableCopyHandler;

    public TableCopyStepContext(TableCopyRequest request,
                                TableCopyCallback callback,
                                TableCopyTracker asyncTracker,
                                TableCopyMetadataAccess metadataAccess,
                                TableCopyTaskHandler tableCopyHandler) {
        this.request = request;
        this.callback = callback;
        this.asyncTracker = asyncTracker;
        this.metadataAccess = metadataAccess;
        this.tableCopyHandler = tableCopyHandler;
    }

    public TableCopyRequest getRequest() {
        return request;
    }

    public TableCopyCallback getCallback() {
        return callback;
    }

    public TableCopyTracker getAsyncTracker() {
        return asyncTracker;
    }

    public TableCopyMetadataAccess getMetadataAccess() {
        return metadataAccess;
    }

    public TableCopyTaskHandler getTableCopyHandler() {
        return tableCopyHandler;
    }
}
