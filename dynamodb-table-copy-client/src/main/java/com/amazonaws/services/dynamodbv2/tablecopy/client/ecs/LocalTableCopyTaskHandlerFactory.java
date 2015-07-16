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
package com.amazonaws.services.dynamodbv2.tablecopy.client.ecs;

import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.request.TableCopyRequest;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTaskHandler;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTaskHandlerFactory;

/**
 * Local process version of the TaskHandler Factory, which spawns the table copy task in a local process
 */
public abstract class LocalTableCopyTaskHandlerFactory implements TableCopyTaskHandlerFactory {

    @Override
    public TableCopyTaskHandler createTaskHandler(TableCopyRequest request) {
        try {
            return new LocalTableCopyTaskHandler(request, generateLocalCommand(request).split(" "));
        } catch (ClassCastException cce) {
            throw new IllegalArgumentException("LocalTableCopyTaskHandlerFactory requires a LocalTableCopyRequest");
        }
    }

    /**
     * Generate the command to kick off the local table copy task.
     * @param request
     * @return
     */
    public abstract String generateLocalCommand(TableCopyRequest request);
}
