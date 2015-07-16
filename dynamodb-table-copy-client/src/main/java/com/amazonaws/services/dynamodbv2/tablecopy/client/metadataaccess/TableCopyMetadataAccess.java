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
package com.amazonaws.services.dynamodbv2.tablecopy.client.metadataaccess;

import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.request.TableCopyRequest;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.workflow.TableCopyStep;

/**
 * Persistent metadata storage interface for table copy task
 */
public interface TableCopyMetadataAccess {
    public TableCopyStep getStep(TableCopyRequest request);

    public void writeStep(TableCopyStep nextStep);
}
