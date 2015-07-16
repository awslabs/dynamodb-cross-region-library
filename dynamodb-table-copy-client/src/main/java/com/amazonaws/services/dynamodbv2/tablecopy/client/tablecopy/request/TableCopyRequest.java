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
package com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.request;

import com.amazonaws.services.dynamodbv2.tablecopy.client.exceptions.TableCopyClientException;

public final class TableCopyRequest {
    // Source table name
    public final String srcTableName;

    // Source endpoint
    public final String srcEndpoint;

    // Ratio of source table read throughput to use for the copy
    public final double srcReadThroughputRatio;

    // Destination table name
    public final String dstTableName;

    // Destination region
    public final String dstEndpoint;

    // Ratio of destination write throughput to use for the copy
    public final double dstWriteThroughputRatio;

    public TableCopyRequest(String srcTableName, String srcEndpoint, double srcReadThroughputRatio,
                            String dstTableName, String dstEndpoint, double dstWriteThroughputRatio) {
        if (srcReadThroughputRatio > 1 || srcReadThroughputRatio <= 0 ||
                dstWriteThroughputRatio > 1 || dstWriteThroughputRatio <= 0) {
            throw new TableCopyClientException("Throughput ratios must be in the range (0, 1]");
        }
        this.srcTableName = srcTableName;
        this.srcEndpoint = srcEndpoint;
        this.srcReadThroughputRatio = srcReadThroughputRatio;
        this.dstTableName = dstTableName;
        this.dstEndpoint = dstEndpoint;
        this.dstWriteThroughputRatio = dstWriteThroughputRatio;
    }

}
