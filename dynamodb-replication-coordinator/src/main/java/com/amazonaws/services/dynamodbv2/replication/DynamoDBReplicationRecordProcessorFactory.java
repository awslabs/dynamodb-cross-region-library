/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.replication;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class DynamoDBReplicationRecordProcessorFactory implements IRecordProcessorFactory {

    private final MetadataStorage md;
    private final AccountMapToAwsAccess awsAccess;

    @Override
    public IRecordProcessor createProcessor() {
        return new DynamoDBReplicationRecordProcessor(md, awsAccess);
    }

    public DynamoDBReplicationRecordProcessorFactory(MetadataStorage md, AccountMapToAwsAccess awsAccess) {
        this.md = md;
        this.awsAccess = awsAccess;
    }

}
