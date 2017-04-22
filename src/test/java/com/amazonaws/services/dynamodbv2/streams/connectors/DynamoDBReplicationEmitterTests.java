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
package com.amazonaws.services.dynamodbv2.streams.connectors;

import java.util.concurrent.Future;

import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Future.class, DynamoDBReplicationEmitter.class})
@PowerMockIgnore({"javax.management.*", "org.apache.log4j.*"})
public class DynamoDBReplicationEmitterTests extends DynamoDBReplicationEmitterTestsBase {
    @Override
    protected IEmitter<Record> createEmitterInstance() {
        return new DynamoDBReplicationEmitter("TEST", "ENDPOINT", "REGION", "TABLE", null, new AWSStaticCredentialsProvider(new BasicAWSCredentials("Access", "Secret")));
    }
}
