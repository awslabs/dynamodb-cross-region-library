/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
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
