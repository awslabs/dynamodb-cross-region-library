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

import static org.powermock.api.easymock.PowerMock.expectLastCall;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.replication.coordinator.state.DynamoDBReplicationGroupMemberDirectTransition;
import com.amazonaws.services.dynamodbv2.replication.coordinator.state.DynamoDBReplicationGroupTransition;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A simple test to ensure DynamoDB Streams record wrapped in Kinesis record format can be deserialized properly
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DynamoDBReplicationGroupTransition.class)
@PowerMockIgnore({"javax.*"})
public class DynamoDBReplicationRecordProcessorTests {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DynamoDBMetadataStorage MD = PowerMock.createMock(DynamoDBMetadataStorage.class);
    private static final AccountMapToAwsAccess ACCESS_MAP = PowerMock.createMock(AccountMapToAwsAccess.class);
    private static final DynamoDBReplicationRecordProcessor PROCESSOR = new DynamoDBReplicationRecordProcessor(MD, ACCESS_MAP);
    private static final Record INVALID_KINESIS_RECORD = new Record().withData(ByteBuffer.wrap(new String("DummyData").getBytes()));
    private static final StreamRecord STREAM_RECORD = new StreamRecord();

    static {
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("hashKey", new AttributeValue("hashKeyValue"));
        Map<String, AttributeValue> oldImage = new HashMap<String, AttributeValue>(key);
        Map<String, AttributeValue> newImage = new HashMap<String, AttributeValue>(key);
        newImage.put("newAttributeKey", new AttributeValue("newAttributeValue"));
        STREAM_RECORD.setKeys(key);
        STREAM_RECORD.setOldImage(oldImage);
        STREAM_RECORD.setNewImage(newImage);
        STREAM_RECORD.setSizeBytes(Long.MAX_VALUE);
        STREAM_RECORD.setSequenceNumber(UUID.randomUUID().toString());
        STREAM_RECORD.setStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES);
    }

    private static final com.amazonaws.services.dynamodbv2.model.Record VALID_DDB_RECORD = new com.amazonaws.services.dynamodbv2.model.Record()
        .withAwsRegion("us-east-1").withEventID(UUID.randomUUID().toString()).withEventName(OperationType.MODIFY).withEventSource("aws:dynamodb")
        .withEventVersion("1.0").withDynamodb(STREAM_RECORD);
    private static final Record VALID_RECORD_ADAPTER = new RecordAdapter(VALID_DDB_RECORD);

    @Test
    public void testInvalidRecord() throws IOException, KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException,
        IllegalArgumentException {
        // mock the checkpoint method
        IRecordProcessorCheckpointer checkpointer = PowerMock.createMock(IRecordProcessorCheckpointer.class);
        checkpointer.checkpoint(EasyMock.anyString());
        expectLastCall();
        replayAll();

        // try to process an invalid kinesis record
        List<Record> records = new ArrayList<Record>();
        records.add(INVALID_KINESIS_RECORD);
        PROCESSOR.processRecords(records, checkpointer);

        // verify checkpoint was called
        verifyAll();
    }

    @Test
    public void testRecordAdapter() throws IOException, KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException,
        IllegalArgumentException {
        // mock the transition method
        mockStatic(DynamoDBReplicationGroupTransition.class);
        DynamoDBReplicationGroupTransition transition = PowerMock.createMock(DynamoDBReplicationGroupMemberDirectTransition.class);
        DynamoDBReplicationGroupTransition.getTransition(STREAM_RECORD);
        expectLastCall().andReturn(transition);
        transition.transition(MD, ACCESS_MAP);
        expectLastCall();

        // mock the checkpoint method
        IRecordProcessorCheckpointer checkpointer = PowerMock.createMock(IRecordProcessorCheckpointer.class);
        checkpointer.checkpoint(EasyMock.anyString());
        expectLastCall();
        replayAll();

        // try to process a valid DynamoDB Streams record wrapped in a kinesis record by RecordAdapter
        List<Record> records = new ArrayList<Record>();
        records.add(VALID_RECORD_ADAPTER);
        PROCESSOR.processRecords(records, checkpointer);

        // verify transition and checkpoint were called
        verifyAll();
    }

    @Test
    public void testValidRecord() throws IOException, KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException,
        IllegalArgumentException {
        // mock the transition method
        mockStatic(DynamoDBReplicationGroupTransition.class);
        DynamoDBReplicationGroupTransition transition = PowerMock.createMock(DynamoDBReplicationGroupMemberDirectTransition.class);
        DynamoDBReplicationGroupTransition.getTransition(STREAM_RECORD);
        expectLastCall().andReturn(transition);
        transition.transition(MD, ACCESS_MAP);
        expectLastCall();

        // mock the checkpoint method
        IRecordProcessorCheckpointer checkpointer = PowerMock.createMock(IRecordProcessorCheckpointer.class);
        checkpointer.checkpoint(EasyMock.anyString());
        expectLastCall();
        replayAll();

        // try to process a valid DynamoDB Streams record wrapped in a kinesis record
        Record validKinesisRecord = new Record().withData(ByteBuffer.wrap(MAPPER.writeValueAsBytes(VALID_DDB_RECORD)));
        List<Record> records = new ArrayList<Record>();
        records.add(validKinesisRecord);
        PROCESSOR.processRecords(records, checkpointer);

        // verify transition and checkpoint were called
        verifyAll();
    }
}
