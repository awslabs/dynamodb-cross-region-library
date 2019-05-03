/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streams.connectors;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.getCurrentArguments;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.expectLastCall;
import static org.powermock.api.easymock.PowerMock.expectNew;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.resetAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;

@RunWith(PowerMockRunner.class)
@PrepareForTest(AmazonDynamoDBAsyncClient.class)
public abstract class DynamoDBReplicationEmitterTestsBase {
    protected static final AmazonDynamoDBAsyncClient DYNAMODB = createMock(AmazonDynamoDBAsyncClient.class);
    protected static final DynamoDBStreamsConnectorConfiguration config = new DynamoDBStreamsConnectorConfiguration(new Properties(), null);

    protected static final DynamoDBBuffer buffer = new DynamoDBBuffer(config);

    private static final String HASH_KEY = "hashKey";
    private static final String SEQ_NUM_PRE = "SEQ_NUM_";
    protected static final Map<String, AttributeValue> KEY1 = new HashMap<String, AttributeValue>();
    protected static final Map<String, AttributeValue> KEY2 = new HashMap<String, AttributeValue>();
    static {
        KEY1.put(HASH_KEY, new AttributeValue().withS("key1"));
        KEY2.put(HASH_KEY, new AttributeValue().withS("key2"));
    }

    protected static final Map<String, AttributeValue> OLDITEM1 = new HashMap<String, AttributeValue>(KEY1);
    protected static final Map<String, AttributeValue> NEWITEM1 = new HashMap<String, AttributeValue>(KEY1);
    protected static final Map<String, AttributeValue> OLDITEM2 = new HashMap<String, AttributeValue>(KEY2);
    protected static final Map<String, AttributeValue> NEWITEM2 = new HashMap<String, AttributeValue>(KEY2);

    static {
        String attribute = "att";
        NEWITEM1.put(attribute, new AttributeValue().withS("1"));
        OLDITEM1.put(attribute, new AttributeValue().withS("0"));
        NEWITEM2.put(attribute, new AttributeValue().withS("1"));
        OLDITEM2.put(attribute, new AttributeValue().withS("0"));
    }

    private static final StreamRecord INSERT1 = new StreamRecord().withKeys(KEY1).withNewImage(OLDITEM1).withSequenceNumber(getSequenceNumber(0))
        .withSizeBytes(1l);
    private static final StreamRecord MODIFY1 = new StreamRecord().withKeys(KEY1).withOldImage(OLDITEM1).withSequenceNumber(getSequenceNumber(1))
        .withNewImage(NEWITEM1).withSizeBytes(1l);
    private static final StreamRecord INSERT2 = new StreamRecord().withKeys(KEY2).withNewImage(OLDITEM2).withSequenceNumber(getSequenceNumber(2))
        .withSizeBytes(3l);
    private static final StreamRecord REMOVE1 = new StreamRecord().withKeys(KEY1).withOldImage(OLDITEM1).withSequenceNumber(getSequenceNumber(4))
        .withSizeBytes(1l);
    protected static final Record ITEM1_INSERT = new Record().withEventName(OperationType.INSERT).withDynamodb(INSERT1);
    protected static final Record ITEM2_INSERT = new Record().withEventName(OperationType.INSERT).withDynamodb(INSERT2);
    protected static final Record ITEM1_MODIFY = new Record().withEventName(OperationType.MODIFY).withDynamodb(MODIFY1);
    protected static final Record ITEM1_REMOVE = new Record().withEventName(OperationType.REMOVE).withDynamodb(REMOVE1);

    private static final IAnswer<Object> SUCCESS_ANSWER = new IAnswer<Object>(){
        @Override
        public Object answer() throws Throwable {
            AsyncHandler<?, ?> handler = (AsyncHandler<?, ?>) getCurrentArguments()[1];
            handler.onSuccess(null, null);
            return null;
        }
    };

    protected static String getSequenceNumber(int seqNum) {
        return SEQ_NUM_PRE + seqNum;
    }

    @SuppressWarnings("deprecation")
    @Test
    public void insertTest() throws Exception {
        // Set up the buffer and do sanity checks
        buffer.clear();
        buffer.consumeRecord(ITEM1_INSERT, ITEM1_INSERT.getDynamodb().getSizeBytes().intValue(), ITEM1_INSERT.getDynamodb().getSequenceNumber());
        assertEquals(ITEM1_INSERT.getDynamodb().getSequenceNumber(), buffer.getFirstSequenceNumber());
        assertEquals(ITEM1_INSERT.getDynamodb().getSequenceNumber(), buffer.getLastSequenceNumber());
        List<Record> buffered = buffer.getRecords();
        assertEquals(1, buffered.size());
        assertTrue(buffered.contains(ITEM1_INSERT));

        // Emit record
        resetAll(DYNAMODB);
        expectNew(AmazonDynamoDBAsyncClient.class, new Class<?>[] {AWSCredentialsProvider.class, ClientConfiguration.class, ExecutorService.class}, anyObject(AWSCredentialsProvider.class), anyObject(ClientConfiguration.class), anyObject(ExecutorService.class)).andReturn(DYNAMODB);
        DYNAMODB.putItemAsync(EasyMock.anyObject(PutItemRequest.class), anyObject(AsyncHandler.class));
        expectLastCall().andAnswer(SUCCESS_ANSWER);

        DYNAMODB.setEndpoint(EasyMock.anyString());
        EasyMock.expectLastCall().anyTimes();
        replayAll(DYNAMODB);
        IEmitter<Record> instance = createEmitterInstance();
        assertTrue(instance.emit(new UnmodifiableBuffer<Record>(buffer)).isEmpty());
        verifyAll();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void modifyTest() throws Exception {
        // Set up the buffer and do sanity checks
        buffer.clear();
        buffer.consumeRecord(ITEM1_MODIFY, ITEM1_MODIFY.getDynamodb().getSizeBytes().intValue(), ITEM1_MODIFY.getDynamodb().getSequenceNumber());
        assertEquals(ITEM1_MODIFY.getDynamodb().getSequenceNumber(), buffer.getFirstSequenceNumber());
        assertEquals(ITEM1_MODIFY.getDynamodb().getSequenceNumber(), buffer.getLastSequenceNumber());
        List<Record> buffered = buffer.getRecords();
        assertEquals(1, buffered.size());
        assertTrue(buffered.contains(ITEM1_MODIFY));

        // Emit record
        resetAll(DYNAMODB);
        DYNAMODB.putItemAsync(EasyMock.anyObject(PutItemRequest.class), anyObject(AsyncHandler.class));
        expectLastCall().andAnswer(SUCCESS_ANSWER);
        expectNew(AmazonDynamoDBAsyncClient.class, new Class<?>[] {AWSCredentialsProvider.class, ClientConfiguration.class, ExecutorService.class}, anyObject(AWSCredentialsProvider.class), anyObject(ClientConfiguration.class), anyObject(ExecutorService.class)).andReturn(DYNAMODB);
        DYNAMODB.setEndpoint(EasyMock.anyString());
        EasyMock.expectLastCall().anyTimes();
        replayAll(DYNAMODB);
        IEmitter<Record> instance = createEmitterInstance();
        assertTrue(instance.emit(new UnmodifiableBuffer<Record>(buffer)).isEmpty());
        verifyAll();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void removeTest() throws Exception {
        // Set up the buffer and do sanity checks
        buffer.clear();
        buffer.consumeRecord(ITEM1_REMOVE, ITEM1_REMOVE.getDynamodb().getSizeBytes().intValue(), ITEM1_REMOVE.getDynamodb().getSequenceNumber());
        assertEquals(ITEM1_REMOVE.getDynamodb().getSequenceNumber(), buffer.getFirstSequenceNumber());
        assertEquals(ITEM1_REMOVE.getDynamodb().getSequenceNumber(), buffer.getLastSequenceNumber());
        List<Record> buffered = buffer.getRecords();
        assertEquals(1, buffered.size());
        assertTrue(buffered.contains(ITEM1_REMOVE));

        // Emit record
        resetAll(DYNAMODB);
        DYNAMODB.deleteItemAsync(anyObject(DeleteItemRequest.class), anyObject(AsyncHandler.class));
        expectLastCall().andAnswer(SUCCESS_ANSWER);
        expectNew(AmazonDynamoDBAsyncClient.class, new Class<?>[] {AWSCredentialsProvider.class, ClientConfiguration.class, ExecutorService.class}, anyObject(AWSCredentialsProvider.class), anyObject(ClientConfiguration.class), anyObject(ExecutorService.class)).andReturn(DYNAMODB);
        DYNAMODB.setEndpoint(EasyMock.anyString());
        EasyMock.expectLastCall().anyTimes();
        replayAll(DYNAMODB);
        IEmitter<Record> instance = createEmitterInstance();
        assertTrue(instance.emit(new UnmodifiableBuffer<Record>(buffer)).isEmpty());
        verifyAll();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void multipleRecordsEmitsTest() throws Exception {
        // Set up the buffer and do sanity checks
        buffer.clear();
        buffer.consumeRecord(ITEM1_INSERT, ITEM1_INSERT.getDynamodb().getSizeBytes().intValue(), ITEM1_INSERT.getDynamodb().getSequenceNumber());
        buffer.consumeRecord(ITEM2_INSERT, ITEM2_INSERT.getDynamodb().getSizeBytes().intValue(), ITEM2_INSERT.getDynamodb().getSequenceNumber());
        assertEquals(ITEM1_INSERT.getDynamodb().getSequenceNumber(), buffer.getFirstSequenceNumber());
        assertEquals(ITEM2_INSERT.getDynamodb().getSequenceNumber(), buffer.getLastSequenceNumber());
        List<Record> buffered = buffer.getRecords();
        assertEquals(2, buffered.size());
        assertTrue(buffered.contains(ITEM1_INSERT));
        assertTrue(buffered.contains(ITEM2_INSERT));

        // Emit record
        resetAll(DYNAMODB);
        DYNAMODB.putItemAsync(EasyMock.anyObject(PutItemRequest.class), anyObject(AsyncHandler.class));
        expectLastCall().andAnswer(SUCCESS_ANSWER).times(2);
        expectNew(AmazonDynamoDBAsyncClient.class, new Class<?>[] {AWSCredentialsProvider.class, ClientConfiguration.class, ExecutorService.class}, anyObject(AWSCredentialsProvider.class), anyObject(ClientConfiguration.class), anyObject(ExecutorService.class)).andReturn(DYNAMODB);
        DYNAMODB.setEndpoint(EasyMock.anyString());
        EasyMock.expectLastCall().anyTimes();
        replayAll(DYNAMODB);
        IEmitter<Record> instance = createEmitterInstance();
        assertTrue(instance.emit(new UnmodifiableBuffer<Record>(buffer)).isEmpty());
        verifyAll();
    }

    protected abstract IEmitter<Record> createEmitterInstance();
}
