/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streams.connectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;

public class DynamoDBBufferTests {
    private static final String HASH_KEY = "hashKey";
    private static final String SEQ_NUM_PRE = "SEQ_NUM_";
    protected static final Map<String, AttributeValue> KEY1 = new HashMap<String, AttributeValue>();
    protected static final Map<String, AttributeValue> KEY2 = new HashMap<String, AttributeValue>();
    static {
        KEY1.put(HASH_KEY, new AttributeValue().withS("key1"));
        KEY2.put(HASH_KEY, new AttributeValue().withS("key2"));
    }
    protected static final Map<String, AttributeValue> NEWITEM1 = new HashMap<String, AttributeValue>(KEY1);
    protected static final Map<String, AttributeValue> OLDITEM1 = new HashMap<String, AttributeValue>(KEY1);
    protected static final Map<String, AttributeValue> OLDITEM2 = new HashMap<String, AttributeValue>(KEY2);
    protected static final Map<String, AttributeValue> NEWITEM2 = new HashMap<String, AttributeValue>(KEY2);

    static {
        String attribute = "att";
        NEWITEM1.put(attribute, new AttributeValue().withS("1"));
        OLDITEM1.put(attribute, new AttributeValue().withS("0"));
        NEWITEM2.put(attribute, new AttributeValue().withS("1"));
        OLDITEM2.put(attribute, new AttributeValue().withS("0"));
    }

    private static final StreamRecord INSERT1 = new StreamRecord().withKeys(KEY1).withNewImage(OLDITEM1)
        .withSequenceNumber(getSequenceNumber(0)).withSizeBytes(1l);
    private static final StreamRecord MODIFY1 = new StreamRecord().withKeys(KEY1).withOldImage(OLDITEM1)
        .withSequenceNumber(getSequenceNumber(1)).withNewImage(NEWITEM1).withSizeBytes(1l);
    private static final StreamRecord INSERT2 = new StreamRecord().withKeys(KEY2).withNewImage(OLDITEM2)
        .withSequenceNumber(getSequenceNumber(2)).withSizeBytes(3l);
    private static final StreamRecord MODIFY2 = new StreamRecord().withKeys(KEY2).withOldImage(OLDITEM2)
        .withNewImage(NEWITEM2).withSequenceNumber(getSequenceNumber(3)).withSizeBytes(3l);
    protected static final Record ITEM1_INSERT = new Record().withEventName(OperationType.INSERT).withDynamodb(INSERT1);
    protected static final Record ITEM2_INSERT = new Record().withEventName(OperationType.INSERT).withDynamodb(INSERT2);
    protected static final Record ITEM1_MODIFY = new Record().withEventName(OperationType.MODIFY).withDynamodb(MODIFY1);
    protected static final Record ITEM2_MODIFY = new Record().withEventName(OperationType.MODIFY).withDynamodb(MODIFY2);

    protected static String getSequenceNumber(int seqNum) {
        return SEQ_NUM_PRE + seqNum;
    }

    @Test
    public void sanityTest() {
        DynamoDBBuffer buffer = new DynamoDBBuffer(new DynamoDBStreamsConnectorConfiguration(new Properties(), null));

        buffer.consumeRecord(ITEM1_INSERT, ITEM1_INSERT.getDynamodb().getSizeBytes().intValue(), ITEM1_INSERT
            .getDynamodb().getSequenceNumber());
        assertEquals(ITEM1_INSERT.getDynamodb().getSequenceNumber(),buffer.getFirstSequenceNumber());
        assertEquals(ITEM1_INSERT.getDynamodb().getSequenceNumber(),buffer.getLastSequenceNumber());
        List<Record> buffered = buffer.getRecords();
        assertEquals(1, buffered.size());
        assertTrue(buffered.contains(ITEM1_INSERT));
        assertTrue(buffer.shouldFlush());
    }

    @Test
    public void multipleRecordsTest() {
        DynamoDBBuffer buffer = new DynamoDBBuffer(new DynamoDBStreamsConnectorConfiguration(new Properties(), null));

        buffer.consumeRecord(ITEM1_INSERT, ITEM1_INSERT.getDynamodb().getSizeBytes().intValue(), ITEM1_INSERT
            .getDynamodb().getSequenceNumber());
        buffer.consumeRecord(ITEM2_INSERT, ITEM2_INSERT.getDynamodb().getSizeBytes().intValue(), ITEM2_INSERT
            .getDynamodb().getSequenceNumber());
        assertEquals(ITEM1_INSERT.getDynamodb().getSequenceNumber(),buffer.getFirstSequenceNumber());
        assertEquals(ITEM2_INSERT.getDynamodb().getSequenceNumber(),buffer.getLastSequenceNumber());
        List<Record> buffered = buffer.getRecords();
        assertEquals(2, buffered.size());
        assertTrue(buffered.contains(ITEM1_INSERT));
        assertTrue(buffered.contains(ITEM2_INSERT));
        assertTrue(buffer.shouldFlush());
    }

    @Test
    public void dedupSanityTest() {
        DynamoDBBuffer buffer = new DynamoDBBuffer(new DynamoDBStreamsConnectorConfiguration(new Properties(), null));

        buffer.consumeRecord(ITEM1_INSERT, ITEM1_INSERT.getDynamodb().getSizeBytes().intValue(), ITEM1_INSERT
            .getDynamodb().getSequenceNumber());
        buffer.consumeRecord(ITEM1_MODIFY, ITEM1_MODIFY.getDynamodb().getSizeBytes().intValue(), ITEM1_MODIFY
            .getDynamodb().getSequenceNumber());
        assertEquals(ITEM1_INSERT.getDynamodb().getSequenceNumber(),buffer.getFirstSequenceNumber());
        assertEquals(ITEM1_MODIFY.getDynamodb().getSequenceNumber(),buffer.getLastSequenceNumber());
        List<Record> buffered = buffer.getRecords();
        assertEquals(1, buffered.size());
        assertTrue(buffered.contains(ITEM1_MODIFY));
        assertFalse(buffered.contains(ITEM1_INSERT));
        assertTrue(buffer.shouldFlush());
    }

    @Test
    public void dedupMultipleRecordsTest() {
        DynamoDBBuffer buffer = new DynamoDBBuffer(new DynamoDBStreamsConnectorConfiguration(new Properties(), null));

        buffer.consumeRecord(ITEM1_INSERT, ITEM1_INSERT.getDynamodb().getSizeBytes().intValue(), ITEM1_INSERT
            .getDynamodb().getSequenceNumber());
        buffer.consumeRecord(ITEM1_MODIFY, ITEM1_MODIFY.getDynamodb().getSizeBytes().intValue(), ITEM1_MODIFY
            .getDynamodb().getSequenceNumber());
        buffer.consumeRecord(ITEM2_INSERT, ITEM2_INSERT.getDynamodb().getSizeBytes().intValue(), ITEM2_INSERT
            .getDynamodb().getSequenceNumber());
        buffer.consumeRecord(ITEM2_MODIFY, ITEM2_MODIFY.getDynamodb().getSizeBytes().intValue(), ITEM2_MODIFY
            .getDynamodb().getSequenceNumber());
        assertEquals(ITEM1_INSERT.getDynamodb().getSequenceNumber(),buffer.getFirstSequenceNumber());
        assertEquals(ITEM2_MODIFY.getDynamodb().getSequenceNumber(),buffer.getLastSequenceNumber());
        List<Record> buffered = buffer.getRecords();
        assertEquals(2, buffered.size());
        assertTrue(buffered.contains(ITEM1_MODIFY));
        assertFalse(buffered.contains(ITEM1_INSERT));
        assertTrue(buffered.contains(ITEM2_MODIFY));
        assertFalse(buffered.contains(ITEM2_INSERT));
        assertTrue(buffer.shouldFlush());
    }

    @Test
    public void testBufferBounds(){
        DynamoDBBuffer buffer = new DynamoDBBuffer(new DynamoDBStreamsConnectorConfiguration(new Properties(), null));
        assertEquals(1,buffer.getBytesToBuffer());
        assertEquals(1,buffer.getNumRecordsToBuffer());
        assertEquals(Long.MAX_VALUE,buffer.getMillisecondsToBuffer());
    }
}
