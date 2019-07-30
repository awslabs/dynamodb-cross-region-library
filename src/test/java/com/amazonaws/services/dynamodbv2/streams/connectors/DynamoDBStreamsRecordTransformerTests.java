/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streams.connectors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Simple tests to ensure DynamoDBStreamsRecordTransformer is deserializing DynamoDB Streams Record wrapped in Kinesis Record properly
 */
public class DynamoDBStreamsRecordTransformerTests {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ITransformer<com.amazonaws.services.dynamodbv2.model.Record, com.amazonaws.services.dynamodbv2.model.Record> TRANSFORMER = new DynamoDBStreamsRecordTransformer();
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

    @Test(expected=IOException.class)
    public void testInvalidRecord() throws IOException {
        TRANSFORMER.toClass(INVALID_KINESIS_RECORD);
    }

    @Test
    public void testRecordAdapter() throws IOException {
        TRANSFORMER.toClass(VALID_RECORD_ADAPTER);
    }

    @Test
    public void testValidRecord() throws IOException {
        Record validKinesisRecord = new Record().withData(ByteBuffer.wrap(MAPPER.writeValueAsBytes(VALID_DDB_RECORD)));
        TRANSFORMER.toClass(validKinesisRecord);
    }
}
