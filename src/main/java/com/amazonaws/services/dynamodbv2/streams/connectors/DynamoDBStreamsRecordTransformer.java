/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streams.connectors;

import java.io.IOException;
import java.nio.charset.Charset;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class implements {@link ITransformer} to convert input Kinesis records to output DynamoDB Streams records. It then converts all records into the format
 * expected by the emitter, which is also Streams records in this case.
 */
public class DynamoDBStreamsRecordTransformer implements ITransformer<Record, Record> {

    private static final ObjectMapper MAPPER = DynamoDBStreamsRecordObjectMapper.getInstance();
    private static final Charset ENCODING = Charset.forName("UTF-8");
    /**
     * {@inheritDoc}
     */
    @Override
    public Record fromClass(final Record record) throws IOException {
        // since the emitter expects DynamoDB stream records, do nothing here
        return record;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Record toClass(final com.amazonaws.services.kinesis.model.Record record) throws IOException {
        if (record instanceof RecordAdapter) {
            return ((RecordAdapter) record).getInternalObject();
        } else {
            return MAPPER.readValue(new String(record.getData().array(), ENCODING), Record.class);
        }
    }
}
