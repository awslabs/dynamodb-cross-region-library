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
