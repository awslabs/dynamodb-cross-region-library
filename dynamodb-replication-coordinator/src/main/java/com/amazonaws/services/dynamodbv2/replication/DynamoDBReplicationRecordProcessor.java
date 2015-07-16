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

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.Constants;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.replication.coordinator.state.DynamoDBReplicationGroupTransition;
import com.amazonaws.services.dynamodbv2.streams.connectors.DynamoDBStreamsRecordObjectMapper;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Implementation of the {@link IRecordProcessor} interface for each instance of KCL worker, which is used to process records from DynamoDB Streams
 *
 */
public class DynamoDBReplicationRecordProcessor implements IRecordProcessor {

    /*
     * Logger for the class
     */
    private static final Logger LOGGER = Logger.getLogger(DynamoDBReplicationRecordProcessor.class);

    private final MetadataStorage md;
    private final AccountMapToAwsAccess awsAccess;
    private String shardId;

    /*
     * Static instance of an object mapper for parsing Kinesis records from DynamoDB streams
     */
    private static final ObjectMapper MAPPER = DynamoDBStreamsRecordObjectMapper.getInstance();

    public DynamoDBReplicationRecordProcessor(MetadataStorage md, AccountMapToAwsAccess awsAccess) {
        if (md == null || awsAccess == null) {
            throw new IllegalArgumentException("Metadata storage and Account Map of Aws Access cannot be null.");
        }
        this.awsAccess = awsAccess;
        this.md = md;
    }

    /**
     * Checkpoints based on the given sequence number
     *
     * @param checkpointer
     *            checkpointer for the processor
     */
    private static void checkpoint(IRecordProcessorCheckpointer checkpointer, String sequenceNumber) {
        try {
            checkpointer.checkpoint(sequenceNumber);
        } catch (KinesisClientLibDependencyException e) {
            LOGGER.error("Problem with dependency of Kinesis", e);
        } catch (InvalidStateException e) {
            LOGGER.error("Problem with the state of Kinesis (e.g. cannot access metadata DynamoDB table", e);
        } catch (ThrottlingException e) {
            LOGGER.error("Throttling exception with DynamoDB checkpoint table", e);
        } catch (ShutdownException e) {
            LOGGER.error("Shutdown received when checkpointing", e);
        }
    }

    /**
     * Deserializes record from Streams wrapped in Kinesis format back to Streams record
     *
     * @param record
     *            record in Kinesis format
     * @return record in Streams record format
     */
    private static StreamRecord getStreamRecord(Record record) {
        if (record instanceof RecordAdapter) {
            RecordAdapter recordAdapter = (RecordAdapter) record;
            if (null != recordAdapter.getInternalObject()) {
                return recordAdapter.getInternalObject().getDynamodb();
            } else {
                LOGGER.error("Could not deserialize record, internal record object is null");
                return null;
            }
        } else {
            try {
                return MAPPER.readValue(new String(record.getData().array(), Constants.ENCODING), com.amazonaws.services.dynamodbv2.model.Record.class).getDynamodb();
            } catch (IOException e) {
                LOGGER.error("Could not deserialize to DynamoDB record!", e);
                return null;
            }
        }
    }

    @Override
    public void initialize(String shardId) {
        this.shardId = shardId;
        LOGGER.info("initializing record processor with shardId: " + shardId);
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        for (Record record : records) {
            // deserialize to Streams record format
            StreamRecord streamRecord = getStreamRecord(record);

            // if deserialized successfully, detect the event change in the record and process event according to the
            // state machine
            if (null != streamRecord) {
                DynamoDBReplicationGroupTransition transition = DynamoDBReplicationGroupTransition.getTransition(streamRecord);
                transition.transition(md, awsAccess);
            }

            // record finished processing, checkpoint record
            checkpoint(checkpointer, record.getSequenceNumber());
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        try {
            switch (reason) {
                case TERMINATE:
                    checkpointer.checkpoint();
                    break;
                case ZOMBIE:
                    break;
                default:
                    throw new IllegalStateException("invalid shutdown reason");
            }
            LOGGER.info("shutting down record processor with shardId: " + shardId + " with reason " + reason);
        } catch (KinesisClientLibDependencyException | InvalidStateException | ThrottlingException | ShutdownException e) {
            LOGGER.error("could not checkpoint at the end of the shard: " + shardId + " with message: " + e);
        }
    }

}
