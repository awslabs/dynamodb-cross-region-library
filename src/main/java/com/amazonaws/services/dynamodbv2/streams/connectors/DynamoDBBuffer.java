/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streams.connectors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.annotation.NotThreadSafe;
import org.apache.log4j.Logger;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;

/**
 * A buffer that stores DynamoDB Streams records. Deduplicates based on the latest record with a given DynamoDB key.
 * First and last sequence numbers are based on the entire range of records considered, even if a record has been
 * overwritten by a newer record with the same key. The buffer is designed to flush on every processRecords call.
 */
@NotThreadSafe
public class DynamoDBBuffer implements IBuffer<Record> {
    /**
     * Logger for the DynamoDBBuffer.
     */
    private static final Logger LOGGER = Logger.getLogger(DynamoDBBuffer.class);

    /**
     * The map of DynamoDB key to DynamoDB Stream record used to buffer writes.
     */
    private final Map<Map<String, AttributeValue>, Record> buffer = new HashMap<Map<String, AttributeValue>, Record>();

    /**
     * The CloudWatch client to use for emitting metrics.
     */
    private final AmazonCloudWatch cloudwatch;
    /**
     * Field to store the first sequence number stored in the buffer.
     */
    private String firstSeqNum = null;
    /**
     * Field to store the last sequence number stored in the buffer.
     */
    private String lastSeqNum = null;

    /**
     * Processed records count for emitting CloudWatch metrics.
     */
    private double processedRecords = 0;

    /**
     * Constructor for buffer.
     *
     * @param configuration
     *            The dynamodb kinesis connector configuration containing parameters for the buffer
     */
    public DynamoDBBuffer(final DynamoDBStreamsConnectorConfiguration configuration) {
        // TODO set up cloudwatch to emit metrics
        cloudwatch = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        // Clear the set and reset sequence number bounds
        getBuffer().clear();
        setFirstSequenceNumber(null);
        setLastSequenceNumber(null);
        setProcessedRecords(0);
        emitCloudWatchMetrics();
        LOGGER.debug("Buffer cleared with buffer size: " + buffer.size() + " (" + processedRecords + " processed)");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void consumeRecord(final Record record, final int recordBytes, final String sequenceNumber) {
        // Use HashMap to deduplicate using the DynamoDB key as the key.
        getBuffer().put(record.getDynamodb().getKeys(), record);
        // Sequence number bound maintenance
        setLastSequenceNumber(sequenceNumber);
        if (getFirstSequenceNumber() == null) {
            setFirstSequenceNumber(getLastSequenceNumber());
        }
        setProcessedRecords(getProcessedRecords() + 1);
        emitCloudWatchMetrics();
    }

    /**
     * Publish relevant CloudWatch metrics.
     */
    protected void emitCloudWatchMetrics() {
        if (null != getCloudwatch()) {
            // TODO Emit CloudWatch metrics about the size of the queue of writes
            MetricDatum recordsProcessedDatum = new MetricDatum().withValue(getProcessedRecords());
            PutMetricDataRequest request = new PutMetricDataRequest().withMetricData(recordsProcessedDatum);
            getCloudwatch().putMetricData(request);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getBytesToBuffer() {
        return 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getFirstSequenceNumber() {
        return firstSeqNum;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getLastSequenceNumber() {
        return lastSeqNum;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMillisecondsToBuffer() {
        return Long.MAX_VALUE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getNumRecordsToBuffer() {
        return 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Record> getRecords() {
        // Convert records to a list
        return new ArrayList<Record>(getBuffer().values());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean shouldFlush() {
        return getBuffer().size() > 0;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "DynamoDBBuffer [buffer=" + getBuffer() + ", cloudwatch=" + getCloudwatch() + ", firstSeqNum="
            + getFirstSequenceNumber() + ", lastSeqNum=" + getLastSequenceNumber() + ", processedRecords="
            + getProcessedRecords() + "]";
    }

    /**
     * @return the buffer
     */
    public Map<Map<String, AttributeValue>, Record> getBuffer() {
        return buffer;
    }

    /**
     * @param lastSeqNumParameter
     *            the lastSeqNum to set
     */
    public void setLastSequenceNumber(final String lastSeqNumParameter) {
        this.lastSeqNum = lastSeqNumParameter;
    }

    /**
     * @param firstSeqNumParameter
     *            the firstSeqNum to set
     */
    public void setFirstSequenceNumber(final String firstSeqNumParameter) {
        this.firstSeqNum = firstSeqNumParameter;
    }

    /**
     * @return the processedRecords
     */
    public double getProcessedRecords() {
        return processedRecords;
    }

    /**
     * @param processedRecords
     *            the processedRecords to set
     */
    public void setProcessedRecords(final double processedRecords) {
        this.processedRecords = processedRecords;
    }

    /**
     * @return the cloudwatch
     */
    public AmazonCloudWatch getCloudwatch() {
        return cloudwatch;
    }

}
