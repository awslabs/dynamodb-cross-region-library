/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streams.connectors;

import java.io.UnsupportedEncodingException;

import org.apache.commons.codec.binary.Hex;

import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.google.common.base.Strings;

import lombok.extern.log4j.Log4j;

@Log4j
public class DynamoDBConnectorUtilities {

    /**
     * Get the taskname from command line arguments if it exists, if not, autogenerate one to be used by KCL in the
     * checkpoint table and to publish CloudWatch metrics
     *
     * @param sourceRegion
     *            region of the source table
     * @param destinationRegion
     *            region of the destination table
     * @param suppliedTaskName
     *            the user supplied task name
     * @param sourceTableName
     *            the source table name
     * @param destinationTableName
     *            the destination table name
     * @return the generated task name
     */
    public static String getTaskName(Region sourceRegion, Region destinationRegion, String suppliedTaskName,
                                     String sourceTableName, String destinationTableName) {
        String taskName;
        if (!Strings.isNullOrEmpty(suppliedTaskName)) {
            taskName = DynamoDBConnectorConstants.SERVICE_PREFIX + suppliedTaskName;
            if (taskName.length() > DynamoDBConnectorConstants.DYNAMODB_TABLENAME_LIMIT) {
                throw new IllegalArgumentException("Provided taskname is too long!");
            }
        } else {
            taskName = sourceRegion.getName() + sourceTableName + destinationRegion.getName() + destinationTableName;
            // hash stack name using MD5
            if (DynamoDBConnectorConstants.MD5_DIGEST == null) {
                // see if we can generate a taskname without hashing
                if (taskName.length() > DynamoDBConnectorConstants.DYNAMODB_TABLENAME_LIMIT) { // must hash the taskname
                    throw new IllegalArgumentException(
                        "Generated taskname is too long and cannot be hashed due to improperly initialized MD5 digest object!");
                }
            } else {
                try {
                    taskName = DynamoDBConnectorConstants.SERVICE_PREFIX
                            + new String(Hex.encodeHex(DynamoDBConnectorConstants.MD5_DIGEST.digest(taskName
                            .getBytes(DynamoDBConnectorConstants.BYTE_ENCODING))));
                } catch (UnsupportedEncodingException e) {
                    throw new IllegalArgumentException("taskName was not encoded as " + DynamoDBConnectorConstants.BYTE_ENCODING, e);
                }
            }
        }

        return taskName;
    }

    /**
     * Check whether Streams is enabled on the given argument with the given stream view type
     * 
     * @param streamsClient 
     *            streams client used to access the given stream
     * @param streamArn
     *            the stream ARN to check against
     * @param viewType
     *            the stream view type to check against
     * @return a boolean indicating whether the given stream is enabled and matches the given stream view type
     */
    public static boolean isStreamsEnabled(AmazonDynamoDBStreams streamsClient, String streamArn,
                                           StreamViewType viewType) {
        // Get and check stream description
        StreamDescription result = streamsClient.describeStream(new DescribeStreamRequest().withStreamArn(streamArn))
            .getStreamDescription();
        if (result.getStreamStatus().equalsIgnoreCase(DynamoDBConnectorConstants.ENABLED_STRING)
            && result.getStreamViewType().equalsIgnoreCase(viewType.toString())) {
            return true;
        }
        log.error(DynamoDBConnectorConstants.STREAM_NOT_READY + " StreamARN: " + streamArn);
        return false;
    }
}
