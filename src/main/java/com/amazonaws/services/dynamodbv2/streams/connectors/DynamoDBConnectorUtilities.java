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

import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Strings;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.util.AwsHostNameUtils;

public class DynamoDBConnectorUtilities {

    /**
     * Logger for the {@link DynamoDBConnectorUtilities} class.
     */
    private static final Logger LOGGER = Logger.getLogger(DynamoDBConnectorUtilities.class);

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
    public static String getTaskName(String sourceRegion, String destinationRegion, String suppliedTaskName,
                                     String sourceTableName, String destinationTableName) {
        String taskName;
        if (!Strings.isNullOrEmpty(suppliedTaskName)) {
            taskName = DynamoDBConnectorConstants.SERVICE_PREFIX + suppliedTaskName;
            if (taskName.length() > DynamoDBConnectorConstants.DYNAMODB_TABLENAME_LIMIT) {
                throw new IllegalArgumentException("Provided taskname is too long!");
            }
        } else {
            taskName = sourceRegion + sourceTableName + destinationRegion + destinationTableName;
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
     * Convert a given endpoint into its corresponding region
     *
     * @param endpoint
     *            given endpoint URL
     * @return the extracted region corresponding to the given endpoint
     */
    public static Region getRegionFromEndpoint(String endpoint) {
        final String regionName = AwsHostNameUtils.parseRegion(endpoint, null);
        if (regionName == null) {
            return null;
        }
        return Region.getRegion(Regions.fromName(regionName));
    }

    /**
     * Convert a given DynamoDB endpoint into its corresponding Streams endpoint
     *
     * @param endpoint
     *            given endpoint URL
     * @return the extracted Streams endpoint corresponding to the given DynamoDB endpoint
     */
    public static String getStreamsEndpoint(String endpoint) {
        if (!endpoint.contains("dynamodb")) {
            return endpoint; //only try to convert canonical
        }
        String regex = DynamoDBConnectorConstants.PROTOCOL_REGEX;
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(endpoint);
        String ret;
        if (matcher.matches()) {
            ret = ((matcher.group(1) == null) ? "" : matcher.group(1)) + DynamoDBConnectorConstants.STREAMS_PREFIX
                + matcher.group(2);
        } else {
            ret = DynamoDBConnectorConstants.STREAMS_PREFIX + endpoint;
        }
        return ret;
    }

    /**
     * Get the current region according to the metadata on the instance
     * 
     * @return current region, or US-EAST-1 if no metadata exists
     */
    public static Region getCurRegion() {
        return Regions.getCurrentRegion() == null ? Region.getRegion(Regions.US_EAST_1) : Regions.getCurrentRegion();
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
        LOGGER.error(DynamoDBConnectorConstants.STREAM_NOT_READY + " StreamARN: " + streamArn);
        return false;
    }
}
