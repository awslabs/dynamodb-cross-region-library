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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.amazonaws.services.dynamodbv2.model.StreamViewType;

import lombok.extern.log4j.Log4j;

@Log4j
public class DynamoDBConnectorConstants {

    /**
     * Command Line Messages
     */
    public static final String MSG_INVALID_PIPELINE = "Pipeline is not recognized.";
    public static final String MSG_NO_STREAMS_FOUND = "Source table does not have Streams enabled.";
    public static final String STREAM_NOT_READY = "Source table does not have Streams enabled, or does not have ViewType NEW_AND_OLD_IMAGES.";

    /**
     * Service-related status
     */
    public static final String ENABLED_STRING = "Enabled";
    public static final StreamViewType NEW_AND_OLD = StreamViewType.NEW_AND_OLD_IMAGES;

    /**
     * Prefixes, suffixes and limits
     */
    public static final String SERVICE_PREFIX = "DynamoDBCrossRegionReplication";
    public static final String STREAMS_PREFIX = "streams.";
    public static final String PROTOCOL_REGEX = "^(https?://)?(.+)";
    public static final int DYNAMODB_TABLENAME_LIMIT = 255;

    /**
     * KCL constants
     */
    public static final int IDLE_TIME_BETWEEN_READS = 500;
    public static final int STREAMS_RECORDS_LIMIT = 1000;
    public static final int KCL_FAILOVER_TIME = 60000;
    public static final long DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS = 10000L;
    public static final String WORKER_LABEL = "worker";

    /**
     * MD5 digest instance
     */
    private static final String HASH_ALGORITHM = "MD5";
    public static final String BYTE_ENCODING = "UTF-8";
    public static final MessageDigest MD5_DIGEST = getMessageDigestInstance(HASH_ALGORITHM);

    private static MessageDigest getMessageDigestInstance(String hashAlgorithm) {
        try {
            return MessageDigest.getInstance(hashAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            log.error("Specified hash algorithm does not exist: " + hashAlgorithm + e);
            return null;
        }
    }
}
