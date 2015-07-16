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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class contains constants used to configure the DynamoDB Streams Connector. The user should use System properties
 * to set their proper configuration. An instance of DynamoDBStreamsConnectorConfiguration is created with System
 * properties and an AWSCredentialsProvider.
 */
public class DynamoDBStreamsConnectorConfiguration extends KinesisConnectorConfiguration {
    /**
     * Logger for the class.
     */
    private static final Logger LOGGER = Logger.getLogger(DynamoDBStreamsConnectorConfiguration.class);

    /**
     * MAPPER for deserializing configuration parameters.
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Cross region replication property key for map of regions to replica tables.
     */
    public static final String PROP_DYNAMODB_REGIONS_TO_TABLES = "dynamodb_regions_to_tables";

    /**
     * Default cross region replication timestamp key.
     */
    public static final String TIMESTAMP_KEY = "timestamp_key_";

    /**
     * Default cross region replication map of region to tables, containing a single default region and single default
     * test table.
     */
    public static final Map<String, List<String>> DEFAULT_DYNAMODB_REGIONS_TO_TABLES = new HashMap<String, List<String>>();
    static {
        List<String> tables = new ArrayList<String>();
        tables.add(DEFAULT_DYNAMODB_DATA_TABLE_NAME);
        DEFAULT_DYNAMODB_REGIONS_TO_TABLES.put(DEFAULT_REGION_NAME, tables);
    }

    /**
     * Cross region replication configurable DynamoDB map of regions to replica tables.
     */
    private final Map<String, List<String>> dynamoDBRegionsToTables;

    /**
     * Constructor for the DynamoDBStreamsConnectorConfiguration class.
     *
     * @param properties
     *            The system properties passed in.
     * @param credentialsProvider
     *            The AWS credentialsProvider
     */
    public DynamoDBStreamsConnectorConfiguration(final Properties properties,
        final AWSCredentialsProvider credentialsProvider) {
        super(properties, credentialsProvider);
        dynamoDBRegionsToTables = getRegionsToTables(PROP_DYNAMODB_REGIONS_TO_TABLES,
            DEFAULT_DYNAMODB_REGIONS_TO_TABLES, properties);

    }

    /**
     * Deserialzation from String to a Map of String to List of Strings objects for regions to tables.
     *
     * @param property
     *            The property key to retrieve
     * @param defaultValue
     *            The default value to use if deserialization fails
     * @param properties
     *            The list of system properties passed in
     * @return The Map of String to List of Strings object representing region to table mapping for replication
     */
    private Map<String, List<String>> getRegionsToTables(final String property,
        final Map<String, List<String>> defaultValue, final Properties properties) {
        if (properties.getProperty(property) == null) {
            LOGGER.debug("No required attributes specified in properties");
            return defaultValue;
        }
        try {
            Map<String, List<String>> deserialized = MAPPER.readValue(properties.getProperty(property),
                new TypeReference<Map<String, List<String>>>() {
                });
            return deserialized;
        } catch (IOException e) {
            LOGGER.warn("Not able to retrieved regions to tables map from properties, got exception: " + e);
            return defaultValue;
        }
    }

    /**
     * Getter for the configurable map of regions to replica tables.
     *
     * @return the dynamoDBRegionsToTables
     */
    public Map<String, List<String>> getDynamoDBRegionsToTables() {
        return dynamoDBRegionsToTables;
    }
}
