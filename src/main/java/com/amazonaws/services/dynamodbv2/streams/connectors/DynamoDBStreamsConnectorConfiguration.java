/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streams.connectors;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * This class contains constants used to configure the DynamoDB Streams Connector. The user should use System properties
 * to set their proper configuration. An instance of DynamoDBStreamsConnectorConfiguration is created with System
 * properties and an AWSCredentialsProvider.
 */
public class DynamoDBStreamsConnectorConfiguration extends KinesisConnectorConfiguration {

    /**
     * Default cross region replication map of region to tables, containing a single default region and single default
     * test table.
     */
    public static final Map<String, List<String>> DEFAULT_DYNAMODB_REGIONS_TO_TABLES =
            ImmutableMap.<String, List<String>>of(DEFAULT_REGION_NAME, Lists.newArrayList(DEFAULT_DYNAMODB_DATA_TABLE_NAME));

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

    }
}
