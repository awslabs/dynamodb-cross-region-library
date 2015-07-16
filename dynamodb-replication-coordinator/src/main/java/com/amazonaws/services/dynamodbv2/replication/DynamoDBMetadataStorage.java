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
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.ConsistentReads;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.Constants;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;

/**
 * DynamoDB table implementation of replication metadata storage, this class follows the singleton design pattern so the instances can be shared
 */
public class DynamoDBMetadataStorage implements MetadataStorage {

    /**
     * Single instance of the DynamoDBMetadataStorage class
     */
    private static DynamoDBMetadataStorage INSTANCE;

    /**
     * Logger for the class
     */
    private static final Logger LOGGER = Logger.getLogger(DynamoDBMetadataStorage.class);

    /**
     * Hashkey of the metadata table
     */
    private static final String HASH_KEY = "ReplicationGroupUUID";

    /**
     * Projection expression for a scan restricted to just the hash key of the metadata table
     */
    private static final String SCAN_PROJECTION_EXPRESSION = HASH_KEY;

    /**
     * DynamoDBMapper used to access and manipulate metadata table information
     */
    private static DynamoDBMapper MAPPER;

    /**
     * DynamoDB table object used to access metadata table
     */
    private static Table TABLE;

    /**
     * DynamoDB metadata table endpoint
     */
    private static String ENDPOINT;

    /**
     * Indicates whether the class has been initialized
     */
    private static boolean initialized = false;

    /*
     * Delimiter used when appending customer user agent
     */
    private static final String USER_AGENT_DELIMITER = ";";

    /**
     * Private constructor for DynamoDBMetadataStorage to prevent it from being instantiated by other classes
     */
    private DynamoDBMetadataStorage() {
    }

    /**
     * Private constructor used by the initialization method to create the singleton instance of the class
     *
     * @param credentialsProvider
     *            the credential to used when accessing the metadata table
     * @param endpoint
     *            the endpoint at which the metadata table can be accessed
     * @param tableName
     *            the name of the metadata table
     */
    private DynamoDBMetadataStorage(AWSCredentialsProvider credentialsProvider, String endpoint, String tableName) {
        ClientConfiguration clientConfigs = new ClientConfiguration();
        clientConfigs.setUserAgent(clientConfigs.getUserAgent() + USER_AGENT_DELIMITER + Constants.USER_AGENT);
        ENDPOINT = endpoint;
        AmazonDynamoDB dynamodb = new AmazonDynamoDBClient(credentialsProvider, clientConfigs);
        dynamodb.setEndpoint(endpoint);
        TABLE = new Table(dynamodb, tableName);
        DynamoDBMapperConfig config = new DynamoDBMapperConfig.Builder().withConsistentReads(ConsistentReads.CONSISTENT)
            .withTableNameOverride(new TableNameOverride(tableName)).build();
        MAPPER = new DynamoDBMapper(dynamodb, config);
    }

    /**
     * Init method for the class, using the given parameters, should only be called once when instantiating this class.
     *
     * @param credentialsProvider
     *            the credential to used when accessing the metadata table
     * @param endpoint
     *            the endpoint at which the metadata table can be accessed
     * @param tableName
     *            the name of the metadata table
     */
    public static void init(AWSCredentialsProvider credentialsProvider, String endpoint, String tableName) {
        if (initialized) {
            String errMessage = "DynamoDBMetadataStorage should not be initialized twice";
            LOGGER.error(errMessage);
            throw new IllegalStateException(errMessage);
        }
        INSTANCE = new DynamoDBMetadataStorage(credentialsProvider, endpoint, tableName);
        initialized = true;
    }

    /**
     * Retrieves the initialized instance of DynamoDBMetadataStorage, otherwise throw an uninitialized error
     *
     * @return the initialized instance of DynamoDBMetadataStorage
     */
    public static DynamoDBMetadataStorage getInstance() throws IllegalStateException {
        if (!initialized) {
            String errMessage = "DynamoDBMetadataStorage class has not been initialized. Please call DynamoDBMetadataStorage.init(credsProvider, endpoint, tablename) to initialize.";
            LOGGER.error(errMessage);
            throw new IllegalStateException(errMessage);
        }
        return INSTANCE;
    }

    /**
     * Retrieve the name of the metadata table this instance has been initialized with
     *
     * @return name of the metadata table
     */
    public String getMetadataTableName() {
        if (!initialized) {
            String errMessage = "DynamoDBMetadataStorage class has not been initialized. Please call DynamoDBMetadataStorage.init(credsProvider, endpoint, tablename) to initialize.";
            LOGGER.error(errMessage);
            throw new IllegalStateException(errMessage);
        }
        return TABLE.getTableName();
    }

    /**
     * Retrieves the endpoint of the metadata table this instance has been initialized with
     *
     * @return endpoint of the metadata table
     */
    public String getMetadataTableEndpoint() {
        if (!initialized) {
            String errMessage = "DynamoDBMetadataStorage class has not been initialized. Please call DynamoDBMetadataStorage.init(credsProvider, endpoint, tablename) to initialize.";
            LOGGER.error(errMessage);
            throw new IllegalStateException(errMessage);
        }
        return ENDPOINT;
    }

    @Override
    public DynamoDBReplicationGroup compareAndWriteReplicationGroup(DynamoDBReplicationGroup expectedValue, DynamoDBReplicationGroup newValue)
        throws IOException {
        validate(expectedValue, newValue);
        do {
            try {
                if (expectedValue == null && newValue != null) {
                    MAPPER.save(newValue);
                } else if (expectedValue != null) {
                    if (newValue == null) {
                        MAPPER.delete(expectedValue);
                    } else {
                        MAPPER.save(newValue);
                    }
                } else {
                    // Redundant. Should not pass validate
                    throw new IllegalArgumentException("expectedValue and newValue cannot both be null");
                }
                return newValue;
            } catch (ProvisionedThroughputExceededException e) {
                continue; // retryable
            } catch (InternalServerErrorException e) {
                continue; // retryable
            } catch (ConditionalCheckFailedException e) {
                if (expectedValue != null) {
                    return readReplicationGroup(expectedValue.getReplicationGroupUUID());
                } else {
                    return readReplicationGroup(newValue.getReplicationGroupUUID());
                }
            } catch (AmazonClientException e) {
                throw new IOException(e);
            }
        } while (true);
    }

    @Override
    public DynamoDBReplicationGroup readReplicationGroup(String replicationGroupUUID) throws IOException {
        DynamoDBReplicationGroup key = new DynamoDBReplicationGroup().withReplicationGroupUUID(replicationGroupUUID);
        do {
            try {
                return MAPPER.load(key);
            } catch (ProvisionedThroughputExceededException e) {
                continue; // retryable
            } catch (InternalServerErrorException e) {
                continue; // retryable
            } catch (AmazonClientException e) {
                throw new IOException(e);
            }
        } while (true);
    }

    @Override
    public List<String> readReplicationGroups() throws IOException {
        List<String> replicationGroups = new ArrayList<String>();
        try {
            ItemCollection<ScanOutcome> items = TABLE.scan(null /* filter expression */, SCAN_PROJECTION_EXPRESSION, null /* name map */, null /* value map */);
            for (Item item : items) {
                replicationGroups.add(item.getString(HASH_KEY));
            }
        } catch (AmazonClientException e) {
            throw new IOException(e);
        }
        return replicationGroups;
    }

    /**
     * Check that the expected and new values are valid. Both values cannot be null. If the value is non-null, the required fields must have values. If both
     * values are non-null, the values must not be equal and the version numbers must match.
     *
     * @param expectedValue
     *            The expected value for the replication group
     * @param newValue
     *            The new value for the replication group
     * @throws IllegalArgumentException
     *             Invalid values for expectedValue and/or newValue
     */
    private void validate(DynamoDBReplicationGroup expectedValue, DynamoDBReplicationGroup newValue) throws IllegalArgumentException {
        if (expectedValue != null && !expectedValue.isValid()) {
            throw new IllegalArgumentException("Invalid value for expectedValue: " + expectedValue);
        }
        if (newValue != null && !newValue.isValid()) {
            throw new IllegalArgumentException("Invalid value for newValue: " + newValue);
        }
        if (expectedValue == null && newValue == null) {
            throw new IllegalArgumentException("Expected and new values cannot both be null");
        }
        if (expectedValue != null && newValue != null) {
            if (newValue.equals(expectedValue)) {
                throw new IllegalArgumentException("No change in the replication group state, current group: " + expectedValue);
            }
            if (expectedValue.getVersion() == null) {
                throw new IllegalArgumentException("Version number of expected replication group cannot be null!");
            }
            if (newValue.getVersion() == null) {
                throw new IllegalArgumentException("Version number of new replication group cannot be null!");
            }
            if (!newValue.getVersion().equals(expectedValue.getVersion())) {
                throw new IllegalArgumentException("New and expected images must have the same version number");
            }
        }
    }
}
