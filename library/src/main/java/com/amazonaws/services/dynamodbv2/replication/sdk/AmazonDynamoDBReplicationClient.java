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
package com.amazonaws.services.dynamodbv2.replication.sdk;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;
import com.amazonaws.services.dynamodbv2.replication.ReplicationPolicy;

/**
 * Wraps the {@link AmazonDynamoDBClient} for replication.
 */
public abstract class AmazonDynamoDBReplicationClient extends AmazonDynamoDBClient {
    /**
     * Logger for the Client.
     */
    protected static final Logger LOGGER = LoggerFactory.getLogger(AmazonDynamoDBReplicationClient.class);
    /**
     * Flag for a user update.
     */
    public static final String USER_UPDATE_KEY = "USER___________UPDATE";
    /**
     * Value for the user update attribute.
     */
    public static final AttributeValue USER_UPDATE_VALUE = new AttributeValue().withNULL(true);
    /**
     * Flag for an item tombstone.
     */
    public static final String TOMBSTONE_KEY = "TOMBSTONE___________DELETE";
    /**
     * Value for an item tombstone.
     */
    public static final AttributeValue TOMBSTONE_VALUE = new AttributeValue().withNULL(true);

    /**
     * The {@link ReplicationPolicy} for this replication application.
     */
    private final ReplicationPolicy replicationPolicy;

    /**
     * Constructs a new client to invoke service methods on AmazonDynamoDBv2 using the specified AWS account credentials
     * and client configuration options. Supports cross region replication.
     * 
     * <p>
     * All service calls made using this new client object are blocking, and will not return until the service call
     * completes.
     * </p>
     * 
     * @param awsCredentials
     *            The AWS credentials (access key ID and secret key) to use when authenticating with AWS services.
     * @param clientConfiguration
     *            The client configuration options controlling how this client connects to AmazonDynamoDBv2 (ex: proxy
     *            settings, retry counts, etc.).
     * @param replicationPolicy
     *            The {@link ReplicationPolicy} for this replication application.
     */
    public AmazonDynamoDBReplicationClient(final AWSCredentials awsCredentials,
        final ClientConfiguration clientConfiguration, final ReplicationPolicy replicationPolicy) {
        super(awsCredentials, clientConfiguration);
        this.replicationPolicy = replicationPolicy;
    }

    /**
     * Constructs a new client to invoke service methods on AmazonDynamoDBv2 using the specified AWS account
     * credentials. Supports cross region replication.
     * 
     * <p>
     * All service calls made using this new client object are blocking, and will not return until the service call
     * completes.
     * </p>
     * 
     * @param awsCredentials
     *            The AWS credentials (access key ID and secret key) to use when authenticating with AWS services.
     * @param replicationPolicy
     *            The {@link ReplicationPolicy} for this replication application.
     */
    public AmazonDynamoDBReplicationClient(final AWSCredentials awsCredentials,
        final ReplicationPolicy replicationPolicy) {
        super(awsCredentials);
        this.replicationPolicy = replicationPolicy;
    }

    /**
     * Constructs a new client to invoke service methods on AmazonDynamoDBv2 using the specified AWS account credentials
     * provider and client configuration options. Supports cross region replication.
     * 
     * <p>
     * All service calls made using this new client object are blocking, and will not return until the service call
     * completes.
     * </p>
     * 
     * @param awsCredentialsProvider
     *            The AWS credentials provider which will provide credentials to authenticate requests with AWS
     *            services.
     * @param clientConfiguration
     *            The client configuration options controlling how this client connects to AmazonDynamoDBv2 (ex: proxy
     *            settings, retry counts, etc.).
     * @param replicationPolicy
     *            The {@link ReplicationPolicy} for this replication application.
     */
    public AmazonDynamoDBReplicationClient(final AWSCredentialsProvider awsCredentialsProvider,
        final ClientConfiguration clientConfiguration, final ReplicationPolicy replicationPolicy) {
        super(awsCredentialsProvider, clientConfiguration);
        this.replicationPolicy = replicationPolicy;
    }

    /**
     * Constructs a new client to invoke service methods on AmazonDynamoDBv2 using the specified AWS account credentials
     * provider, client configuration options and request metric collector. Supports cross region replication.
     * 
     * <p>
     * All service calls made using this new client object are blocking, and will not return until the service call
     * completes.
     * </p>
     * 
     * @param awsCredentialsProvider
     *            The AWS credentials provider which will provide credentials to authenticate requests with AWS
     *            services.
     * @param clientConfiguration
     *            The client configuration options controlling how this client connects to AmazonDynamoDBv2 (ex: proxy
     *            settings, retry counts, etc.).
     * @param requestMetricCollector
     *            optional request metric collector
     * @param replicationPolicy
     *            The {@link ReplicationPolicy} for this replication application.
     */
    public AmazonDynamoDBReplicationClient(final AWSCredentialsProvider awsCredentialsProvider,
        final ClientConfiguration clientConfiguration, final RequestMetricCollector requestMetricCollector,
        final ReplicationPolicy replicationPolicy) {
        super(awsCredentialsProvider, clientConfiguration, requestMetricCollector);
        this.replicationPolicy = replicationPolicy;
    }

    /**
     * Constructs a new client to invoke service methods on AmazonDynamoDBv2 using the specified AWS account credentials
     * provider. Supports cross region replication.
     * 
     * <p>
     * All service calls made using this new client object are blocking, and will not return until the service call
     * completes.
     * </p>
     * 
     * @param awsCredentialsProvider
     *            The AWS credentials provider which will provide credentials to authenticate requests with AWS
     *            services.
     * @param replicationPolicy
     *            The {@link ReplicationPolicy} for this replication application.
     */
    public AmazonDynamoDBReplicationClient(final AWSCredentialsProvider awsCredentialsProvider,
        final ReplicationPolicy replicationPolicy) {
        super(awsCredentialsProvider);
        this.replicationPolicy = replicationPolicy;
    }

    /**
     * Constructs a new client to invoke service methods on AmazonDynamoDBv2. Supports cross region replication. A
     * credentials provider chain will be used that searches for credentials in this order:
     * <ul>
     * <li>Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY</li>
     * <li>Java System Properties - aws.accessKeyId and aws.secretKey</li>
     * <li>Instance profile credentials delivered through the Amazon EC2 metadata service</li>
     * </ul>
     * 
     * <p>
     * All service calls made using this new client object are blocking, and will not return until the service call
     * completes.
     * </p>
     * 
     * @param clientConfiguration
     *            The client configuration options controlling how this client connects to AmazonDynamoDBv2 (ex: proxy
     *            settings, retry counts, etc.).
     * @param replicationPolicy
     *            The {@link ReplicationPolicy} for this replication application.
     * 
     * @see DefaultAWSCredentialsProviderChain
     */
    public AmazonDynamoDBReplicationClient(final ClientConfiguration clientConfiguration,
        final ReplicationPolicy replicationPolicy) {
        super(clientConfiguration);
        this.replicationPolicy = replicationPolicy;
    }

    /**
     * Constructs a new client to invoke service methods on AmazonDynamoDBv2. Supports cross region replication. A
     * credentials provider chain will be used that searches for credentials in this order:
     * <ul>
     * <li>Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY</li>
     * <li>Java System Properties - aws.accessKeyId and aws.secretKey</li>
     * <li>Instance profile credentials delivered through the Amazon EC2 metadata service</li>
     * </ul>
     * <p>
     * All service calls made using this new client object are blocking, and will not return until the service call
     * completes.
     * </p>
     * 
     * @param replicationPolicy
     *            The {@link ReplicationPolicy} for this replication application.
     * @see DefaultAWSCredentialsProviderChain
     */
    public AmazonDynamoDBReplicationClient(final ReplicationPolicy replicationPolicy) {
        super();
        this.replicationPolicy = replicationPolicy;
    }

    @Override
    public BatchGetItemResult batchGetItem(final BatchGetItemRequest request) {
        // Filter deleted items
        if (replicationPolicy.getDeleteBehavior().equals(ReplicationPolicy.DeleteBehavior.TOMBSTONE)) {
            for (final Entry<String, KeysAndAttributes> tableReads : request.getRequestItems().entrySet()) {
                if (!tableReads.getValue().getAttributesToGet().contains(TOMBSTONE_KEY)) {
                    tableReads.getValue().getAttributesToGet().add(TOMBSTONE_KEY);
                }
            }
        }
        final BatchGetItemResult result = super.batchGetItem(request);
        if (replicationPolicy.getDeleteBehavior().equals(ReplicationPolicy.DeleteBehavior.TOMBSTONE)) {
            for (final Entry<String, List<Map<String, AttributeValue>>> itemList : result.getResponses().entrySet()) {
                final Iterator<Map<String, AttributeValue>> itemIt = itemList.getValue().iterator();
                while (itemIt.hasNext()) {
                    final Map<String, AttributeValue> item = itemIt.next();
                    if (item.containsKey(TOMBSTONE_KEY)) {
                        itemIt.remove();
                    }
                }
            }
        }
        return result;
    }

    /**
     * Batch write is not supported by the replication library because it does not support conditional writes.
     * 
     * @param request
     *            A {@link BatchWriteItemRequest} that will be ignored.
     * @see AmazonDynamoDBClient#batchWriteItem(BatchWriteItemRequest)
     */
    @Override
    public BatchWriteItemResult batchWriteItem(final BatchWriteItemRequest request) throws AmazonServiceException,
        AmazonClientException {
        throw new UnsupportedOperationException("Batch writes do not support conditional writes");
    }

    /**
     * @see AmazonDynamoDBClient#createTable(CreateTableRequest)
     */
    @Override
    public CreateTableResult createTable(final CreateTableRequest request) {
        return super.createTable(request);
    }

    /**
     * Helper method for deleteItem. Converts a delete request into a tombstone put request.
     * 
     * @param request
     *            The delete request to convert
     * @return The tombstone put request
     */
    private PutItemRequest tombstoneRequest(DeleteItemRequest request) {
        PutItemRequest tombstoneReq = new PutItemRequest();
        tombstoneReq.setTableName(request.getTableName());
        tombstoneReq.setItem(request.getKey());
        if (tombstoneReq.getItem() == null) {
            // Missing key in DeleteItemRequest - will result in 400 but not a NPE.
            tombstoneReq.setItem(new HashMap<String, AttributeValue>());
        }
        tombstoneReq.getItem().putAll(getGeneratedAttributes());
        tombstoneReq.getItem().put(TOMBSTONE_KEY, TOMBSTONE_VALUE);
        // Add user write flag
        tombstoneReq.getItem().put(USER_UPDATE_KEY, USER_UPDATE_VALUE);
        String condExpr = replicationPolicy.getConditionExpression();
        if (request.getConditionExpression() == null) {
            tombstoneReq.setConditionExpression(condExpr);
        } else {
            tombstoneReq.setConditionExpression("(" + request.getConditionExpression() + ") AND (" + condExpr + ")");
        }
        Map<String, AttributeValue> newExpressionAttributes = replicationPolicy.getExpressionAttributes(tombstoneReq
            .getItem());
        if (request.getExpressionAttributeValues() == null) {
            tombstoneReq.setExpressionAttributeValues(newExpressionAttributes);
        } else {
            tombstoneReq.getExpressionAttributeValues().putAll(newExpressionAttributes);
        }
        if (request.getReturnValues() != null) {
            ReturnValue rv = ReturnValue.valueOf(request.getReturnValues());
            switch (rv) {
                case ALL_OLD:
                case NONE:
                    tombstoneReq.setReturnValues(rv);
                    break;
                default:
                    throw new IllegalArgumentException("Delete operation supports NONE or ALL_OLD return values");
            }
        }
        return tombstoneReq;
    }

    /**
     * Helper method for deleteItem. Converts a tombstone put result into a delete result.
     * 
     * @param result
     *            The tombstone put result
     * @return The delete result
     */
    private static DeleteItemResult tombstoneResult(PutItemResult tombstoneRes) {
        DeleteItemResult result = new DeleteItemResult();
        result.setAttributes(tombstoneRes.getAttributes());
        result.setConsumedCapacity(tombstoneRes.getConsumedCapacity());
        result.setItemCollectionMetrics(tombstoneRes.getItemCollectionMetrics());
        return result;
    }

    /**
     * @see AmazonDynamoDBClient#deleteItem(DeleteItemRequest)
     */
    @Override
    public DeleteItemResult deleteItem(final DeleteItemRequest request) {
        switch (replicationPolicy.getDeleteBehavior()) {
            case TOMBSTONE:
                PutItemRequest tombstoneReq = tombstoneRequest(request);
                PutItemResult tombstoneRes = super.putItem(tombstoneReq);
                DeleteItemResult result = tombstoneResult(tombstoneRes);
                return result;
            default:
                throw new UnsupportedOperationException("Unsupported delete behavior: "
                    + replicationPolicy.getDeleteBehavior());
        }
    }

    /**
     * @see AmazonDynamoDBClient#deleteTable(DeleteTableRequest)
     */
    @Override
    public DeleteTableResult deleteTable(final DeleteTableRequest request) {
        return super.deleteTable(request);
    }

    /**
     * @see AmazonDynamoDBClient#describeTable(DescribeTableRequest)
     */
    @Override
    public DescribeTableResult describeTable(final DescribeTableRequest request) {
        // Nothing to do yet
        return super.describeTable(request);
    }

    /**
     * Gets auto-generated attributes to add to the item.
     * 
     * @return Auto-generated attributes to add to the item.
     */
    public abstract Map<String, AttributeValue> getGeneratedAttributes();

    /**
     * @see AmazonDynamoDBClient#getItem(GetItemRequest)
     */
    @Override
    public GetItemResult getItem(final GetItemRequest request) {
        // Filter deleted item
        if (replicationPolicy.getDeleteBehavior().equals(ReplicationPolicy.DeleteBehavior.TOMBSTONE)) {
            if (request.getAttributesToGet() != null && !request.getAttributesToGet().contains(TOMBSTONE_KEY)) {
                request.getAttributesToGet().add(TOMBSTONE_KEY);
            }
        }
        final GetItemResult result = super.getItem(request);
        if (replicationPolicy.getDeleteBehavior().equals(ReplicationPolicy.DeleteBehavior.TOMBSTONE)
            && result.getItem().containsKey(TOMBSTONE_KEY)) {
            throw new ResourceNotFoundException("Item not found: " + request.getKey());
        }
        return result;
    }

    /**
     * Gets the required attributes for this replication application.
     * 
     * @return Get required attributes for this replication application.
     */
    public abstract Collection<AttributeDefinition> getRequiredAttributes();

    /**
     * @see AmazonDynamoDBClient#listTables()
     */
    @Override
    public ListTablesResult listTables(final ListTablesRequest request) {
        // Nothing to do yet
        return super.listTables(request);
    }

    /**
     * @see AmazonDynamoDBClient#putItem(PutItemRequest)
     */
    @Override
    public PutItemResult putItem(final PutItemRequest request) {
        // Add user write flag
        request.getItem().put(USER_UPDATE_KEY, USER_UPDATE_VALUE);
        // Add generated attributes
        request.getItem().putAll(getGeneratedAttributes());
        // Set conditions
        String conditions = replicationPolicy.getConditionExpression();
        
        if (request.getConditionExpression() == null) {
            request.setConditionExpression(conditions);
        } else {
            request.setConditionExpression("(" + request.getConditionExpression() + ") AND (" + conditions + ")");
        }
        if (request.getExpressionAttributeValues() == null) {
            request.setExpressionAttributeValues(replicationPolicy.getExpressionAttributes(request.getItem()));
        } else {
            request.getExpressionAttributeValues().putAll(replicationPolicy.getExpressionAttributes(request.getItem()));
        }
        return super.putItem(request);
    }

    /**
     * @see AmazonDynamoDBClient#query(QueryRequest)
     */
    @Override
    public QueryResult query(final QueryRequest request) {
        // Filter deleted records
        if (replicationPolicy.getDeleteBehavior().equals(ReplicationPolicy.DeleteBehavior.TOMBSTONE)) {
            if (request.getQueryFilter() == null) {
                request.setQueryFilter(new HashMap<String, Condition>());
            }
            request.getQueryFilter().put(TOMBSTONE_KEY,
                new Condition().withAttributeValueList(TOMBSTONE_VALUE).withComparisonOperator(ComparisonOperator.NE));
        }
        return super.query(request);
    }

    /**
     * @see AmazonDynamoDBClient#scan(ScanRequest)
     */
    @Override
    public ScanResult scan(final ScanRequest request) {
        // Filter deleted records
        if (replicationPolicy.getDeleteBehavior().equals(ReplicationPolicy.DeleteBehavior.TOMBSTONE)) {
            if (request.getScanFilter() == null) {
                request.setScanFilter(new HashMap<String, Condition>());
            }
            request.getScanFilter().put(TOMBSTONE_KEY,
                new Condition().withAttributeValueList(TOMBSTONE_VALUE).withComparisonOperator(ComparisonOperator.NE));
        }
        return super.scan(request);
    }

    /**
     * @see AmazonDynamoDBClient#updateItem(UpdateItemRequest)
     */
    @Override
    public UpdateItemResult updateItem(final UpdateItemRequest request) {
        final String SET_KEYWORD = "SET";
        final String REMOVE_KEYWORD = "REMOVE";
        final String ADD_KEYWORD = "ADD";
        // if the user tries to set the USER_UPDATE_KEY, we log a warning and
        // the service should throw an exception from double setting the key
        if (request.getUpdateExpression().contains(USER_UPDATE_KEY)) {
            LOGGER.warn(USER_UPDATE_KEY + " found in update expression! Users should not modify this key!");
        }
        // Add user update flag by parsing the update expression to add to the start of SET clause
        String updateExpression = request.getUpdateExpression();
        if (updateExpression.contains(SET_KEYWORD)) {
            updateExpression = updateExpression.substring(0, updateExpression.indexOf(SET_KEYWORD))
                + SET_KEYWORD
                + " "
                + USER_UPDATE_KEY
                + "=:"
                + USER_UPDATE_KEY
                + ","
                + updateExpression.substring(updateExpression.indexOf(SET_KEYWORD) + SET_KEYWORD.length(),
                    updateExpression.length());

        } else {
            updateExpression = SET_KEYWORD + " " + USER_UPDATE_KEY + "=:" + USER_UPDATE_KEY + " "
                + updateExpression;
        }
        request.addExpressionAttributeValuesEntry(":" + USER_UPDATE_KEY, USER_UPDATE_VALUE);

        // Add generated attributes by parsing updateExpressions and adding to start of SET statement
        for (final Entry<String, AttributeValue> generatedAttribute : getGeneratedAttributes().entrySet()) {
            updateExpression = updateExpression.substring(0, updateExpression.indexOf(SET_KEYWORD))
                + SET_KEYWORD
                + " "
                + generatedAttribute.getKey()
                + "=:"
                + generatedAttribute.getKey()
                + ","
                + updateExpression.substring(updateExpression.indexOf(SET_KEYWORD) + SET_KEYWORD.length(),
                    updateExpression.length());
            request.addExpressionAttributeValuesEntry(":" + generatedAttribute.getKey(), generatedAttribute.getValue());
        }

        // Remove the tombstone flag by parsing the update expression and add to the beginning of the REMOVE clause
        if (updateExpression.contains(SET_KEYWORD) || updateExpression.contains(ADD_KEYWORD)) {
            if (updateExpression.contains(REMOVE_KEYWORD)) {
                updateExpression = updateExpression.substring(0, updateExpression.indexOf(REMOVE_KEYWORD))
                    + REMOVE_KEYWORD
                    + " "
                    + TOMBSTONE_KEY
                    + ","
                    + updateExpression.substring(updateExpression.indexOf(REMOVE_KEYWORD) + REMOVE_KEYWORD.length(),
                        updateExpression.length());
            } else {
                updateExpression = REMOVE_KEYWORD + " " + TOMBSTONE_KEY + " " + updateExpression;
            }
        }
        request.setUpdateExpression(updateExpression);

        String conditions = replicationPolicy.getConditionExpression();
        if (request.getConditionExpression() == null) {
            request.setConditionExpression(conditions);
        } else {
            request.setConditionExpression("(" + request.getConditionExpression() + ") AND (" + conditions + ")");
        }
        if (request.getExpressionAttributeValues() == null) {
            request.setExpressionAttributeValues(replicationPolicy.getExpressionAttributes(request));
        } else {
            request.getExpressionAttributeValues().putAll(replicationPolicy.getExpressionAttributes(request));
        }
        return super.updateItem(request);
    }

    /**
     * @see AmazonDynamoDBClient#updateTable(UpdateTableRequest)
     */
    @Override
    public UpdateTableResult updateTable(final UpdateTableRequest request) {
        // Nothing to do yet
        return super.updateTable(request);
    }

}
