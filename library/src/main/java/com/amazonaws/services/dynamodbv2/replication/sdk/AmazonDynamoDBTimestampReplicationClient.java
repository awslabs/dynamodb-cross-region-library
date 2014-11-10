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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.replication.ReplicationPolicy;
import com.amazonaws.services.dynamodbv2.replication.impl.BasicTimestampReplicationPolicy;
import com.fasterxml.jackson.databind.util.ISO8601Utils;

public class AmazonDynamoDBTimestampReplicationClient extends AmazonDynamoDBReplicationClient {
    /**
     * The timestamp attribute type.
     */
    private static final ScalarAttributeType TIMESTAMP_TYPE = ScalarAttributeType.S;
    /**
     * All timestamps in UTC.
     */
    private static final String GMT = "GMT";

    private static final Collection<AttributeDefinition> REQUIRED_ATTRIBUTES = Collections
        .unmodifiableCollection(Arrays.asList(new AttributeDefinition(BasicTimestampReplicationPolicy.TIMESTAMP_KEY,
            TIMESTAMP_TYPE)));

    /**
     * Constructs a new client to invoke service methods on AmazonDynamoDBv2 using the specified AWS account credentials
     * and client configuration options. Supports cross region replication with forward Timestamp as the conflict
     * resolution policy.
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
    public AmazonDynamoDBTimestampReplicationClient(final AWSCredentials awsCredentials,
        final ClientConfiguration clientConfiguration, final ReplicationPolicy replicationPolicy) {
        super(awsCredentials, clientConfiguration, replicationPolicy);
    }

    /**
     * Constructs a new client to invoke service methods on AmazonDynamoDBv2 using the specified AWS account
     * credentials. Supports cross region replication with forward Timestamp as the conflict resolution policy.
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
    public AmazonDynamoDBTimestampReplicationClient(final AWSCredentials awsCredentials,
        final ReplicationPolicy replicationPolicy) {
        super(awsCredentials, replicationPolicy);
    }

    /**
     * Constructs a new client to invoke service methods on AmazonDynamoDBv2 using the specified AWS account credentials
     * provider and client configuration options. Supports cross region replication with forward Timestamp as the
     * conflict resolution policy.
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
    public AmazonDynamoDBTimestampReplicationClient(final AWSCredentialsProvider awsCredentialsProvider,
        final ClientConfiguration clientConfiguration, final ReplicationPolicy replicationPolicy) {
        super(awsCredentialsProvider, clientConfiguration, replicationPolicy);
    }

    /**
     * Constructs a new client to invoke service methods on AmazonDynamoDBv2 using the specified AWS account credentials
     * provider, client configuration options and request metric collector. Supports cross region replication with
     * forward Timestamp as the conflict resolution policy.
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
    public AmazonDynamoDBTimestampReplicationClient(final AWSCredentialsProvider awsCredentialsProvider,
        final ClientConfiguration clientConfiguration, final RequestMetricCollector requestMetricCollector,
        final ReplicationPolicy replicationPolicy) {
        super(awsCredentialsProvider, clientConfiguration, requestMetricCollector, replicationPolicy);
    }

    /**
     * Constructs a new client to invoke service methods on AmazonDynamoDBv2 using the specified AWS account credentials
     * provider. Supports cross region replication with forward Timestamp as the conflict resolution policy.
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
    public AmazonDynamoDBTimestampReplicationClient(final AWSCredentialsProvider awsCredentialsProvider,
        final ReplicationPolicy replicationPolicy) {
        super(awsCredentialsProvider, replicationPolicy);
    }

    /**
     * Constructs a new client to invoke service methods on AmazonDynamoDBv2. Supports cross region replication with
     * forward Timestamp as the conflict resolution policy. A credentials provider chain will be used that searches for
     * credentials in this order:
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
    public AmazonDynamoDBTimestampReplicationClient(final ClientConfiguration clientConfiguration,
        final ReplicationPolicy replicationPolicy) {
        super(clientConfiguration, replicationPolicy);
    }

    /**
     * Constructs a new client to invoke service methods on AmazonDynamoDBv2. Supports cross region replication with
     * forward Timestamp as the conflict resolution policy. A credentials provider chain will be used that searches for
     * credentials in this order:
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
    public AmazonDynamoDBTimestampReplicationClient(final ReplicationPolicy replicationPolicy) {
        super(replicationPolicy);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, AttributeValue> getGeneratedAttributes() {
        final Map<String, AttributeValue> generatedAttributes = new HashMap<String, AttributeValue>();
        final String timestamp = ISO8601Utils.format(new Date(), true, TimeZone.getTimeZone(GMT));
        final String nonce = UUID.randomUUID().toString();
        final AttributeValue timestampPlusNonce = new AttributeValue(timestamp + nonce);

        generatedAttributes.put(BasicTimestampReplicationPolicy.TIMESTAMP_KEY, timestampPlusNonce);
        return generatedAttributes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<AttributeDefinition> getRequiredAttributes() {
        return REQUIRED_ATTRIBUTES;
    }

}
