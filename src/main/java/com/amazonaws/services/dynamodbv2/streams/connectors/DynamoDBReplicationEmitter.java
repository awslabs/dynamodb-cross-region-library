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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsync;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClient;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.ItemCollectionSizeLimitExceededException;
import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;

/**
 * A general emitter for replication DynamoDB writes from a DynamoDB Stream to another DynamoDB table. Assumes the IBuffer implementation deduplicates writes to a single write per
 * item key. Asynchronously makes the writes to the DynamoDB table.
 */
public class DynamoDBReplicationEmitter implements IEmitter<Record> {

    /**
     * CloudWatch Metric for Records that failed.
     */
    private static final String RECORDS_FAILED = "RecordsFailed";
    /**
     * CloudWatch Metric for Records successfully written to the destination table.
     */
    private static final String RECORDS_WRITTEN = "RecordsWritten";
    /**
     * CloudWatch Metric for number of retries to write Records to the destination table.
     */
    private static final String RECORDS_RETRIED = "RecordsRetried";

    private static final int WAIT_TIME_MS = 100;

    // private static final int THREADPOOL_SIZE = 1000;

    /**
     * Logger for the {@link DynamoDBReplicationEmitter} class.
     */
    private static final Logger LOGGER = Logger.getLogger(DynamoDBReplicationEmitter.class);
    /**
     * DynamoDB Replication Emitter User Agent
     */
    public static final String USER_AGENT = "DynamoDBReplicationEmitter-1.0";

    /**
     * AmazonCloudWatch for emitting metrics.
     */
    private static final AtomicReference<AmazonCloudWatchAsync> CLOUDWATCH = new AtomicReference<AmazonCloudWatchAsync>();
    /**
     * Asynchronous DynamoDB client for writing to the DynamoDB table.
     */
    private static final AtomicReference<AmazonDynamoDBAsync> DYNAMODB = new AtomicReference<AmazonDynamoDBAsync>();
    /**
     * Maximum number of threads for the Async clients.
     */
    public static final int MAX_THREADS = 1000;
    /**
     * The DynamoDB endpoint.
     */
    private final String endpoint;
    /**
     * The DynamoDB region.
     */
    private final String region;
    /**
     * The DynamoDB table.
     */
    private final String tableName;
    /**
     * The KCL application name
     */
    private final String applicationName;
    /**
     * Emitter shutdown status. Makes the shutdown process idempotent.
     */
    private boolean isShutdown = false;

    private final boolean skipErrors;

    /**
     * Constructor with default CloudWatch client and default DynamoDBAsync.
     *
     * @param configuration
     *            The configuration for this emitter.
     */
    public DynamoDBReplicationEmitter(final DynamoDBStreamsConnectorConfiguration configuration) {
        this(configuration.APP_NAME, configuration.DYNAMODB_ENDPOINT, configuration.REGION_NAME, configuration.DYNAMODB_DATA_TABLE_NAME,
            (AmazonCloudWatchAsync) new AmazonCloudWatchAsyncClient(new DefaultAWSCredentialsProviderChain(), Executors.newFixedThreadPool(MAX_THREADS)).withRegion(Regions.getCurrentRegion() == null ? Region.getRegion(Regions.US_EAST_1) : Regions.getCurrentRegion()), new DefaultAWSCredentialsProviderChain());
    }

    /**
     * Constructor with given parameters, used for when MultiRegionEmitter creates emitter dynamically.
     *
     * @param endpoint
     *            The endpoint of the emitter
     * @param region
     *            The region of the emitter
     * @param tableName
     *            The tableName the emitter should emit to
     * @param cloudwatch
     *            The cloudwatch client used for this application
     * @param credentialProvider
     *            The credential provider used for the DynamoDB client
     */
    public DynamoDBReplicationEmitter(final String applicationName, final String endpoint, final String region, final String tableName,
        final AmazonCloudWatchAsync cloudwatch, final AWSCredentialsProvider credentialProvider) {
        this(applicationName, endpoint, region, tableName,
                new AmazonDynamoDBAsyncClient(credentialProvider, new ClientConfiguration().withMaxConnections(MAX_THREADS).withRetryPolicy(PredefinedRetryPolicies.DYNAMODB_DEFAULT), Executors.newFixedThreadPool(MAX_THREADS)),
                cloudwatch);
    }

    /**
     * Constructor with given parameters and default CloudWatch client.
     * @param configuration
     *            The configuration for this emitter
     * @param dynamoDBAsync
     *            The DynamoDB client used for this application
     * @param cloudwatch
     *            The cloudwatch client used for this application
     */
    public DynamoDBReplicationEmitter(final DynamoDBStreamsConnectorConfiguration configuration, final AmazonDynamoDBAsync dynamoDBAsync,
        final AmazonCloudWatchAsync cloudwatch) {
        this(configuration.APP_NAME, configuration.DYNAMODB_ENDPOINT, configuration.REGION_NAME, configuration.DYNAMODB_DATA_TABLE_NAME,
                dynamoDBAsync, cloudwatch);
    }

    /**
     * Constructor with given parameters, used for when MultiRegionEmitter creates emitter dynamically.
     *
     * @param endpoint
     *            The endpoint of the emitter
     * @param region
     *            The region of the emitter
     * @param tableName
     *            The tableName the emitter should emit to
     * @param dynamoDBAsync
     *            The DynamoDB client used for this application
     * @param cloudwatch
     *            The cloudwatch client used for this application
     */
    public DynamoDBReplicationEmitter(final String applicationName, final String endpoint, final String region, final String tableName,
        final AmazonDynamoDBAsync dynamoDBAsync, final AmazonCloudWatchAsync cloudwatch) {
        this.applicationName = applicationName;
        this.endpoint = endpoint;
        this.region = region;
        this.tableName = tableName;

        final boolean setDynamoDB = DYNAMODB.compareAndSet(null, dynamoDBAsync);
        if (setDynamoDB && dynamoDBAsync != null) {
            DYNAMODB.get().setEndpoint(endpoint);
        }
        final boolean setCloudWatch = CLOUDWATCH.compareAndSet(null, cloudwatch);
        if (setCloudWatch && cloudwatch != null) {
            CLOUDWATCH.get().setRegion(Regions.getCurrentRegion() == null ? Region.getRegion(Regions.US_EAST_1) : Regions.getCurrentRegion());
        }
        skipErrors = false; // TODO make configurable
    }

    /**
     * Creates a DynamoDB write request based on the DynamoDB Stream record.
     *
     * @param record
     *            The DynamoDB Stream record containing information about the update to a DynamoDB table
     * @return A DynamoDB request based on the DynamoDB Stream record
     */
    private AmazonWebServiceRequest createRequest(final Record record) {
        final String eventName = record.getEventName();
        final AmazonWebServiceRequest request;
        if (eventName.equalsIgnoreCase(OperationType.INSERT.toString()) || eventName.equalsIgnoreCase(OperationType.MODIFY.toString())) {
            // For INSERT or MODIFY: Put the new image in the DynamoDB table
            PutItemRequest putItemRequest = new PutItemRequest();
            putItemRequest.setItem(record.getDynamodb().getNewImage());
            putItemRequest.setTableName(getTableName());
            request = putItemRequest;
        } else if (eventName.equalsIgnoreCase(OperationType.REMOVE.toString())) {
            // For REMOVE: Delete the item from the DynamoDB table
            DeleteItemRequest deleteItemRequest = new DeleteItemRequest();
            deleteItemRequest.setKey(record.getDynamodb().getKeys());
            deleteItemRequest.setTableName(getTableName());
            request = deleteItemRequest;
        } else {
            // This should only happen if DynamoDB Streams adds/changes its operation types
            LOGGER.warn("Unsupported operation type detected: " + eventName + ". Record: " + record);
            request = null;
        }
        if (null != request) {
            request.getRequestClientOptions().appendUserAgent(USER_AGENT);
        }
        return request;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Record> emit(final UnmodifiableBuffer<Record> buffer) {
        if (isShutdown) {
            if (buffer.getRecords().isEmpty()) {
                // This is OK, but not expected
                LOGGER.warn("Record processor called emit after calling shutdown. Continuing becuase buffer is empty.");
                return Collections.emptyList();
            } else {
                throw new IllegalStateException("Cannot emit records after emitter has been shutdown.");
            }
        }
        // Asynchronously process all writes, but block on the results.
        List<Record> records = buffer.getRecords();
        // Stores records that failed with a non-retryable exception
        final List<Record> failedRecords = Collections.synchronizedList(new ArrayList<Record>());
        // Queue of records to submit
        final BlockingQueue<Record> toSubmit = new LinkedBlockingQueue<Record>(records);
        // Used to detect when all requests have either succeeded or resulted in a non-retryable exception
        final CountDownLatch doneSignal = new CountDownLatch(records.size());
        final AtomicInteger retryCount = new AtomicInteger();
        boolean interrupted = false;
        try {
            while (doneSignal.getCount() > 0) {
                Record recordToSubmit = null;
                try {
                    recordToSubmit = toSubmit.poll(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
                final Record record = recordToSubmit;
                if (null == record) {
                    continue; // Check if all records have completed and if not try to poll again
                }
                // Generate the request based on the record
                AmazonWebServiceRequest request = createRequest(record);
                if (request == null) { // Should only happen if DynamoDB Streams API updates to support different operations
                                       // than {INSERT, MODIFY, REMOVE}.
                    continue;
                }
                // Submit the write request based on its type
                if (request instanceof PutItemRequest) { // PUT
                    getDynamodb().putItemAsync((PutItemRequest) request,
                        (AsyncHandler<PutItemRequest, PutItemResult>) getHandler(toSubmit, failedRecords, retryCount, doneSignal, record));
                } else if (request instanceof DeleteItemRequest) { // DELETE
                    getDynamodb().deleteItemAsync((DeleteItemRequest) request,
                        (AsyncHandler<DeleteItemRequest, DeleteItemResult>) getHandler(toSubmit, failedRecords, retryCount, doneSignal, record));
                } else if (request instanceof UpdateItemRequest) { // UPDATE
                    getDynamodb().updateItemAsync((UpdateItemRequest) request,
                        (AsyncHandler<UpdateItemRequest, UpdateItemResult>) getHandler(toSubmit, failedRecords, retryCount, doneSignal, record));
                } else { // Should only happen if DynamoDB allows a new operation other than {PutItem, DeleteItem,
                         // UpdateItem} for single item writes.
                    LOGGER.warn("Unsupported DynamoDB request: " + request);
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        emitCloudWatchMetrics(records, failedRecords, retryCount);
        if (!records.isEmpty()) {
            LOGGER.debug("Successfully emitted " + (records.size() - failedRecords.size()) + " records ending with sequence number "
                + buffer.getLastSequenceNumber());
        } else {
            LOGGER.debug("No records to emit");
        }
        return failedRecords;
    }

    private AsyncHandler<? extends AmazonWebServiceRequest, ?> getHandler(final BlockingQueue<Record> toSubmit, final List<Record> failedRecords,
        final AtomicInteger retryCount, final CountDownLatch doneSignal, final Record record) {
        return new AsyncHandler<AmazonWebServiceRequest, Object>() {
            @Override
            public void onError(Exception exception) {
                if (isRetryable(exception)) { // Throttling or 500 response
                    retryCount.incrementAndGet();
                    // Retryable
                    while (!toSubmit.offer(record)) {
                        ; // NOP
                    }
                } else if (exception instanceof ItemCollectionSizeLimitExceededException) {
                    // Not Retryable, but from DynamoDB
                    LOGGER.error("Local Secondary Index is full: " + record, exception);
                    if (skipErrors) {
                        failedRecords.add(record);
                        doneSignal.countDown();
                    } else {
                        System.exit(StatusCodes.EIO);
                    }
                } else if (exception instanceof AmazonServiceException && 413 == ((AmazonServiceException) exception).getStatusCode()) {
                    LOGGER.error("Request entity too large: " + record, exception);
                    if (skipErrors) {
                        failedRecords.add(record);
                        doneSignal.countDown();
                    } else {
                        System.exit(StatusCodes.EIO);
                    }
                } else if (exception instanceof AmazonClientException) {
                    // This block catches unrecoverable AmazonWebServices errors:
                    //
                    // ConditionalCheckFailedException - not possible as we are not making conditional writes
                    // LimitExceededException - not possible for PutItem, UpdateItem, or DeleteItem
                    // ResourceInUseException - not possible for PutItem, UpdateItem, or DeleteItem
                    // ResourceNotFoundException - table does not exist
                    // AmazonServiceException - any unhandled response from the DynamoDB service
                    // AmazonClientException - any other 400 response: validation, authentication, authorization, or configuration exception
                    //
                    LOGGER.fatal("Exception emitting record: " + record, exception);
                    System.exit(StatusCodes.EIO);
                } else {
                    // This block catches all other exceptions. Since it was not expected, this is an unrecoverable exception.
                    LOGGER.fatal("Abnormal exception emitting record: " + record, exception);
                    System.exit(StatusCodes.EIO);
                }
            }

            private boolean isRetryable(Exception exception) {
                if (exception instanceof ProvisionedThroughputExceededException) {
                    return true;
                } else if (exception instanceof InternalServerErrorException) {
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            public void onSuccess(AmazonWebServiceRequest request, Object result) {
                LOGGER.trace("Record emitted successfully: " + record.getDynamodb().getSequenceNumber());
                doneSignal.countDown();
            }
        };
    }

    /**
     * Emit CloudWatch metrics based on the records submitted for processing and failed writes.
     *
     * @param records
     *            The records submitted for processing
     * @param failures
     *            The records that failed to write to DynamoDB
     */
    protected synchronized void emitCloudWatchMetrics(final List<Record> records, final List<Record> failures, final AtomicInteger retryCount) {
        AmazonCloudWatchAsync cloudwatch = CLOUDWATCH.get();
        if (null == cloudwatch) {
            return;
        }
        if (isShutdown) {
            if (records.isEmpty() && failures.isEmpty()) {
                LOGGER.warn("emitCloudWatchMetrics called after shutdown. Continuing because records and failures lists are empty");
                return;
            } else {
                throw new IllegalStateException("emitCloudWatchMetrics called after shutdown");
            }
        }
        final List<MetricDatum> metrics = new ArrayList<MetricDatum>();
        final double successful = records.size() - failures.size();
        if (successful > 0) {
            metrics.add(new MetricDatum().withMetricName(RECORDS_WRITTEN).withValue(successful).withUnit(StandardUnit.Count).withTimestamp(new Date()));
        }
        final double retries = retryCount.get();
        if (retries > 0) {
            metrics.add(new MetricDatum().withMetricName(RECORDS_RETRIED).withValue(retries).withUnit(StandardUnit.Count).withTimestamp(new Date()));
        }
        if (metrics.isEmpty()) {
            return;
        }
        final PutMetricDataRequest request = new PutMetricDataRequest().withNamespace(applicationName).withMetricData(metrics);
        cloudwatch.putMetricDataAsync(request, new AsyncHandler<PutMetricDataRequest, PutMetricDataResult>() {
            @Override
            public void onSuccess(PutMetricDataRequest request, PutMetricDataResult result) {
                LOGGER.trace("Published metric: " + request);
            }

            @Override
            public void onError(Exception exception) {
                LOGGER.error("Could not publish metric: " + request, exception);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fail(final List<Record> records) {
        if (isShutdown) {
            if (records.isEmpty()) {
                // This is OK (but not expected)
                LOGGER.warn("Emitter fail method called after shutdown method was called. Continuing because list is empty");
                return;
            } else {
                throw new IllegalStateException("Emitter fail method called after shutdown method was called.");
            }
        }
        for (Record record : records) {
            LOGGER.error("Could not emit record: " + record);
        }
        final AmazonCloudWatchAsync cloudwatch = CLOUDWATCH.get();
        if (null != cloudwatch) {
            final double failed = records.size();
            final MetricDatum recordsProcessedFailedDatum = new MetricDatum().withMetricName(RECORDS_FAILED).withValue(failed).withUnit(StandardUnit.Count)
                .withTimestamp(new Date());
            final PutMetricDataRequest request = new PutMetricDataRequest().withNamespace(applicationName).withMetricData(recordsProcessedFailedDatum);
            cloudwatch.putMetricDataAsync(request);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown() {
        if (isShutdown) {
            LOGGER.warn("shutdown called multiple times");
            return;
        }
        isShutdown = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "DynamoDBReplicationEmitter [endpoint=" + getEndpoint() + ", region=" + getRegion() + ", tableName=" + getTableName() + "]";
    }

    /**
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @return the dynamodb
     */
    public AmazonDynamoDBAsync getDynamodb() {
        return DYNAMODB.get();
    }

    /**
     * @return the endpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * @return the region
     */
    public String getRegion() {
        return region;
    }

}
