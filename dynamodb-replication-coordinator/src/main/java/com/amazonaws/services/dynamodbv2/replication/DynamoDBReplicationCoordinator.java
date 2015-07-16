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

import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.Constants;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.streams.connectors.StatusCodes;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

/**
 * A coordinator instance for the DynamoDB cross region replication application that operates based on status of the replication group, as well as the group's
 * members. Each action is event-driven, detected by {@link DynamoDBReplicationCoordinator}.
 *
 */
public class DynamoDBReplicationCoordinator {

    /*
     * Logger for the class
     */
    private static final Logger LOGGER = Logger.getLogger(DynamoDBReplicationCoordinator.class);

    /*
     * Command Line Messages
     */
    private static final String MSG_NO_STREAMS_FOUND = "No streams found on the metadata table";
    private static final String MSG_STREAMS_NOT_SUPPORTED = "DynamoDB metadata table region does not support Streams.";
    private static final String MSG_ATTR_DEFN_MATCH = "Existing metadata table does not have the correct attribute definitions";
    private static final String MSG_KEY_SCHEMA_MATCH = "Existing metadata table does not have the correct key schema";

    /*
     * Prefixes and suffixes
     */
    private static final String TASKNAME_DELIMITER = "_";
    private static final String DYNAMODB_REPLICATION_PREFIX = "DynamoDBCrossRegionReplication";

    /*
     * Metadata table and worker constants
     */
    private static final String WORKER_LABEL = "worker";
    private static final long DEFAULT_METADATA_THROUGHPUT = 10l;
    private static final long WAITING_TIME_OUT = 60000l;

    /**
     * Command line main method entry point
     *
     * @param args
     *            command line arguments
     */
    public static void main(String[] args) {
        // Initialize command line arguments and JCommander parser
        CommandLineArgs params = new CommandLineArgs();
        JCommander cmd = new JCommander(params);

        try {
            // parse given arguments
            cmd.parse(args);

            // show usage information if help flag exists
            if (params.getHelp()) {
                cmd.usage();
                System.exit(StatusCodes.EINVAL);
            }

            // use default credential provider chain to locate appropriate credentials
            AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

            // initialize DynamoDB metadata storage on the metadata table
            DynamoDBMetadataStorage.init(credentialsProvider, params.getMetadataTableEndpoint(), params.getMetadataTableName());

            // get singleton instance of metadata storage
            DynamoDBMetadataStorage metadataStorage = DynamoDBMetadataStorage.getInstance();

            // initialize DynamoDB client and set the endpoint properly
            AmazonDynamoDBClient dynamodbClient = new AmazonDynamoDBClient(credentialsProvider).withEndpoint(params.getMetadataTableEndpoint());

            // set up the metadata table
            setUpMetadataTable(params, dynamodbClient);

            // run KCL worker on the metadata table to process eventsFs
            runKclWorker(params, dynamodbClient, credentialsProvider, metadataStorage);

        } catch (ParameterException e) {
            LOGGER.error(e.getMessage());
            cmd.usage();
            System.exit(StatusCodes.EINVAL);
        } catch (Exception e) {
            LOGGER.fatal(e.getMessage());
            System.exit(StatusCodes.EINVAL);
        }
    }

    /**
     * Set up the KCL worker on the metadata table based on the coordinator replication event processor
     *
     * @param params
     *            parameters given to the main method, containing metadata table information
     * @param dynamodbClient
     *            DynamoDB client used to access the metadata table
     * @param credentialsProvider
     *            credentials used to access metadata table, streams, and cloudwatch
     */
    private static void runKclWorker(CommandLineArgs params, AmazonDynamoDBClient dynamodbClient, AWSCredentialsProvider credentialsProvider,
        DynamoDBMetadataStorage metadataStorage) throws Exception {
        // obtain the Stream ID associated with the metadata table
        String streamArn = dynamodbClient.describeTable(params.getMetadataTableName()).getTable().getLatestStreamArn();
        if (streamArn == null) {
            throw new ParameterException(MSG_NO_STREAMS_FOUND);
        }

        // extract streams endpoint
        String streamsEndpoint = DynamoDBReplicationUtilities.getStreamsEndpoint(params.getMetadataTableEndpoint());

        // initialize DynamoDB Streams Adapter client and set the Streams endpoint properly
        AmazonDynamoDBStreamsAdapterClient streamsAdapterClient = new AmazonDynamoDBStreamsAdapterClient(credentialsProvider);
        streamsAdapterClient.setEndpoint(streamsEndpoint);

        // initialize CloudWatch client and set the region to emit metrics to
        AmazonCloudWatch cloudWatchClient = new AmazonCloudWatchClient(credentialsProvider);
        cloudWatchClient.setRegion(Region.getRegion(Regions.fromName(DynamoDBReplicationUtilities.getRegionFromEndpoint(params.getMetadataTableEndpoint()))));

        // try to get taskname from command line arguments, auto generate one if needed
        String taskName = getTaskName(params);

        // create map of account to AWS access
        AccountMapToAwsAccess accounts = new AccountMapToAwsAccess();
        accounts.addAwsAccessAccount(params.getAccountId(), new AwsAccess(credentialsProvider));

        // create the record processor factory
        DynamoDBReplicationRecordProcessorFactory factory = new DynamoDBReplicationRecordProcessorFactory(metadataStorage, accounts);
        KinesisClientLibConfiguration kclConfig = new KinesisClientLibConfiguration(taskName, streamArn, credentialsProvider, streamArn + WORKER_LABEL)
            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

        // create KCL worker to consume streams from metadata table and run the worker
        Worker worker = new Worker(factory, kclConfig, streamsAdapterClient, dynamodbClient, cloudWatchClient);
        worker.run();
    }

    /**
     * Set up metadata table if it does not exist, if it does, verify its key schema and streams specification
     *
     * @param params
     *            parameters given to the main method, containing metadata table information
     * @param dynamodbClient
     *            DynamoDB client used to access the metadata table
     */
    public static void setUpMetadataTable(CommandLineArgs params, AmazonDynamoDBClient dynamodbClient) throws Exception {
        // Create default streams specification
        StreamSpecification replicationStreamSpec = new StreamSpecification().withStreamEnabled(DynamoDBReplicationUtilities.defaultStreamEnabled)
            .withStreamViewType(DynamoDBReplicationUtilities.defaultStreamViewType);

        // Create default metadata table configuration
        AttributeDefinition metadataAttributeDefinition = new AttributeDefinition(Constants.REPLICATION_GROUP_UUID, ScalarAttributeType.S);
        KeySchemaElement metadataKeySchema = new KeySchemaElement(Constants.REPLICATION_GROUP_UUID, KeyType.HASH);
        ProvisionedThroughput metadataProvisionedThroughput = new ProvisionedThroughput(DEFAULT_METADATA_THROUGHPUT, DEFAULT_METADATA_THROUGHPUT);

        // check if the metadata table exists, create it if not
        try {
            DescribeTableResult result = dynamodbClient.describeTable(new DescribeTableRequest(params.getMetadataTableName()));

            // verify existing table matches the replication group attribute definitions and key schema
            List<AttributeDefinition> resultAttrDefn = result.getTable().getAttributeDefinitions();
            List<KeySchemaElement> resultKeySchema = result.getTable().getKeySchema();
            coordinatorAssert(resultAttrDefn.size() == 1, MSG_ATTR_DEFN_MATCH);
            coordinatorAssert(resultAttrDefn.get(0).equals(metadataAttributeDefinition), MSG_ATTR_DEFN_MATCH);
            coordinatorAssert(resultKeySchema.size() == 1, MSG_KEY_SCHEMA_MATCH);
            coordinatorAssert(resultKeySchema.get(0).equals(metadataKeySchema), MSG_KEY_SCHEMA_MATCH);

            // turn on Streams if necessary
            StreamSpecification streamSpec = result.getTable().getStreamSpecification();
            if (streamSpec == null) {
                coordinatorFail(MSG_STREAMS_NOT_SUPPORTED);
            } else if (!replicationStreamSpec.equals(streamSpec)) {
                dynamodbClient
                    .updateTable(new UpdateTableRequest().withTableName(params.getMetadataTableName()).withStreamSpecification(replicationStreamSpec));
            }
        } catch (ResourceNotFoundException e) {
            // Create table
            dynamodbClient.createTable(new CreateTableRequest().withAttributeDefinitions(metadataAttributeDefinition).withKeySchema(metadataKeySchema)
                .withProvisionedThroughput(metadataProvisionedThroughput).withTableName(params.getMetadataTableName())
                .withStreamSpecification(replicationStreamSpec));
        }

        // wait for metadata table to become ACTIVE
        DynamoDBReplicationUtilities.waitForTableActive(dynamodbClient, params.getMetadataTableName(), WAITING_TIME_OUT);
    }

    /**
     * Get the taskname from command line arguments if it exists, if not, autogenerate one to be used by KCL in the checkpoint table and to publish CloudWatch
     * metrics
     *
     * @param params
     *            command line arguments, use to check if there exists an user-specificed task name
     * @return the generated task name
     */
    private static String getTaskName(CommandLineArgs params) {
        String taskName;
        if (params.getTaskName() != null) {
            taskName = params.getTaskName();
        } else {
            taskName = DYNAMODB_REPLICATION_PREFIX + TASKNAME_DELIMITER + DynamoDBReplicationUtilities.getRegionFromEndpoint(params.getMetadataTableEndpoint())
                + TASKNAME_DELIMITER + params.getMetadataTableName();
        }
        return taskName;
    }

    /**
     * Assert function for the coordinator, logs errors if assert evaluates to false
     *
     * @param condition
     *            Condition to assert on
     * @param errorMessage
     *            Error message to log if the assertion evaluates to false
     */
    public static void coordinatorAssert(boolean condition, String errorMessage) {
        if (!condition) {
            throw new IllegalStateException(errorMessage);
        }
    }

    /**
     * Fail function for the coordinator, logs an error message
     *
     * @param errorMessage
     */
    public static void coordinatorFail(String errorMessage) {
        coordinatorAssert(false /* a failed condition */, errorMessage);
    }

}
