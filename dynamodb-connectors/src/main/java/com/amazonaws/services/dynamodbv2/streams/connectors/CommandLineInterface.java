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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.connectors.KinesisClientLibraryPipelinedRecordProcessorFactory;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.util.AwsHostNameUtils;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

/**
 * A command line interface allowing the connector to be launched independently from command line
 */
public class CommandLineInterface {

    /**
     * Logger for the {@link CommandLineInterface} class.
     */
    private static final Logger LOGGER = Logger.getLogger(CommandLineInterface.class);

    /**
     * Command Line Messages
     */
    public static final String MSG_INVALID_PIPELINE = "Pipeline is not recognized.";
    public static final String MSG_NO_STREAMS_FOUND = "Source table does not have Streams enabled.";

    /**
     * Prefixes, suffixes and limits
     */
    public static final String TASKNAME_DELIMITER = "_";
    public static final String SERVICE_PREFIX = "DynamoDBCrossRegionReplication";
    public static final String STREAMS_PREFIX = "streams.";
    public static final String PROTOCOL_REGEX = "^(https?://)?(.+)";
    private static final int DYNAMODB_TABLENAME_LIMIT = 255;

    /**
     * KCL constants
     */
    public static final int IDLE_TIME_BETWEEN_READS = 1;
    public static final int STREAMS_RECORDS_LIMIT = 1000;
    public static final int KCL_RECORD_BUFFER_SIZE = 10 * STREAMS_RECORDS_LIMIT;
    public static final String WORKER_LABEL = "worker";

    /**
     * MD5 digest instance
     */
    private static final String HASH_ALGORITHM = "MD5";
    private static final String BYTE_ENCODING = "UTF-8";
    public static final MessageDigest MD5_DIGEST = getMessageDigestInstance(HASH_ALGORITHM);

    private static MessageDigest getMessageDigestInstance(String hashAlgorithm) {
        try {
            return MessageDigest.getInstance(hashAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("Specified hash algorithm does not exist: " + hashAlgorithm + e);
            return null;
        }
    }

    /**
     * Command line main method entry point
     *
     * @param args
     *            command line arguments
     */
    public static void main(String[] args) {
        // Streams source endpoint
        String streamsEndpoint;

        // Source and destination regions
        Region sourceRegion;
        Region destinationRegion;

        // Pipeline
        IKinesisConnectorPipeline<Record, Record> pipeline;

        // Taskname for checkpoint table and CloudWatch metrics
        String taskName;

        // KCL configuration
        Properties properties = new Properties();

        // Initialize command line arguments and JCommander parser
        CommandLineArgs params = new CommandLineArgs();
        JCommander cmd = new JCommander(params);

        try {
            // parse given arguments
            cmd.parse(args);

            // show usage information if help flag exists
            if (params.getHelp()) {
                cmd.usage();
                return;
            }

            // extract streams endpoint, source and destination regions
            streamsEndpoint = getStreamsEndpoint(params.getSourceEndpoint());
            sourceRegion = getRegionFromEndpoint(params.getSourceEndpoint());
            destinationRegion = getRegionFromEndpoint(params.getDestinationEndpoint());
            int getRecordsLimit = null == params.getBatchSize() ? STREAMS_RECORDS_LIMIT : params.getBatchSize();

            // use default credential provider chain to locate appropriate credentials
            AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

            // initialize DynamoDB client and set the endpoint properly
            AmazonDynamoDBClient dynamodbClient = new AmazonDynamoDBClient(credentialsProvider);
            dynamodbClient.setEndpoint(params.getSourceEndpoint());

            // obtain the Stream ID associated with the source table
            String streamArn = dynamodbClient.describeTable(params.getSourceTable()).getTable().getLatestStreamArn();
            if (streamArn == null) {
                throw new ParameterException(MSG_NO_STREAMS_FOUND);
            }

            // initialize DynamoDB client for KCL. Use the local region for lowest latency access
            AmazonDynamoDB kclDynamoDBClient = new AmazonDynamoDBClient(credentialsProvider);
            kclDynamoDBClient.setRegion(Regions.getCurrentRegion());

            // initialize DynamoDB Streams Adapter client and set the Streams endpoint properly
            ClientConfiguration streamsClientConfig = new ClientConfiguration().withGzip(false);
            AmazonDynamoDBStreamsAdapterClient streamsAdapterClient = new AmazonDynamoDBStreamsAdapterClient(credentialsProvider, streamsClientConfig);
            streamsAdapterClient.setEndpoint(streamsEndpoint);

            // initialize CloudWatch client and set the region to emit metrics to
            AmazonCloudWatch kclCloudWatchClient = new AmazonCloudWatchClient(credentialsProvider);
            kclCloudWatchClient.setRegion(Regions.getCurrentRegion());

            // try to get taskname from command line arguments, auto generate one if needed
            taskName = getTaskName(sourceRegion, destinationRegion, params);

            // use the master to replicas pipeline
            pipeline = new DynamoDBMasterToReplicasPipeline();

            // set the appropriate Connector properties for the destination
            properties.put(DynamoDBStreamsConnectorConfiguration.PROP_APP_NAME, taskName);
            properties.put(DynamoDBStreamsConnectorConfiguration.PROP_DYNAMODB_ENDPOINT, params.getDestinationEndpoint());
            properties.put(DynamoDBStreamsConnectorConfiguration.PROP_DYNAMODB_DATA_TABLE_NAME, params.getDestinationTable());
            properties.put(DynamoDBStreamsConnectorConfiguration.PROP_REGION_NAME, destinationRegion.getName());

            // create the record processor factory based on given pipeline and connector configurations
            KinesisConnectorRecordProcessorFactory<Record, Record> factory = new KinesisConnectorRecordProcessorFactory<>(pipeline,
                new DynamoDBStreamsConnectorConfiguration(properties, credentialsProvider));

            KinesisClientLibraryPipelinedRecordProcessorFactory pipelinedFactory = new KinesisClientLibraryPipelinedRecordProcessorFactory(factory,
                KCL_RECORD_BUFFER_SIZE);

            // create the KCL configuration with default values
            KinesisClientLibConfiguration kclConfig = new KinesisClientLibConfiguration(taskName, streamArn, credentialsProvider, WORKER_LABEL + taskName
                + UUID.randomUUID().toString()).withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON) // worker will use checkpoint table if
                                                                                                                  // available, otherwise it is safer to start
                                                                                                                  // at beginning of the stream TODO consistent
                                                                                                                  // scan checkpoint
                .withMaxRecords(getRecordsLimit) // we want the maximum batch size to
                                                 // avoid network transfer
                // latency overhead
                .withIdleTimeBetweenReadsInMillis(IDLE_TIME_BETWEEN_READS) // wait as little
                                                                           // as possible
                .withValidateSequenceNumberBeforeCheckpointing(false); // Remove calls to GetShardIterator

            // create the KCL worker for this connector
            Worker worker = new Worker(pipelinedFactory, kclConfig, streamsAdapterClient, kclDynamoDBClient, kclCloudWatchClient);

            // start the worker
            worker.run();
        } catch (ParameterException e) {
            LOGGER.error(e.getMessage());
            cmd.usage();
            System.exit(StatusCodes.EINVAL);
        } catch (Exception e) {
            LOGGER.fatal(e + " " + e.getMessage());
            System.exit(StatusCodes.EINVAL);
        }

    }

    /**
     * Get the taskname from command line arguments if it exists, if not, autogenerate one to be used by KCL in the checkpoint table and to publish CloudWatch
     * metrics
     *
     * @param sourceRegion
     *            region of the source table
     * @param destinationRegion
     *            region of the destination table
     * @param params
     *            command line arguments, use to check if there exists an user-specified task name
     * @return the generated task name
     * @throws UnsupportedEncodingException
     */
    private static String getTaskName(Region sourceRegion, Region destinationRegion, CommandLineArgs params) throws UnsupportedEncodingException {
        String taskName;
        if (params.getTaskName() != null) {
            taskName = SERVICE_PREFIX + params.getTaskName();
            if (taskName.length() > DYNAMODB_TABLENAME_LIMIT) {
                throw new ParameterException("Provided taskname is too long!");
            }
        } else {
            taskName = sourceRegion + params.getSourceTable() + destinationRegion + params.getDestinationTable();
            // hash stack name using MD5
            if (MD5_DIGEST == null) {
                // see if we can generate a taskname without hashing
                if (taskName.length() > DYNAMODB_TABLENAME_LIMIT) { // must hash the taskname
                    throw new IllegalArgumentException("Generated taskname is too long and cannot be hashed due to improperly initialized MD5 digest object!");
                }
            } else {
                taskName = SERVICE_PREFIX + new String(Hex.encodeHex(MD5_DIGEST.digest(taskName.getBytes(BYTE_ENCODING))));
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
    private static Region getRegionFromEndpoint(String endpoint) {
        return Region.getRegion(Regions.fromName(AwsHostNameUtils.parseRegionName(endpoint, null)));
    }

    /**
     * Convert a given DynamoDB endpoint into its corresponding Streams endpoint
     *
     * @param endpoint
     *            given endpoint URL
     * @return the extracted Streams endpoint corresponding to the given DynamoDB endpoint
     */
    private static String getStreamsEndpoint(String endpoint) {
        String regex = PROTOCOL_REGEX;
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(endpoint);
        String ret;
        if (matcher.matches()) {
            ret = ((matcher.group(1) == null) ? "" : matcher.group(1)) + STREAMS_PREFIX + matcher.group(2);
        } else {
            ret = STREAMS_PREFIX + endpoint;
        }
        return ret;
    }

}
