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

import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
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

            // get current region, if no result set to default us-east-1 region
            Region curRegion = DynamoDBConnectorUtilities.getCurRegion();

            // extract streams endpoint, source and destination regions
            streamsEndpoint = DynamoDBConnectorUtilities.getStreamsEndpoint(params.getSourceEndpoint());
            sourceRegion = DynamoDBConnectorUtilities.getRegionFromEndpoint(params.getSourceEndpoint());
            destinationRegion = DynamoDBConnectorUtilities.getRegionFromEndpoint(params.getDestinationEndpoint());
            int getRecordsLimit = null == params.getBatchSize() ? DynamoDBConnectorConstants.STREAMS_RECORDS_LIMIT
                : params.getBatchSize();

            // use default credential provider chain to locate appropriate credentials
            AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

            // initialize DynamoDB client and set the endpoint properly
            AmazonDynamoDBClient dynamodbClient = new AmazonDynamoDBClient(credentialsProvider);
            dynamodbClient.setEndpoint(params.getSourceEndpoint());
            
            // initialize Streams client
            AmazonDynamoDBStreams streamsClient = new AmazonDynamoDBStreamsClient(credentialsProvider);
            streamsClient.setEndpoint(streamsEndpoint);

            // obtain the Stream ID associated with the source table
            String streamArn = dynamodbClient.describeTable(params.getSourceTable()).getTable().getLatestStreamArn();
            if (streamArn == null) {
                throw new IllegalArgumentException(DynamoDBConnectorConstants.MSG_NO_STREAMS_FOUND);
            } else if (!DynamoDBConnectorUtilities.isStreamsEnabled(streamsClient, streamArn, DynamoDBConnectorConstants.NEW_AND_OLD)) {
                throw new IllegalArgumentException(DynamoDBConnectorConstants.STREAM_NOT_READY);
            }

            // initialize DynamoDB client for KCL. Use the local region for lowest latency access
            AmazonDynamoDB kclDynamoDBClient = new AmazonDynamoDBClient(credentialsProvider);
            kclDynamoDBClient.setRegion(curRegion);

            // initialize DynamoDB Streams Adapter client and set the Streams endpoint properly
            ClientConfiguration streamsClientConfig = new ClientConfiguration().withGzip(false);
            AmazonDynamoDBStreamsAdapterClient streamsAdapterClient = new AmazonDynamoDBStreamsAdapterClient(
                credentialsProvider, streamsClientConfig);
            streamsAdapterClient.setEndpoint(streamsEndpoint);

            // initialize CloudWatch client and set the region to emit metrics to
            AmazonCloudWatch kclCloudWatchClient = new AmazonCloudWatchClient(credentialsProvider);
            kclCloudWatchClient.setRegion(curRegion);

            // try to get taskname from command line arguments, auto generate one if needed
            taskName = DynamoDBConnectorUtilities.getTaskName(sourceRegion, destinationRegion, params);

            // use the master to replicas pipeline
            pipeline = new DynamoDBMasterToReplicasPipeline();

            // set the appropriate Connector properties for the destination
            properties.put(DynamoDBStreamsConnectorConfiguration.PROP_APP_NAME, taskName);
            properties.put(DynamoDBStreamsConnectorConfiguration.PROP_DYNAMODB_ENDPOINT,
                params.getDestinationEndpoint());
            properties.put(DynamoDBStreamsConnectorConfiguration.PROP_DYNAMODB_DATA_TABLE_NAME,
                params.getDestinationTable());
            properties.put(DynamoDBStreamsConnectorConfiguration.PROP_REGION_NAME, destinationRegion.getName());

            // create the record processor factory based on given pipeline and connector configurations
            KinesisConnectorRecordProcessorFactory<Record, Record> factory = new KinesisConnectorRecordProcessorFactory<>(
                pipeline, new DynamoDBStreamsConnectorConfiguration(properties, credentialsProvider));

            // create the KCL configuration with default values
            KinesisClientLibConfiguration kclConfig = new KinesisClientLibConfiguration(taskName, streamArn,
                credentialsProvider, DynamoDBConnectorConstants.WORKER_LABEL + taskName + UUID.randomUUID().toString())
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON) // worker will use checkpoint table
                                                                                   // if
                                                                                   // available, otherwise it is safer
                                                                                   // to start
                                                                                   // at beginning of the stream
                .withMaxRecords(getRecordsLimit) // we want the maximum batch size to
                                                 // avoid network transfer
                // latency overhead
                .withIdleTimeBetweenReadsInMillis(DynamoDBConnectorConstants.IDLE_TIME_BETWEEN_READS) // wait a
                                                                                                      // reasonable
                                                                                                      // amount of time
                .withValidateSequenceNumberBeforeCheckpointing(false) // Remove calls to GetShardIterator
                .withFailoverTimeMillis(DynamoDBConnectorConstants.KCL_FAILOVER_TIME); // avoid losing leases too often

            // create the KCL worker for this connector
            Worker worker = new Worker(factory, kclConfig, streamsAdapterClient, kclDynamoDBClient, kclCloudWatchClient);

            // start the worker
            System.out.println("Starting replication now, check logs for more details.");
            worker.run();
        } catch (ParameterException e) {
            LOGGER.error(e);
            JCommander.getConsole().println(e.toString());
            cmd.usage();
            System.exit(StatusCodes.EINVAL);
        } catch (Exception e) {
            LOGGER.fatal(e);
            JCommander.getConsole().println(e.toString());;
            System.exit(StatusCodes.EINVAL);
        }
    }
}
