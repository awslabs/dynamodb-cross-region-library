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

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.*;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
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
    public static final String US_EAST_1 = "us-east-1";

    /**
     * Command line main method entry point
     *
     * @param args
     *            command line arguments
     */
    public static void main(String[] args) {
        try {
            final Optional<Worker> workerOption = mainUnsafe(args);
            if (!workerOption.isPresent()) {
                return;
            }
            System.out.println("Starting replication now, check logs for more details.");
            workerOption.get().run();
        } catch (ParameterException e) {
            LOGGER.error(e);
            JCommander.getConsole().println(e.toString());
            System.exit(StatusCodes.EINVAL);
        } catch (Exception e) {
            LOGGER.fatal(e);
            JCommander.getConsole().println(e.toString());
            System.exit(StatusCodes.EINVAL);
        }
    }

    static Optional<Worker> mainUnsafe(String[] args) {
        // Initialize command line arguments and JCommander parser
        CommandLineArgs params = new CommandLineArgs();
        JCommander cmd = new JCommander(params);

        // parse given arguments
        cmd.parse(args);

        // show usage information if help flag exists
        if (params.getHelp()) {
            cmd.usage();
            return Optional.absent();
        }

        final CommandLineInterface cli = new CommandLineInterface(params);
        // create worker
        return Optional.of(cli.createWorker());
    }

    private enum SigningRegionTransformer implements Function<Region, String> {
        INSTANCE;
        @Override
        public String apply(Region input) {
            return input.getName();
        }
    }

    private final Optional<Region> sourceRegionFromEndpoint;
    private final String sourceDynamodbEndpoint;
    private final String streamsEndpoint;
    private final String sourceTable;
    private final Optional<Region> kclRegionFromEndpoint;
    private final String kclDynamodbEndpoint;
    private final Optional<Region> destinationRegionFromEndpoint;
    private final String destinationDynamodbEndpoint;
    private final Optional<Integer> getRecordsLimit;
    private final boolean isPublishCloudWatch;
    private final String taskName;
    private final String destinationTable;
    private final Optional<Long> parentShardPollIntervalMillis;

    private CommandLineInterface(CommandLineArgs params) throws ParameterException {

        //set the source dynamodb endpoint
        sourceDynamodbEndpoint = params.getSourceEndpoint();
        // extract streams endpoint, source and destination regions
        sourceRegionFromEndpoint = Optional.fromNullable(DynamoDBConnectorUtilities.getRegionFromEndpoint(sourceDynamodbEndpoint));
        //if the source signing region is obtainable from the source endpoint
        streamsEndpoint = !sourceRegionFromEndpoint.isPresent() ? params.getSourceEndpoint() : // probably DynamoDB local
                DynamoDBConnectorUtilities.getStreamsEndpoint(params.getSourceEndpoint());
        //get source table name
        sourceTable = params.getSourceTable();
        // get kcl endpoint and region or null for region if cannot parse region from endpoint
        kclDynamodbEndpoint = params.getKclEndpoint();
        kclRegionFromEndpoint = Optional.fromNullable(DynamoDBConnectorUtilities.getRegionFromEndpoint(kclDynamodbEndpoint));
        // get kcl endpoint and region or null for region if cannot parse region from endpoint
        destinationDynamodbEndpoint = params.getDestinationEndpoint();
        destinationRegionFromEndpoint = Optional.fromNullable(DynamoDBConnectorUtilities.getRegionFromEndpoint(destinationDynamodbEndpoint));
        destinationTable = params.getDestinationTable();
        getRecordsLimit = Optional.fromNullable(params.getBatchSize());
        isPublishCloudWatch = !params.isDontPublishCloudwatch();
        taskName = params.getTaskName();
        parentShardPollIntervalMillis = Optional.fromNullable(params.getParentShardPollIntervalMillis());
    }

    public Worker createWorker() {
        final String destinationRegionString =
                destinationRegionFromEndpoint.transform(SigningRegionTransformer.INSTANCE).or(US_EAST_1);

        // use default credential provider chain to locate appropriate credentials
        final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

        // initialize DynamoDB client and set the endpoint properly
        final String sourceSigningRegion = sourceRegionFromEndpoint.transform(SigningRegionTransformer.INSTANCE).or(US_EAST_1);
        final AmazonDynamoDB dynamodbClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(sourceDynamodbEndpoint, sourceSigningRegion))
                .build();

        // initialize Streams client
        final AmazonDynamoDBStreams streamsClient = AmazonDynamoDBStreamsClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(streamsEndpoint, sourceSigningRegion /*signingRegion*/))
                .build();

        // obtain the Stream ID associated with the source table
        final String streamArn = dynamodbClient.describeTable(sourceTable).getTable().getLatestStreamArn();
        Preconditions.checkArgument(streamArn != null, DynamoDBConnectorConstants.MSG_NO_STREAMS_FOUND);
        Preconditions.checkArgument(DynamoDBConnectorUtilities.isStreamsEnabled(streamsClient, streamArn, DynamoDBConnectorConstants.NEW_AND_OLD),
                DynamoDBConnectorConstants.STREAM_NOT_READY);

        // initialize DynamoDB client for KCL. Use the local region for lowest latency access
        final String kclRegionString = kclRegionFromEndpoint.transform(SigningRegionTransformer.INSTANCE).or(US_EAST_1);
        final AmazonDynamoDB kclDynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(kclDynamodbEndpoint, kclRegionString))
                .build();

        // initialize DynamoDB Streams Adapter client and set the Streams endpoint properly
        final ClientConfiguration streamsClientConfig = new ClientConfiguration().withGzip(false);
        final AmazonDynamoDBStreamsAdapterClient streamsAdapterClient = new AmazonDynamoDBStreamsAdapterClient(
                credentialsProvider, streamsClientConfig);
        streamsAdapterClient.setEndpoint(streamsEndpoint);

        // initialize CloudWatch client and set the region to emit metrics to
        final AmazonCloudWatch kclCloudWatchClient;
        if (isPublishCloudWatch) {
            kclCloudWatchClient = AmazonCloudWatchClientBuilder.standard()
                    .withCredentials(credentialsProvider)
                    .withRegion(kclRegionString).build();
        } else {
            kclCloudWatchClient = new NoopCloudWatch();
        }

        // try to get taskname from command line arguments, auto generate one if needed
        final String actualTaskName = DynamoDBConnectorUtilities.getTaskName(sourceSigningRegion, destinationRegionString, taskName, sourceTable, destinationTable);

        // set the appropriate Connector properties for the destination
        // KCL configuration
        final Properties properties = new Properties();
        properties.put(DynamoDBStreamsConnectorConfiguration.PROP_APP_NAME, actualTaskName);
        properties.put(DynamoDBStreamsConnectorConfiguration.PROP_DYNAMODB_ENDPOINT, destinationDynamodbEndpoint);
        properties.put(DynamoDBStreamsConnectorConfiguration.PROP_DYNAMODB_DATA_TABLE_NAME, destinationTable);
        properties.put(DynamoDBStreamsConnectorConfiguration.PROP_REGION_NAME, destinationRegionString);

        // create the record processor factory based on given pipeline and connector configurations
        // use the master to replicas pipeline
        final KinesisConnectorRecordProcessorFactory<Record, Record> factory = new KinesisConnectorRecordProcessorFactory<>(
                new DynamoDBMasterToReplicasPipeline(), new DynamoDBStreamsConnectorConfiguration(properties, credentialsProvider));

        // create the KCL configuration with default values
        final KinesisClientLibConfiguration kclConfig = new KinesisClientLibConfiguration(actualTaskName,
                streamArn,
                credentialsProvider,
                DynamoDBConnectorConstants.WORKER_LABEL + actualTaskName + UUID.randomUUID().toString())
                // worker will use checkpoint table if available, otherwise it is safer
                // to start at beginning of the stream
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                // we want the maximum batch size to avoid network transfer latency overhead
                .withMaxRecords(getRecordsLimit.or(DynamoDBConnectorConstants.STREAMS_RECORDS_LIMIT))
                // wait a reasonable amount of time - default 0.5 seconds
                .withIdleTimeBetweenReadsInMillis(DynamoDBConnectorConstants.IDLE_TIME_BETWEEN_READS)
                // Remove calls to GetShardIterator
                .withValidateSequenceNumberBeforeCheckpointing(false)
                // make parent shard poll interval tunable to decrease time to run integration test
                .withParentShardPollIntervalMillis(parentShardPollIntervalMillis.or(DynamoDBConnectorConstants.DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS))
                // avoid losing leases too often - default 60 seconds
                .withFailoverTimeMillis(DynamoDBConnectorConstants.KCL_FAILOVER_TIME);

        // create the KCL worker for this connector
        return new Worker(factory, kclConfig, streamsAdapterClient, kclDynamoDBClient, kclCloudWatchClient);
    }
}
