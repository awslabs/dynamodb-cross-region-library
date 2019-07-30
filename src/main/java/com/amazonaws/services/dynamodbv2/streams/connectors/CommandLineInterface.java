/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streams.connectors;

import java.util.Properties;
import java.util.UUID;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.log4j.Log4j;

/**
 * A command line interface allowing the connector to be launched independently from command line
 */
@Log4j
public class CommandLineInterface {

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
            log.error(e);
            JCommander.getConsole().println(e.toString());
            System.exit(StatusCodes.EINVAL);
        } catch (Exception e) {
            log.fatal(e);
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
        if (params.isHelp()) {
            cmd.usage();
            return Optional.absent();
        }

        final CommandLineInterface cli = new CommandLineInterface(params);
        // create worker
        return Optional.of(cli.createWorker());
    }

    @Getter(AccessLevel.PACKAGE)
    private final Region sourceRegion;
    private final Optional<String> sourceDynamodbEndpoint;
    private final Optional<String> sourceDynamodbStreamsEndpoint;
    private final String sourceTable;
    private final Optional<Region> kclRegion;
    private final Optional<String> kclDynamodbEndpoint;
    private final Region destinationRegion;
    private final Optional<String> destinationDynamodbEndpoint;
    private final Optional<Integer> getRecordsLimit;
    private final boolean isPublishCloudWatch;
    private final String taskName;
    private final String destinationTable;
    private final Optional<Long> parentShardPollIntervalMillis;

    @VisibleForTesting
    CommandLineInterface(CommandLineArgs params) throws ParameterException {

        // extract streams endpoint, source and destination regions
        sourceRegion = RegionUtils.getRegion(params.getSourceSigningRegion());

        // set the source dynamodb endpoint
        sourceDynamodbEndpoint = Optional.fromNullable(params.getSourceEndpoint());
        sourceDynamodbStreamsEndpoint = Optional.fromNullable(params.getSourceEndpoint());

        // get source table name
        sourceTable = params.getSourceTable();

        // get kcl endpoint and region or null for region if cannot parse region from endpoint
        kclRegion = Optional.fromNullable(RegionUtils.getRegion(params.getKclSigningRegion()));
        kclDynamodbEndpoint = Optional.fromNullable(params.getKclEndpoint());

        // get destination endpoint and region or null for region if cannot parse region from endpoint
        destinationRegion = RegionUtils.getRegion(params.getDestinationSigningRegion());
        destinationDynamodbEndpoint = Optional.fromNullable(params.getDestinationEndpoint());
        destinationTable = params.getDestinationTable();

        // other crr parameters
        getRecordsLimit = Optional.fromNullable(params.getBatchSize());
        isPublishCloudWatch = !params.isDontPublishCloudwatch();
        taskName = params.getTaskName();
        parentShardPollIntervalMillis = Optional.fromNullable(params.getParentShardPollIntervalMillis());
    }

    @VisibleForTesting
    static AwsClientBuilder.EndpointConfiguration createEndpointConfiguration(Region region, Optional<String> endpoint, String endpointPrefix) {
        return new AwsClientBuilder.EndpointConfiguration(endpoint.or("https://" + region.getServiceEndpoint(endpointPrefix)), region.getName());
    }

    public Worker createWorker() {

        // use default credential provider chain to locate appropriate credentials
        final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

        // initialize DynamoDB client and set the endpoint properly for source table / region
        final AmazonDynamoDB dynamodbClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withEndpointConfiguration(createEndpointConfiguration(sourceRegion, sourceDynamodbEndpoint, AmazonDynamoDB.ENDPOINT_PREFIX))
                .build();

        // initialize Streams client
        final AwsClientBuilder.EndpointConfiguration streamsEndpointConfiguration = createEndpointConfiguration(sourceRegion,
                sourceDynamodbStreamsEndpoint, AmazonDynamoDBStreams.ENDPOINT_PREFIX);
        final ClientConfiguration streamsClientConfig = new ClientConfiguration().withGzip(false);
        final AmazonDynamoDBStreams streamsClient = AmazonDynamoDBStreamsClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withEndpointConfiguration(streamsEndpointConfiguration)
                .withClientConfiguration(streamsClientConfig)
                .build();

        // obtain the Stream ID associated with the source table
        final String streamArn = dynamodbClient.describeTable(sourceTable).getTable().getLatestStreamArn();
        final boolean streamEnabled = DynamoDBConnectorUtilities.isStreamsEnabled(streamsClient, streamArn, DynamoDBConnectorConstants.NEW_AND_OLD);
        Preconditions.checkArgument(streamArn != null, DynamoDBConnectorConstants.MSG_NO_STREAMS_FOUND);
        Preconditions.checkState(streamEnabled, DynamoDBConnectorConstants.STREAM_NOT_READY);

        // initialize DynamoDB client for KCL
        final AmazonDynamoDB kclDynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withEndpointConfiguration(createKclDynamoDbEndpointConfiguration())
                .build();

        // initialize DynamoDB Streams Adapter client and set the Streams endpoint properly
        final AmazonDynamoDBStreamsAdapterClient streamsAdapterClient = new AmazonDynamoDBStreamsAdapterClient(streamsClient);

        // initialize CloudWatch client and set the region to emit metrics to
        final AmazonCloudWatch kclCloudWatchClient;
        if (isPublishCloudWatch) {
            kclCloudWatchClient = AmazonCloudWatchClientBuilder.standard()
                    .withCredentials(credentialsProvider)
                    .withRegion(kclRegion.or(sourceRegion).getName()).build();
        } else {
            kclCloudWatchClient = new NoopCloudWatch();
        }

        // try to get taskname from command line arguments, auto generate one if needed
        final AwsClientBuilder.EndpointConfiguration destinationEndpointConfiguration = createEndpointConfiguration(destinationRegion,
                destinationDynamodbEndpoint, AmazonDynamoDB.ENDPOINT_PREFIX);
        final String actualTaskName = DynamoDBConnectorUtilities.getTaskName(sourceRegion, destinationRegion, taskName, sourceTable, destinationTable);

        // set the appropriate Connector properties for the destination KCL configuration
        final Properties properties = new Properties();
        properties.put(DynamoDBStreamsConnectorConfiguration.PROP_APP_NAME, actualTaskName);
        properties.put(DynamoDBStreamsConnectorConfiguration.PROP_DYNAMODB_ENDPOINT, destinationEndpointConfiguration.getServiceEndpoint());
        properties.put(DynamoDBStreamsConnectorConfiguration.PROP_DYNAMODB_DATA_TABLE_NAME, destinationTable);
        properties.put(DynamoDBStreamsConnectorConfiguration.PROP_REGION_NAME, destinationRegion.getName());

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

    @VisibleForTesting
    EndpointConfiguration createKclDynamoDbEndpointConfiguration() {
        return createEndpointConfiguration(kclRegion.or(sourceRegion),
                kclRegion.isPresent() ? kclDynamodbEndpoint : sourceDynamodbEndpoint, AmazonDynamoDB.ENDPOINT_PREFIX);
    }
}
