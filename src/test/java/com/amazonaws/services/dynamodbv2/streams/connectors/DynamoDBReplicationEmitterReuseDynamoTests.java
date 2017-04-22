package com.amazonaws.services.dynamodbv2.streams.connectors;


import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Future.class, DynamoDBReplicationEmitter.class})
@PowerMockIgnore({"javax.management.*", "org.apache.log4j.*"})
public class DynamoDBReplicationEmitterReuseDynamoTests extends DynamoDBReplicationEmitterTestsBase {
    /**
     * Maximum number of threads for the Async clients.
     */
    public static final int MAX_THREADS = 10;

    @Override
    protected IEmitter<Record> createEmitterInstance() {
        Properties properties = new Properties();
        properties.setProperty("APP_NAME", "TEST");
        properties.setProperty("DYNAMODB_ENDPOINT", "ENDPOINT");
        properties.setProperty("REGION_NAME", "REGION");
        properties.setProperty("DYNAMODB_DATA_TABLE_NAME", "TABLE");
        AWSStaticCredentialsProvider credentialProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials("Access", "Secret"));
        return new DynamoDBReplicationEmitter(new DynamoDBStreamsConnectorConfiguration(properties, credentialProvider),
            new AmazonDynamoDBAsyncClient(credentialProvider, new ClientConfiguration().withMaxErrorRetry(0), Executors.newFixedThreadPool(MAX_THREADS)), null);
    }
}
