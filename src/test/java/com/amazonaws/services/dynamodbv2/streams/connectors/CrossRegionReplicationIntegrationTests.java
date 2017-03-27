package com.amazonaws.services.dynamodbv2.streams.connectors;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.streams.connectors.CommandLineInterface;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by amcp on 2017/04/15.
 */
public class CrossRegionReplicationIntegrationTests {

    public static final String INVENTORY_TABLE_IAD = "inventoryIad";
    public static final String INVENTORY_TABLE_PDX = "inventoryPdx";
    public static final String SKU_CODE = "skuCode";
    public static final String STORE = "store";
    public static final String DYNAMODB_LOCAL_ENDPOINT = "http://0.0.0.0:4567";
    public static final String ASIN_1 = "ASIN1";
    public static final String SEA = "SEA";
    public static final String CRR_INTEGRATION_TEST = "crrIntegrationTest";

    private CreateTableRequest createTableRequest(String tableName) {
        return new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(new KeySchemaElement(SKU_CODE, KeyType.HASH),
                        new KeySchemaElement(STORE, KeyType.RANGE))
                .withAttributeDefinitions(
                        new AttributeDefinition(SKU_CODE, ScalarAttributeType.S),
                        new AttributeDefinition(STORE, ScalarAttributeType.S))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
    }

    private AmazonDynamoDB dynamoDbIad;
    private AmazonDynamoDB dynamoDbPdx;
    private Table iadTable;
    private Table pdxTable;

    @Before
    public void setup() {
        dynamoDbIad = buildDynamoDbClient(Regions.US_EAST_1);
        iadTable = new Table(dynamoDbIad, INVENTORY_TABLE_IAD);
        dynamoDbPdx = buildDynamoDbClient(Regions.US_WEST_2);
        pdxTable = new Table(dynamoDbIad, INVENTORY_TABLE_PDX);
        try {
            dynamoDbIad.deleteTable(INVENTORY_TABLE_IAD);
            for (String tableName : dynamoDbIad.listTables().getTableNames()) {
                if (tableName.contains(CRR_INTEGRATION_TEST)) {
                    dynamoDbIad.deleteTable(tableName); //KCL lease table used in test
                    break;
                }
            }
            dynamoDbPdx.deleteTable(INVENTORY_TABLE_PDX);
        } catch(ResourceNotFoundException e) {
            //do nothing
        }
    }

    @After
    public void tearDown() {
        dynamoDbIad.shutdown();
        dynamoDbPdx.shutdown();
    }

    private AmazonDynamoDB buildDynamoDbClient(Regions region) {
        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(DYNAMODB_LOCAL_ENDPOINT,
                                region.getName()))
                .build();
    }

    @Test
    public void testBothTablesAddStreamAfterCreation() throws InterruptedException {
        //create table one in one region
        final CreateTableRequest iadCreateTableRequest = createTableRequest(INVENTORY_TABLE_IAD);
        dynamoDbIad.createTable(iadCreateTableRequest
                .withStreamSpecification(new StreamSpecification()
                        .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                        .withStreamEnabled(true)));
        //create table two in another region
        final CreateTableRequest pdxCreateTableRequest = createTableRequest(INVENTORY_TABLE_PDX);
        dynamoDbPdx.createTable(pdxCreateTableRequest);

        //create and start the command line client and worker
        final List<String> commandLineArgs = Lists.newArrayList(
                "--sourceEndpoint",
                DYNAMODB_LOCAL_ENDPOINT,
                // override the signing region as DynamoDB Local uses it to create different table namespaces
                "--sourceSigningRegion",
                Regions.US_EAST_1.getName(),
                "--sourceTable",
                INVENTORY_TABLE_IAD,
                "--destinationEndpoint",
                DYNAMODB_LOCAL_ENDPOINT,
                // override the signing region as DynamoDB Local uses it to create different table namespaces
                "--destinationSigningRegion",
                Regions.US_WEST_2.getName(),
                "--destinationTable",
                INVENTORY_TABLE_PDX,
                "--kclEndpoint",
                DYNAMODB_LOCAL_ENDPOINT,
                // override the signing region as DynamoDB Local uses it to create different table namespaces
                "--kclSigningRegion",
                Regions.US_EAST_1.getName(),
                "--taskName",
                CRR_INTEGRATION_TEST,
                // 100ms - override to reduce the time to sleep
                "--parentShardPollIntervalMillis",
                "100",
                "--dontPublishCloudwatch");
        final String[] args = commandLineArgs.toArray(new String[commandLineArgs.size()]);
        final Worker worker = CommandLineInterface.mainUnsafe(args).get();
        final Thread workerThread = new Thread(worker, "KCLWorker");
        workerThread.start();

        //perform the updates on the source table
        final Item asin1sea = new Item().withString(SKU_CODE, ASIN_1).withString(STORE, SEA);
        iadTable.putItem(asin1sea);
        final Item asin1seaRead = iadTable.getItem(SKU_CODE, ASIN_1, STORE, SEA);
        assertEquals(asin1sea, asin1seaRead);

        //verify the updates on the destination table
        //wait for the worker to start and the update to propagate
        Thread.sleep(10000);
        final List<Item> pdxItems = new ArrayList<>();
        for(Item item : pdxTable.scan()) {
            pdxItems.add(item);
        }
        assertEquals(1, pdxItems.size());
        final Item copied = Iterables.getOnlyElement(pdxItems);
        assertEquals(asin1sea, copied);

        //close the worker
        worker.shutdown(); //this leaks threads, I wonder
    }
}
