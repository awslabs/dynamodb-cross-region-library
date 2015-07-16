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

import static org.easymock.EasyMock.anyObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.powermock.api.easymock.PowerMock.expectLastCall;
import static org.powermock.api.easymock.PowerMock.mockStaticPartial;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.serverRunner.ServerRunner;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinitionDescription;
import com.amazonaws.services.dynamodbv2.model.Constants;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DynamoDBArn;
import com.amazonaws.services.dynamodbv2.model.DynamoDBConnectorDescription;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMember;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMemberStatus;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupStatus;
import com.amazonaws.services.dynamodbv2.model.DynamoDBTableCopyDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElementDescription;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDesc;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.streams.connectors.DynamoDBConnectorType;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

/**
 * Integration test for the replication coordinator that illustrates the life cycle for group creation, update, deletion etc.
 *
 * General life cycle of a replication group:
 * <ol>
 * <li>1. Upon group creation, all members should be in status CREATING
 * <li>2. Move one member at a time into WAITING status (at this point the physical table should be created), members without bootstrap task are prioritized.
 * <li>3. Prioritize moving WAITING members without bootstrap task to ACTIVE status (by launching DynamoDB Replication Connectors), then move WAITING members
 * with bootstrap task into BOOTSTRAPPING status.
 * <li>4. Once BOOTSTRAPPING status changes to BOOTSTRAP_COMPLETE, move member into ACTIVE status.
 * <li>5. If all members of a group are in ACTIVE status, change group status to ACTIVE.
 * </ol>
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DynamoDBReplicationUtilities.class)
@PowerMockIgnore({"javax.*"})
public class DynamoDBReplicationCoordinatorLocalIntegrationTest {
    private static final Logger LOG = Logger.getLogger(DynamoDBReplicationCoordinatorLocalIntegrationTest.class);
    private static final String REPLICA_TABLE_WITHOUT_BOOTSTRAP_NAME = "replicaTableWithoutBootstrap";
    private static final String REPLICA_TABLE_WITH_BOOTSTRAP_NAME = "replicaTableWithBootstrap";
    private static final int PROCESS_STREAMS_RECORD_WAIT_TIME = 15000;
    private static final String MASTER_TABLE_NAME = "masterTable";
    private static final String DEFAULT_REGION = "us-east-1";
    private static final String DYNAMODB_LOCAL_ENDPOINT = "http://localhost:8000";
    private static final String STREAMS_ENDPOINT = "http://localhost:8000";

    // FIXME Replace with the new in process interface when invoking DDB Local
    private static final String[] localArgs = {"-inMemory", "-port", "8000"};
    private static final ExecutorService local_thread = Executors.newSingleThreadExecutor();
    private static Worker kinesis_worker = null;
    private static final String MD_TABLE_NAME = "metadataTable";
    private static final String REPLICATION_TASK_NAME = "replicationCoordinatorTask";
    private static final String REPLICATION_GROUP_UUID = "TestReplicationGroupUUID";
    private static final AWSCredentials LOCAL_CREDS = new BasicAWSCredentials("Testing", "NoSecretsHere");
    private static final String ACCOUNT_NUM = "123456654321";
    private static final AWSCredentialsProvider CRED_PROV = new StaticCredentialsProvider(LOCAL_CREDS);
    private static final AccountMapToAwsAccess accounts = new AccountMapToAwsAccess();
    static {
        DynamoDBMetadataStorage.init(CRED_PROV, DYNAMODB_LOCAL_ENDPOINT, MD_TABLE_NAME);
        accounts.addAwsAccessAccount(ACCOUNT_NUM, new AwsAccess(CRED_PROV));
    }
    private static final DynamoDBMetadataStorage MD = DynamoDBMetadataStorage.getInstance();
    private static final StreamSpecification REPLICATION_STREAM_SPEC = new StreamSpecification().withStreamEnabled(true).withStreamViewType(
        StreamViewType.NEW_AND_OLD_IMAGES);

    @BeforeClass
    public static void startLocal() {
        local_thread.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.info("Starting DynamoDB Local");
                    ServerRunner.main(localArgs);
                } catch (Exception e) {
                    LOG.error(e);
                }
            }
        });
    }

    @AfterClass
    public static void stopLocal() {
        local_thread.shutdownNow();
        LOG.info("Stopping DynamoDB Local");
    }

    @Before
    public void setupMetadataTableAndWorker() throws Exception {
        // Set up worker thread for kinesis worker
        ExecutorService worker_thread = Executors.newSingleThreadExecutor();

        // Create the metadata table
        AmazonDynamoDB ddbClient = accounts.getAccessAccount(ACCOUNT_NUM).getDynamoDB(DYNAMODB_LOCAL_ENDPOINT);
        ddbClient.createTable(new CreateTableRequest()
            .withAttributeDefinitions(new AttributeDefinition(Constants.REPLICATION_GROUP_UUID, ScalarAttributeType.S))
            .withKeySchema(new KeySchemaElement(Constants.REPLICATION_GROUP_UUID, KeyType.HASH)).withProvisionedThroughput(new ProvisionedThroughput(1l, 1l))
            .withTableName(MD_TABLE_NAME).withStreamSpecification(REPLICATION_STREAM_SPEC));

        // setup a KCL worker on the metadata table
        AmazonDynamoDBStreamsAdapterClient streamsAdapterClient = new AmazonDynamoDBStreamsAdapterClient(CRED_PROV);
        streamsAdapterClient.setEndpoint(STREAMS_ENDPOINT);

        String streamArn = ddbClient.describeTable(MD_TABLE_NAME).getTable().getLatestStreamArn();

        assertNotNull(streamArn);

        DynamoDBReplicationRecordProcessorFactory factory = new DynamoDBReplicationRecordProcessorFactory(MD, accounts);
        KinesisClientLibConfiguration kclConfig = new KinesisClientLibConfiguration(REPLICATION_TASK_NAME, streamArn, CRED_PROV, "worker1")
            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        kinesis_worker = new Worker(factory, kclConfig, streamsAdapterClient, ddbClient, null /* do not need to publish metrics in a test */);

        // start the KCL worker
        worker_thread.submit(kinesis_worker);
        worker_thread.shutdown();

        // wait for the KCL checkpoint table to be active
        DynamoDBReplicationUtilities.waitForKCLToBecomeActive(ddbClient, REPLICATION_TASK_NAME, PROCESS_STREAMS_RECORD_WAIT_TIME);
    }

    @After
    public void dropMetadataTableAndWorker() throws Exception {
        // verify the connector launching process
        verifyAll();

        // shutdown kinesis worker task
        kinesis_worker.shutdown();

        // delete metadata tables
        AmazonDynamoDB ddbClient = accounts.getAccessAccount(ACCOUNT_NUM).getDynamoDB(DYNAMODB_LOCAL_ENDPOINT);
        ddbClient.deleteTable(MD_TABLE_NAME);
        ddbClient.deleteTable(REPLICATION_TASK_NAME);
    }

    /**
     * Create an empty DynamoDB replication group with the bare minimums
     *
     * @return Empty DynamoDB replication group object
     */
    private DynamoDBReplicationGroup createEmptyReplicationGroup() {
        Map<String, DynamoDBReplicationGroupMember> members = new HashMap<String, DynamoDBReplicationGroupMember>();
        DynamoDBReplicationGroup group = new DynamoDBReplicationGroup()
            .withAttributeDefinitions(Arrays.asList(new AttributeDefinitionDescription().withAttributeName("hashKey").withAttributeType("S")))
            .withKeySchema(Arrays.asList(new KeySchemaElementDescription().withAttributeName("hashKey").withKeyType("HASH")))
            .withReplicationGroupMembers(members).withReplicationGroupUUID(REPLICATION_GROUP_UUID)
            .withReplicationGroupStatus(DynamoDBReplicationGroupStatus.CREATING).withConnectorType(DynamoDBConnectorType.SINGLE_MASTER_TO_READ_REPLICA);
        return group;
    }

    /**
     * Create a DynamoDB replication group member that is a master
     *
     * @return Master DynamoDB replication group member
     */
    private DynamoDBReplicationGroupMember createMasterMember() {
        DynamoDBReplicationGroupMember member = new DynamoDBReplicationGroupMember().withEndpoint(DYNAMODB_LOCAL_ENDPOINT)
            .withARN(new DynamoDBArn().withAccountNumber(ACCOUNT_NUM).withRegion(DEFAULT_REGION).withTableName(MASTER_TABLE_NAME).getArnString())
            .withProvisionedThroughput(new ProvisionedThroughputDesc().withReadCapacityUnits(1l).withWriteCapacityUnits(1l))
            .withReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.CREATING).withStreamsEnabled(true);
        return member;
    }

    /**
     * Create a DynamoDB replication group member that is a replica
     *
     * @param hasBootstrappingTask
     *            indicates whether the replica contains a bootstrap task or not
     * @param tableName
     *            table name of the replica
     * @return Replica DynamoDB replication group member
     */
    private DynamoDBReplicationGroupMember createReplicaMember(boolean hasBootstrappingTask, String tableName) {
        DynamoDBReplicationGroupMember member = new DynamoDBReplicationGroupMember().withEndpoint(DYNAMODB_LOCAL_ENDPOINT)
            .withARN(new DynamoDBArn().withAccountNumber(ACCOUNT_NUM).withRegion(DEFAULT_REGION).withTableName(tableName).getArnString())
            .withProvisionedThroughput(new ProvisionedThroughputDesc().withReadCapacityUnits(1l).withWriteCapacityUnits(1l))
            .withReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.CREATING).withStreamsEnabled(false)
            .withConnectors(new ArrayList<DynamoDBConnectorDescription>()).withTableCopyTask(hasBootstrappingTask ? new DynamoDBTableCopyDescription() : null);
        return member;
    }

    /**
     * Write the given value into the metadata table, conditional on the expected value
     *
     * @param expectedValue
     *            expected value in the metadata table
     * @param newValue
     *            given value to overwrite the expected value in the metadata table
     * @param waitTime
     *            amount of time to wait after the write to the metadata table
     * @throws InterruptedException
     */
    private void writeToMetadataTableThenWait(DynamoDBReplicationGroup expectedValue, DynamoDBReplicationGroup newValue, int waitTime)
        throws InterruptedException {
        try {
            DynamoDBReplicationGroup result = MD.compareAndWriteReplicationGroup(expectedValue, newValue);
            assertEquals(result, newValue);
        } catch (IOException e) {
            fail(e.getMessage());
        }
        Thread.sleep(waitTime);
    }

    /**
     * Checks status and size of the given group, as well as status of individual group members
     *
     * @param group
     *            DynamoDB replication group to check
     * @param expectedStatus
     *            expected status of the group
     * @param expectedSize
     *            expected size of the group
     * @param activeMemberArns
     *            list of DynamoDB replication group member ARNs that should be in ACTIVE status
     * @param bootstrappingMemberArns
     *            list of DynamoDB replication group member ARNs that should be in BOOTSTRAPPING status
     */
    private void checkGroupStatusAndSize(DynamoDBReplicationGroup group, DynamoDBReplicationGroupStatus expectedStatus, int expectedSize,
        List<String> activeMemberArns, List<String> bootstrappingMemberArns) {
        assertEquals(expectedStatus, group.getReplicationGroupStatus());
        Map<String, DynamoDBReplicationGroupMember> members = group.getReplicationGroupMembers();
        assertEquals(expectedSize, members.size());
        if (activeMemberArns != null) {
            for (String activeArn : activeMemberArns) {
                assertEquals(DynamoDBReplicationGroupMemberStatus.ACTIVE, members.get(activeArn).getReplicationGroupMemberStatus());
            }
        }

        if (bootstrappingMemberArns != null) {
            for (String bootstrappingMemberArn : bootstrappingMemberArns) {
                assertEquals(DynamoDBReplicationGroupMemberStatus.BOOTSTRAPPING, members.get(bootstrappingMemberArn).getReplicationGroupMemberStatus());
            }
        }
    }

    @Test
    public void testCreateEmptyReplicationGroup() throws InterruptedException, IOException {
        // create an empty replication group
        DynamoDBReplicationGroup emptyGroup = createEmptyReplicationGroup();

        // add the empty replication group to the metadata table, then wait a specified amount of time
        writeToMetadataTableThenWait(null, emptyGroup, PROCESS_STREAMS_RECORD_WAIT_TIME);

        // read the current replication group out from metadata table
        DynamoDBReplicationGroup resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // current group status should be ACTIVE, since empty replication group should always be ACTIVE
        assertEquals(DynamoDBReplicationGroupStatus.ACTIVE, resultingGroup.getReplicationGroupStatus());
    }

    @Test
    public void testCreateReplicationGroupWithMaster() throws InterruptedException, IOException {
        // create replication group with one member who is a master (no bootstrapping needed)
        DynamoDBReplicationGroupMember member = createMasterMember();
        DynamoDBReplicationGroup group = createEmptyReplicationGroup();
        group.addReplicationGroupMember(member);

        // add the replication group to the metadata table, then wait a specified amount of time
        writeToMetadataTableThenWait(null, group, PROCESS_STREAMS_RECORD_WAIT_TIME);

        // check group size is 1, and that both group status and group member status is ACTIVE
        checkGroupStatusAndSize(MD.readReplicationGroup(REPLICATION_GROUP_UUID), DynamoDBReplicationGroupStatus.ACTIVE, 1,
            Arrays.asList(new String[] {member.getArn()}), null);
    }

    @Test
    public void testCreateReplicationGroupWithSingleReplica() throws InterruptedException, IOException {
        // create replication group with one member (bootstrapping needed)
        DynamoDBReplicationGroupMember masterMember = createMasterMember();
        DynamoDBReplicationGroupMember member = createReplicaMember(true, REPLICA_TABLE_WITH_BOOTSTRAP_NAME);
        DynamoDBReplicationGroup group = createEmptyReplicationGroup();
        group.addReplicationGroupMember(masterMember);
        group.addReplicationGroupMember(member);

        // write the replication group to the metadata table, then wait a specified amount of time
        writeToMetadataTableThenWait(null, group, 2 * PROCESS_STREAMS_RECORD_WAIT_TIME);

        // read current image of the replication group
        DynamoDBReplicationGroup resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // check current group size is 2, group status is CREATING, and master member is ACTIVE, and replica member is BOOTSTRAPPING
        checkGroupStatusAndSize(resultingGroup, DynamoDBReplicationGroupStatus.CREATING, 2, Arrays.asList(new String[] {masterMember.getArn()}),
            Arrays.asList(new String[] {member.getArn()}));

        // emulate bootstrapping complete
        DynamoDBReplicationGroup updatedGroup = new DynamoDBReplicationGroup(resultingGroup);
        updatedGroup.getReplicationGroupMembers().get(member.getArn()).setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_COMPLETE);

        // write the updated replication group to the metadata table, then wait a specified amount of time
        writeToMetadataTableThenWait(resultingGroup, updatedGroup, PROCESS_STREAMS_RECORD_WAIT_TIME);

        // check the current group size is 2, group status is ACTIVE, and both members of the group are also ACTIVE
        checkGroupStatusAndSize(MD.readReplicationGroup(REPLICATION_GROUP_UUID), DynamoDBReplicationGroupStatus.ACTIVE, 2,
            Arrays.asList(new String[] {masterMember.getArn(), member.getArn()}), null);
    }

    @Test
    public void testCreateReplicationGroupWithMultipleReplica() throws InterruptedException, IOException {
        // create replication group with multiple members (some need bootstrapping)
        DynamoDBReplicationGroupMember masterMember = createMasterMember();
        DynamoDBReplicationGroupMember memberWithBootstrap = createReplicaMember(true, REPLICA_TABLE_WITH_BOOTSTRAP_NAME);
        DynamoDBReplicationGroupMember memberWithoutBootstrap = createReplicaMember(false, REPLICA_TABLE_WITHOUT_BOOTSTRAP_NAME);
        DynamoDBReplicationGroup group = createEmptyReplicationGroup();
        group.addReplicationGroupMember(masterMember);
        group.addReplicationGroupMember(memberWithBootstrap);
        group.addReplicationGroupMember(memberWithoutBootstrap);

        // write the replication group to the metadata table, then wait a specified amount of time
        writeToMetadataTableThenWait(null, group, 3 * PROCESS_STREAMS_RECORD_WAIT_TIME);

        // read the current image of the replication group
        DynamoDBReplicationGroup resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // check the current group size is 3, group status is CREATING, the master member and the replica member without bootstrap task are both ACTIVE, and the
        // replica member with a bootstrap task is BOOTSTRAPPING
        checkGroupStatusAndSize(resultingGroup, DynamoDBReplicationGroupStatus.CREATING, 3,
            Arrays.asList(new String[] {masterMember.getArn(), memberWithoutBootstrap.getArn()}), Arrays.asList(new String[] {memberWithBootstrap.getArn()}));

        // emulate bootstrapping complete
        DynamoDBReplicationGroup updatedGroup = new DynamoDBReplicationGroup(resultingGroup);
        updatedGroup.getReplicationGroupMembers().get(memberWithBootstrap.getArn())
            .setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_COMPLETE);

        // write the updated replication group to the metadata table, then wait a specified amount of time
        writeToMetadataTableThenWait(resultingGroup, updatedGroup, PROCESS_STREAMS_RECORD_WAIT_TIME);

        // read the current image of the replication group
        resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // check the current group size is 3, group status is ACTIVE, and all three replication group members are also ACTIVE
        checkGroupStatusAndSize(resultingGroup, DynamoDBReplicationGroupStatus.ACTIVE, 3,
            Arrays.asList(new String[] {masterMember.getArn(), memberWithoutBootstrap.getArn(), memberWithBootstrap.getArn()}), null);
    }

    @Test
    public void testAddReplicationGroupMember() throws InterruptedException, IOException {
        // add new member with bootstrap task to an active group
        testCreateReplicationGroupWithMaster();
        DynamoDBReplicationGroup resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);
        DynamoDBReplicationGroup updatedGroup = new DynamoDBReplicationGroup(resultingGroup);
        DynamoDBReplicationGroupMember memberWithBootstrap = createReplicaMember(true, REPLICA_TABLE_WITH_BOOTSTRAP_NAME);
        updatedGroup.addReplicationGroupMember(memberWithBootstrap);
        updatedGroup.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.UPDATING);

        // write the updated replication group (with the newly added member) to the metadata table, then wait
        writeToMetadataTableThenWait(resultingGroup, updatedGroup, PROCESS_STREAMS_RECORD_WAIT_TIME);

        // read current image of the replication group
        resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // check current group size is 2, the group status is UPDATING, and the newly added replica group member is BOOTSTRAPPING
        checkGroupStatusAndSize(resultingGroup, DynamoDBReplicationGroupStatus.UPDATING, 2, null, Arrays.asList(new String[] {memberWithBootstrap.getArn()}));

        // add new member without bootstrap task to an existing group with bootstrapping member
        DynamoDBReplicationGroupMember memberWithoutBootstrap = createReplicaMember(false, REPLICA_TABLE_WITHOUT_BOOTSTRAP_NAME);
        updatedGroup = new DynamoDBReplicationGroup(resultingGroup);
        updatedGroup.addReplicationGroupMember(memberWithoutBootstrap);

        // write the updated replication group (with the replica member without a bootstrap task) to the metadata table, then wait
        writeToMetadataTableThenWait(resultingGroup, updatedGroup, PROCESS_STREAMS_RECORD_WAIT_TIME);

        // read current image of the replication group
        resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // check current group size is 3, the group status is UPDATING, and the newly added replica group member is ACTIVE (since no bootstrapping is required
        // for this member, it can transition into ACTIVE after connectors are launched)
        checkGroupStatusAndSize(resultingGroup, DynamoDBReplicationGroupStatus.UPDATING, 3, Arrays.asList(new String[] {memberWithoutBootstrap.getArn()}), null);
    }

    @Test
    public void testModifyReplicationGroupMember() throws Exception {
        // mock the launching of connectors
        mockStaticPartial(DynamoDBReplicationUtilities.class, "launchConnectorService");
        DynamoDBReplicationUtilities.launchConnectorService(anyObject(AccountMapToAwsAccess.class), anyObject(DynamoDBReplicationGroup.class),
            anyObject(DynamoDBReplicationGroupMember.class), anyObject(DynamoDBConnectorDescription.class));
        expectLastCall();
        replayAll();

        // create group with one master and one replica without bootstrap task
        DynamoDBReplicationGroupMember masterMember = createMasterMember();
        DynamoDBReplicationGroupMember member = createReplicaMember(false, REPLICA_TABLE_WITHOUT_BOOTSTRAP_NAME);
        DynamoDBReplicationGroup group = createEmptyReplicationGroup();
        group.addReplicationGroupMember(masterMember);
        group.addReplicationGroupMember(member);

        // write the replication group to the metadata table, then wait a specified amount of time
        writeToMetadataTableThenWait(null, group, 2 * PROCESS_STREAMS_RECORD_WAIT_TIME);

        // read current image of the replication group
        DynamoDBReplicationGroup resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // check the current group size is 2, group status is ACTIVE, and both members (master and replica) are ACTIVE
        checkGroupStatusAndSize(resultingGroup, DynamoDBReplicationGroupStatus.ACTIVE, 2, Arrays.asList(new String[] {masterMember.getArn(), member.getArn()}),
            null);

        // add a connector to the replica table (modification of the replication group member)
        DynamoDBConnectorDescription connector = new DynamoDBConnectorDescription().withSourceTableEndpoint(DYNAMODB_LOCAL_ENDPOINT).withSourceTableArn(
            new DynamoDBArn().withAccountNumber(ACCOUNT_NUM).withRegion(DEFAULT_REGION).withTableName(MASTER_TABLE_NAME).getArnString());
        List<DynamoDBConnectorDescription> connectors = new ArrayList<DynamoDBConnectorDescription>();
        connectors.add(connector);
        DynamoDBReplicationGroup updatedGroup = new DynamoDBReplicationGroup(resultingGroup);
        DynamoDBReplicationGroupMember updatedMember = updatedGroup.getReplicationGroupMembers().get(member.getArn());
        updatedMember.setConnectors(connectors);

        // set both the status of the replication group member and replication group to be UPDATING
        updatedMember.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.UPDATING);
        updatedGroup.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.UPDATING);

        // write the updated replication group to the metadata table, then wait
        writeToMetadataTableThenWait(resultingGroup, updatedGroup, PROCESS_STREAMS_RECORD_WAIT_TIME);

        // verify the launching of connectors
        verifyAll();

        // read current image of the replication group
        resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // check current group size is 2, group status is ACTIVE, and all members are ACTIVE
        checkGroupStatusAndSize(resultingGroup, DynamoDBReplicationGroupStatus.ACTIVE, 2, Arrays.asList(new String[] {masterMember.getArn(), member.getArn()}),
            null);
    }

    @Test
    public void testRemoveReplicationGroupMember() throws Exception {
        // mock the checking the existence of bootstrap service
        mockStaticPartial(DynamoDBReplicationUtilities.class, "checkIfTableCopyServiceExists");
        DynamoDBReplicationUtilities.checkIfTableCopyServiceExists(anyObject(AccountMapToAwsAccess.class), anyObject(DynamoDBReplicationGroupMember.class),
            anyObject(DynamoDBTableCopyDescription.class));
        expectLastCall().andReturn(true /* pretend table copy service exists */);
        replayAll();

        // create a replication group with a master and a replica
        testCreateReplicationGroupWithSingleReplica();

        // get the current replication group
        DynamoDBReplicationGroup curGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // copy the current group and update the status of replica member to DELETING, update group status to UPDATING
        DynamoDBReplicationGroup updatedGroup = new DynamoDBReplicationGroup(curGroup);
        updatedGroup.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.UPDATING);

        DynamoDBReplicationGroupMember memberWithBootstrap = createReplicaMember(true, REPLICA_TABLE_WITH_BOOTSTRAP_NAME);
        DynamoDBReplicationGroupMember memberToDelete = updatedGroup.getReplicationGroupMembers().get(memberWithBootstrap.getArn());
        memberToDelete.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.DELETING);

        // write the updated group to the metadata table, then wait
        writeToMetadataTableThenWait(curGroup, updatedGroup, PROCESS_STREAMS_RECORD_WAIT_TIME);

        // get the current replication group
        curGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // emulate bootstrapping cancelled
        updatedGroup = new DynamoDBReplicationGroup(curGroup);
        updatedGroup.getReplicationGroupMembers().get(memberWithBootstrap.getArn())
            .setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_CANCELLED);

        // write the updated group to the metadata table, then wait
        writeToMetadataTableThenWait(curGroup, updatedGroup, PROCESS_STREAMS_RECORD_WAIT_TIME);

        // check the current group size is 1, group status is ACTIVE, and all members are ACTIVE
        DynamoDBReplicationGroup resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);
        DynamoDBReplicationGroupMember masterMember = createMasterMember();
        checkGroupStatusAndSize(resultingGroup, DynamoDBReplicationGroupStatus.ACTIVE, 1, Arrays.asList(new String[] {masterMember.getArn()}), null);

        // remove the last replication group member
        updatedGroup = new DynamoDBReplicationGroup(resultingGroup);
        updatedGroup.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.UPDATING);
        memberToDelete = updatedGroup.getReplicationGroupMembers().get(masterMember.getArn());
        memberToDelete.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.DELETING);

        // write the updated group to the metadata table, then wait
        writeToMetadataTableThenWait(resultingGroup, updatedGroup, PROCESS_STREAMS_RECORD_WAIT_TIME);

        // check the current group is now empty
        resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);
        checkGroupStatusAndSize(resultingGroup, DynamoDBReplicationGroupStatus.ACTIVE, 0, null, null);
    }

    @Test
    public void testDeleteReplicationGroup() throws Exception {
        // mock the checking the existence of bootstrap service
        mockStaticPartial(DynamoDBReplicationUtilities.class, "checkIfTableCopyServiceExists");
        DynamoDBReplicationUtilities.checkIfTableCopyServiceExists(anyObject(AccountMapToAwsAccess.class), anyObject(DynamoDBReplicationGroupMember.class),
            anyObject(DynamoDBTableCopyDescription.class));
        expectLastCall().andReturn(false /* table copy service should not exist for an active group */);
        replayAll();

        // create a replication group with a master and a replica
        testCreateReplicationGroupWithSingleReplica();

        // get the current replication group
        DynamoDBReplicationGroup curGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // delete current replication group
        DynamoDBReplicationGroup updatedGroup = new DynamoDBReplicationGroup(curGroup);
        updatedGroup.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.DELETING);
        for (DynamoDBReplicationGroupMember member : updatedGroup.getReplicationGroupMembers().values()) {
            member.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.DELETING);
        }

        // write to metadata table then wait
        writeToMetadataTableThenWait(curGroup, updatedGroup, PROCESS_STREAMS_RECORD_WAIT_TIME);

        // check the metadata table is empty
        assertEquals(MD.readReplicationGroups().size(), 0);
    }

    @Test
    public void testCreateReplicationGroupFailed() throws Exception {
        // mock the launching of connectors
        mockStaticPartial(DynamoDBReplicationUtilities.class, "launchConnectorService");
        DynamoDBReplicationUtilities.launchConnectorService(anyObject(AccountMapToAwsAccess.class), anyObject(DynamoDBReplicationGroup.class),
            anyObject(DynamoDBReplicationGroupMember.class), anyObject(DynamoDBConnectorDescription.class));
        expectLastCall().andThrow(new IllegalStateException());
        replayAll();

        // create one master and one replica without bootstrap task
        DynamoDBReplicationGroupMember masterMember = createMasterMember();
        DynamoDBReplicationGroupMember member = createReplicaMember(false, REPLICA_TABLE_WITHOUT_BOOTSTRAP_NAME);

        // create connector to add to the replica member
        DynamoDBConnectorDescription connector = new DynamoDBConnectorDescription().withSourceTableEndpoint(DYNAMODB_LOCAL_ENDPOINT).withSourceTableArn(
            new DynamoDBArn().withAccountNumber(ACCOUNT_NUM).withRegion(DEFAULT_REGION).withTableName(MASTER_TABLE_NAME).getArnString());
        List<DynamoDBConnectorDescription> connectors = new ArrayList<DynamoDBConnectorDescription>();
        connectors.add(connector);
        member.setConnectors(connectors);

        // create replication group then add the master and replica member
        DynamoDBReplicationGroup group = createEmptyReplicationGroup();
        group.addReplicationGroupMember(masterMember);
        group.addReplicationGroupMember(member);

        // write the replication group to the metadata table, then wait a specified amount of time
        writeToMetadataTableThenWait(null, group, 2 * PROCESS_STREAMS_RECORD_WAIT_TIME);

        // verify the launching of connectors
        verifyAll();

        // read current image of the replication group
        DynamoDBReplicationGroup resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // check the current group size is 2, group status is ACTIVE, and master member is ACTIVE
        checkGroupStatusAndSize(resultingGroup, DynamoDBReplicationGroupStatus.CREATING, 2, Arrays.asList(new String[] {masterMember.getArn()}), null);

        // check replica member is in state CREATE_FAILED
        assertEquals(resultingGroup.getReplicationGroupMembers().get(member.getArn()).getReplicationGroupMemberStatus(),
            DynamoDBReplicationGroupMemberStatus.CREATE_FAILED);
    }

    @Test
    public void testDeleteReplicationGroupFailed() throws Exception {
        // create a group with a master member and a replica member with connectors
        testModifyReplicationGroupMember();

        // mock connector deletion
        mockStaticPartial(DynamoDBReplicationUtilities.class, "deleteConnectorService");
        DynamoDBReplicationUtilities.deleteConnectorService(anyObject(AccountMapToAwsAccess.class), anyObject(DynamoDBReplicationGroupMember.class),
            anyObject(DynamoDBConnectorDescription.class));
        expectLastCall().andThrow(new IllegalStateException());
        replayAll();

        // read current image of the replication group
        DynamoDBReplicationGroup resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // delete current replication group
        DynamoDBReplicationGroup updatedGroup = new DynamoDBReplicationGroup(resultingGroup);
        updatedGroup.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.DELETING);
        for (DynamoDBReplicationGroupMember memberToDelete : updatedGroup.getReplicationGroupMembers().values()) {
            memberToDelete.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.DELETING);
        }

        // write to metadata table then wait
        writeToMetadataTableThenWait(resultingGroup, updatedGroup, PROCESS_STREAMS_RECORD_WAIT_TIME);

        // verify the deletion of connectors
        verifyAll();

        // read current image of the replication group
        resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // check the current group size is 1, group status is DELETING, and there are no ACTIVE or BOOTSTRAPPING members
        checkGroupStatusAndSize(resultingGroup, DynamoDBReplicationGroupStatus.DELETING, 1, null, null);

        // check replica member is in state DELETE_FAILED
        assertEquals(resultingGroup.getReplicationGroupMembers().get(createReplicaMember(false, REPLICA_TABLE_WITHOUT_BOOTSTRAP_NAME).getArn())
            .getReplicationGroupMemberStatus(), DynamoDBReplicationGroupMemberStatus.DELETE_FAILED);
    }

    @Test
    public void testUpdateReplicationGroupAddFailed() throws Exception {
        // mock the launching of connectors
        mockStaticPartial(DynamoDBReplicationUtilities.class, "launchConnectorService");
        DynamoDBReplicationUtilities.launchConnectorService(anyObject(AccountMapToAwsAccess.class), anyObject(DynamoDBReplicationGroup.class),
            anyObject(DynamoDBReplicationGroupMember.class), anyObject(DynamoDBConnectorDescription.class));
        expectLastCall().andThrow(new IllegalStateException());
        replayAll();

        // add new member with bootstrap task to an active group
        testCreateReplicationGroupWithMaster();
        DynamoDBReplicationGroup resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);
        DynamoDBReplicationGroup updatedGroup = new DynamoDBReplicationGroup(resultingGroup);
        DynamoDBReplicationGroupMember memberWithoutBootstrap = createReplicaMember(false, REPLICA_TABLE_WITHOUT_BOOTSTRAP_NAME);

        // create connector to add to the replica member
        DynamoDBConnectorDescription connector = new DynamoDBConnectorDescription().withSourceTableEndpoint(DYNAMODB_LOCAL_ENDPOINT).withSourceTableArn(
            new DynamoDBArn().withAccountNumber(ACCOUNT_NUM).withRegion(DEFAULT_REGION).withTableName(MASTER_TABLE_NAME).getArnString());
        List<DynamoDBConnectorDescription> connectors = new ArrayList<DynamoDBConnectorDescription>();
        connectors.add(connector);
        memberWithoutBootstrap.setConnectors(connectors);
        updatedGroup.addReplicationGroupMember(memberWithoutBootstrap);
        updatedGroup.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.UPDATING);

        // write the updated replication group (with the newly added member) to the metadata table, then wait
        writeToMetadataTableThenWait(resultingGroup, updatedGroup, PROCESS_STREAMS_RECORD_WAIT_TIME);

        // verify the launching of connectors
        verifyAll();

        // read current image of the replication group
        resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // check current group size is 2, the group status is UPDATING, and the master member is ACTIVE
        checkGroupStatusAndSize(resultingGroup, DynamoDBReplicationGroupStatus.UPDATING, 2, Arrays.asList(new String[] {createMasterMember().getArn()}), null);

        // check replica member is in state CREATE_FAILED
        assertEquals(resultingGroup.getReplicationGroupMembers().get(memberWithoutBootstrap.getArn()).getReplicationGroupMemberStatus(),
            DynamoDBReplicationGroupMemberStatus.CREATE_FAILED);
    }

    @Test
    public void testUpdateReplicationGroupRemoveFailed() throws Exception {
        // create a group with a master member and a replica member with connectors
        testModifyReplicationGroupMember();

        // mock connector deletion
        mockStaticPartial(DynamoDBReplicationUtilities.class, "deleteConnectorService");
        DynamoDBReplicationUtilities.deleteConnectorService(anyObject(AccountMapToAwsAccess.class), anyObject(DynamoDBReplicationGroupMember.class),
            anyObject(DynamoDBConnectorDescription.class));
        expectLastCall().andThrow(new IllegalStateException());
        replayAll();

        // read current image of the replication group
        DynamoDBReplicationGroup resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // delete replica member from the replication group
        DynamoDBReplicationGroup updatedGroup = new DynamoDBReplicationGroup(resultingGroup);
        updatedGroup.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.UPDATING);
        DynamoDBReplicationGroupMember memberToDelete = createReplicaMember(false, REPLICA_TABLE_WITHOUT_BOOTSTRAP_NAME);
        updatedGroup.getReplicationGroupMembers().get(memberToDelete.getArn()).setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.DELETING);

        // write to metadata table then wait
        writeToMetadataTableThenWait(resultingGroup, updatedGroup, PROCESS_STREAMS_RECORD_WAIT_TIME);

        // verify the deletion of connectors
        verifyAll();

        // read current image of the replication group
        resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // check the current group size is 2, group status is UPDATING, and the master member is still ACTIVE
        checkGroupStatusAndSize(resultingGroup, DynamoDBReplicationGroupStatus.UPDATING, 2, Arrays.asList(new String[] {createMasterMember().getArn()}), null);

        // check replica member is in state DELETE_FAILED
        assertEquals(resultingGroup.getReplicationGroupMembers().get(memberToDelete.getArn()).getReplicationGroupMemberStatus(),
            DynamoDBReplicationGroupMemberStatus.DELETE_FAILED);
    }

    @Test
    public void testUpdateReplicationGroupUpdateFailed() throws Exception {
        // mock the launching of connectors
        mockStaticPartial(DynamoDBReplicationUtilities.class, "launchConnectorService");
        DynamoDBReplicationUtilities.launchConnectorService(anyObject(AccountMapToAwsAccess.class), anyObject(DynamoDBReplicationGroup.class),
            anyObject(DynamoDBReplicationGroupMember.class), anyObject(DynamoDBConnectorDescription.class));
        expectLastCall().andThrow(new IllegalStateException());
        replayAll();

        // create group with one master and one replica without bootstrap task
        DynamoDBReplicationGroupMember masterMember = createMasterMember();
        DynamoDBReplicationGroupMember member = createReplicaMember(false, REPLICA_TABLE_WITHOUT_BOOTSTRAP_NAME);
        DynamoDBReplicationGroup group = createEmptyReplicationGroup();
        group.addReplicationGroupMember(masterMember);
        group.addReplicationGroupMember(member);

        // write the replication group to the metadata table, then wait a specified amount of time
        writeToMetadataTableThenWait(null, group, 2 * PROCESS_STREAMS_RECORD_WAIT_TIME);

        // read current image of the replication group
        DynamoDBReplicationGroup resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // check the current group size is 2, group status is ACTIVE, and both members (master and replica) are ACTIVE
        checkGroupStatusAndSize(resultingGroup, DynamoDBReplicationGroupStatus.ACTIVE, 2, Arrays.asList(new String[] {masterMember.getArn(), member.getArn()}),
            null);

        // add a connector to the replica table (modification of the replication group member)
        DynamoDBConnectorDescription connector = new DynamoDBConnectorDescription().withSourceTableEndpoint(DYNAMODB_LOCAL_ENDPOINT).withSourceTableArn(
            new DynamoDBArn().withAccountNumber(ACCOUNT_NUM).withRegion(DEFAULT_REGION).withTableName(MASTER_TABLE_NAME).getArnString());
        List<DynamoDBConnectorDescription> connectors = new ArrayList<DynamoDBConnectorDescription>();
        connectors.add(connector);
        DynamoDBReplicationGroup updatedGroup = new DynamoDBReplicationGroup(resultingGroup);
        DynamoDBReplicationGroupMember updatedMember = updatedGroup.getReplicationGroupMembers().get(member.getArn());
        updatedMember.setConnectors(connectors);

        // set both the status of the replication group member and replication group to be UPDATING
        updatedMember.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.UPDATING);
        updatedGroup.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.UPDATING);

        // write the updated replication group to the metadata table, then wait
        writeToMetadataTableThenWait(resultingGroup, updatedGroup, PROCESS_STREAMS_RECORD_WAIT_TIME);

        // verify the launching of connectors
        verifyAll();

        // read current image of the replication group
        resultingGroup = MD.readReplicationGroup(REPLICATION_GROUP_UUID);

        // check the current group size is 2, group status is UPDATING, and the master member is still ACTIVE
        checkGroupStatusAndSize(resultingGroup, DynamoDBReplicationGroupStatus.UPDATING, 2, Arrays.asList(new String[] {createMasterMember().getArn()}), null);

        // check replica member is in state DELETE_FAILED
        assertEquals(resultingGroup.getReplicationGroupMembers().get(member.getArn()).getReplicationGroupMemberStatus(),
            DynamoDBReplicationGroupMemberStatus.UPDATE_FAILED);
    }
}
