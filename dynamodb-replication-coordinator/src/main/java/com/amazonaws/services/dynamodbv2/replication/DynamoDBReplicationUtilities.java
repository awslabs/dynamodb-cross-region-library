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

import static com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationCoordinator.coordinatorAssert;
import static com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationCoordinator.coordinatorFail;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.ConversionSchema;
import com.amazonaws.services.dynamodbv2.datamodeling.ConversionSchemas;
import com.amazonaws.services.dynamodbv2.datamodeling.ItemConverter;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinitionDescription;
import com.amazonaws.services.dynamodbv2.model.Constants;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.DynamoDBArn;
import com.amazonaws.services.dynamodbv2.model.DynamoDBConnectorDescription;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMember;
import com.amazonaws.services.dynamodbv2.model.DynamoDBTableCopyDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElementDescription;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.SecondaryIndexDesc;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.Cluster;
import com.amazonaws.services.ecs.model.ContainerDefinition;
import com.amazonaws.services.ecs.model.CreateServiceRequest;
import com.amazonaws.services.ecs.model.DeleteServiceRequest;
import com.amazonaws.services.ecs.model.DescribeClustersRequest;
import com.amazonaws.services.ecs.model.DescribeClustersResult;
import com.amazonaws.services.ecs.model.DescribeServicesRequest;
import com.amazonaws.services.ecs.model.DescribeServicesResult;
import com.amazonaws.services.ecs.model.KeyValuePair;
import com.amazonaws.services.ecs.model.RegisterTaskDefinitionRequest;
import com.amazonaws.services.ecs.model.RegisterTaskDefinitionResult;
import com.amazonaws.services.ecs.model.Service;
import com.amazonaws.services.ecs.model.UpdateServiceRequest;
import com.amazonaws.util.AwsHostNameUtils;

/**
 * An utility class for common methods used during the replication process.
 */
public class DynamoDBReplicationUtilities {

    /*
     * Logger for the DynamoDBReplicationUtilities class
     */
    private static final Logger LOGGER = Logger.getLogger(DynamoDBReplicationUtilities.class);

    /*
     * Constants for labeling AWS regions
     */
    private static final String AP_NORTHEAST_1 = Regions.AP_NORTHEAST_1.getName();
    private static final String US_EAST_1 = Regions.US_EAST_1.getName();
    private static final String SA_EAST_1 = Regions.SA_EAST_1.getName();
    private static final String AP_SOUTHEAST_2 = Regions.AP_SOUTHEAST_2.getName();
    private static final String AP_SOUTHEAST_1 = Regions.AP_SOUTHEAST_1.getName();
    private static final String EU_WEST_1 = Regions.EU_WEST_1.getName();
    private static final String EU_CENTRAL_1 = Regions.EU_CENTRAL_1.getName();
    private static final String US_WEST_2 = Regions.US_WEST_2.getName();
    private static final String US_WEST_1 = Regions.US_WEST_1.getName();

    /*
     * Constants used for launching DynamoDB connector and KCL checkpointing
     */
    private static final String DOT_DELIMITER = ".";
    private static final String AMAZONAWS_SUFFIX = "amazonaws.com";
    private static final String ECS_SERVICE_NAME = "ecs";
    private static final String KCL_LEASE_KEY = "leaseCounter";
    private static final String HASH_ALGORITHM = "MD5";

    /*
     * Constants used for communicating with EC2 Container Service (ECS)
     */
    private static final String DEFAULT_ECS_REGION = US_EAST_1;
    private static final String[] DEFAULT_ECS_TASK_ENTRYPOINT = {"sh", "-c"};
    private static final boolean DEFAULT_ECS_TASK_ESSENTIAL = true;
    private static final Integer ZERO_DESIRED_TASK = 0;
    private static final Integer INITIAL_DESIRED_TASK = 1;
    private static final Integer DEFAULT_SERVICE_STATUS_TIMEOUT = 15;
    private static final String ECS_STATUS_INACTIVE = "INACTIVE";
    private static final String ECS_STATUS_ACTIVE = "ACTIVE";
    private static final String CONNECTOR_TASK_DEFINITION_NAME = "DynamoDBReplicationConnector";
    private static final String TABLECOPY_TASK_DEFINITION_NAME = "DynamoDBTableCopy";
    private static final String DESTINATION_TABLE_LABEL = "DESTINATION_TABLE";
    private static final String DESTINATION_ENDPOINT_LABEL = "DESTINATION_ENDPOINT";
    private static final String CONNECTOR_JAR_BUCKET_NAME_LABEL = "CONNECTOR_JAR_BUCKET_NAME";
    private static final String CONNECTOR_JAR_FILE_LABEL = "CONNECTOR_JAR_FILE";
    private static final String CONNECTOR_TYPE_LABEL = "CONNECTOR_TYPE";
    private static final String SOURCE_TABLE_LABEL = "SOURCE_TABLE";
    private static final String SOURCE_ENDPOINT_LABEL = "SOURCE_ENDPOINT";
    private static final Set<String> ECS_SUPPORTED_REGIONS = new TreeSet<String>(new Comparator<String>() {
        // make sure DEFAULT_ECS_REGION is always checked first
        @Override
        public int compare(String string0, String string1) {
            if (string0.equals(DEFAULT_ECS_REGION)) {
                return -1;
            } else if (string1.equals(DEFAULT_ECS_REGION)) {
                return 1;
            } else {
                return string0.compareTo(string1);
            }
        }
    });
    static {
        ECS_SUPPORTED_REGIONS.add(US_EAST_1);
        ECS_SUPPORTED_REGIONS.add(US_WEST_2);
        ECS_SUPPORTED_REGIONS.add(EU_WEST_1);
        ECS_SUPPORTED_REGIONS.add(AP_NORTHEAST_1);
        ECS_SUPPORTED_REGIONS.add(AP_SOUTHEAST_2);
    }

    /*
     * Constants for EC2 Container Service Task Definition for DynamoDB Connectors
     */
    private static final int DEFAULT_CONNECTOR_TASK_CPU = 512;
    private static final int DEFAULT_CONNECTOR_TASK_MEMORY = 512;
    private static final String DEFAULT_CONNECTOR_TASK_IMAGE = "dynamodbecosystemdev/dynamodbcrossregionreplication";
    private static final String[] DEFAULT_CONNECTOR_TASK_COMMAND = {"/opt/scripts/start_connector.sh"};
    private static final String DEFAULT_CONNECTOR_JAR_FILE = "DynamoDBConnectors.jar";
    private static final String DEFAULT_CONNECTOR_JAR_BUCKET_NAME = "dynamodb-cross-region";

    /*
     * Constants for EC2 Container Service Task Definition for DynamoDB Tablecopy
     */
    private static final String DEFAULT_SOURCE_READ_FRACTION = "1";
    private static final String DEFAULT_DESTINATION_WRITE_FRACTION = "1";
    private static final int DEFAULT_TABLECOPY_TASK_CPU = 512;
    private static final int DEFAULT_TABLECOPY_TASK_MEMORY = 512;
    private static final String DEFAULT_TABLECOPY_TASK_IMAGE = "dynamodbecosystemdev/dynamodbcrossregionreplication";
    private static final String[] DEFAULT_TABLECOPY_TASK_COMMAND = {"/opt/scripts/start_tablecopy.sh"};
    private static final String SOURCE_READ_FRACTION_LABEL = "SOURCE_READ_FRACTION";
    private static final String DESTINATION_WRITE_FRACTION_LABEL = "DESTINATION_WRITE_FRACTION";
    private static final String REPLICATION_GROUP_MEMBER_LABEL = "REPLICATION_GROUP_MEMBER";
    private static final String REPLICATION_GROUP_UUID_LABEL = "REPLICATION_GROUP_UUID";
    private static final String METADATA_TABLE_ENDPOINT_LABEL = "METADATA_TABLE_ENDPOINT";
    private static final String METADATA_TABLE_NAME_LABEL = "METADATA_TABLE_NAME";
    private static final String ECS_SERVICE_NAME_LABEL = "ECS_SERVICE_NAME";

    /*
     * Prefixes and pattern matching for region and streams endpoint parsing
     */
    public static final String STREAMS_PREFIX = "streams.";
    public static final String PROTOCOL_REGEX = "^(https?://)?(.+)";

    /*
     * The amount of time for thread sleep and time outs
     */
    private static final long SLEEP_LENGTH = 5000l;
    private static final long WAITING_TIME_OUT = 60000l;

    /*
     * Converter for switching between Map<String, AttributeValue> and Item
     */
    public static final ItemConverter ITEM_CONVERTER = ConversionSchemas.V2_COMPATIBLE.getConverter((new ConversionSchema.Dependencies()));

    /*
     * MD5 digest instance
     */
    public static final MessageDigest MD5_DIGEST = getMessageDigestInstance(HASH_ALGORITHM);

    /*
     * Default stream specifications for tables in the replication group
     */
    public static final boolean defaultStreamEnabled = true;
    public static final StreamViewType defaultStreamViewType = StreamViewType.NEW_AND_OLD_IMAGES;

    private static MessageDigest getMessageDigestInstance(String hashAlgorithm) {
        try {
            return MessageDigest.getInstance(hashAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("Specified hash algorithm does not exist: " + hashAlgorithm + e);
            return null;
        }
    }

    /**
     * Creates the table for the given replication group member, if it does not exist. Otherwise, if the table exists but has an incorrect
     * {@link StreamSpecification}, update the table with the correct {@link StreamSpecification}.
     *
     * @param group
     *            the replication group containing the table that needs to be created
     * @param createMember
     *            the replication group member representing the table that needs to be created
     * @param awsAccess
     *            AWS access class providing DynamoDB clients to create the table
     */
    public static void createTableIfNotExists(DynamoDBReplicationGroup group, DynamoDBReplicationGroupMember createMember, AccountMapToAwsAccess awsAccess)
        throws Exception {
        // The account number of the replication group member
        String accountNumber = new DynamoDBArn().withArnString(createMember.getArn()).getAccountNumber();

        // Retrieve the appropriate DynamoDB client based on the account number
        AwsAccess accountAccess = awsAccess.getAccessAccount(accountNumber);
        coordinatorAssert(accountAccess != null, "No account access credentials found for account number: " + accountNumber);
        AmazonDynamoDB ddbClient = accountAccess.getDynamoDB(createMember.getEndpoint());

        // Get table name from the replication group member ARN
        String tableName = new DynamoDBArn().withArnString(createMember.getArn()).getTableName();

        // Create default streams specification
        StreamSpecification replicationStreamSpec = new StreamSpecification().withStreamEnabled(defaultStreamEnabled).withStreamViewType(defaultStreamViewType);
        try {
            DescribeTableResult result = ddbClient.describeTable(tableName);

            // verify existing table matches the replication group attribute definitions and key schema
            TableDescription table = result.getTable();
            coordinatorAssert(
                table.getAttributeDefinitions().equals(AttributeDefinitionDescription.convertToAttributeDefinitions(group.getAttributeDefinitions())),
                "Existing attribution definitions of replication member table do not match the group's.");
            coordinatorAssert(table.getKeySchema().equals(KeySchemaElementDescription.convertToKeySchemaElements(group.getKeySchema())),
                "Existing key schema of replication member table does not match the group's");

            // turn on Streams if necessary
            if (createMember.getStreamsEnabled()) {
                StreamSpecification streamSpec = result.getTable().getStreamSpecification();
                if (streamSpec == null || !replicationStreamSpec.equals(streamSpec)) {
                    ddbClient.updateTable(new UpdateTableRequest().withTableName(tableName).withStreamSpecification(replicationStreamSpec));
                }
            }
        } catch (ResourceNotFoundException e) {
            // Create table
            coordinatorAssert(createMember.getProvisionedThroughput() != null, "ProvisionedThroughput for member not specified for table creations.");
            CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                .withAttributeDefinitions(AttributeDefinitionDescription.convertToAttributeDefinitions(group.getAttributeDefinitions()))
                .withKeySchema(KeySchemaElementDescription.convertToKeySchemaElements(group.getKeySchema()))
                .withProvisionedThroughput(createMember.getProvisionedThroughput().toProvisionedThroughput());
            if (createMember.getGlobalSecondaryIndexes() != null && !createMember.getGlobalSecondaryIndexes().isEmpty()) {
                createTableRequest.setGlobalSecondaryIndexes(SecondaryIndexDesc.toGSIList(createMember.getGlobalSecondaryIndexes()));
            }

            if (createMember.getLocalSecondaryIndexes() != null && !createMember.getLocalSecondaryIndexes().isEmpty()) {
                createTableRequest.setLocalSecondaryIndexes(SecondaryIndexDesc.toLSIList(createMember.getLocalSecondaryIndexes()));
            }

            if (createMember.getStreamsEnabled()) {
                createTableRequest.setStreamSpecification(replicationStreamSpec);
            }
            ddbClient.createTable(createTableRequest);
        }
        waitForTableActive(ddbClient, tableName, WAITING_TIME_OUT);
    }

    /**
     * Wait for the table to finish creating or updating, status changed to ACTIVE
     *
     * @param ddbClient
     *            the DynamoDB client used to access the table
     * @param memberTableName
     *            the name of the table
     */
    public static void waitForTableActive(AmazonDynamoDB ddbClient, String memberTableName, long timeOut) throws Exception {
        long timeRemaining = timeOut;
        while (true) {
            long startTime = System.currentTimeMillis();
            TableDescription tableDesc = ddbClient.describeTable(memberTableName).getTable();
            TableStatus status = TableStatus.valueOf(tableDesc.getTableStatus());
            switch (status) {
                case ACTIVE:
                    return;
                case UPDATING:
                    break;
                case CREATING:
                    break;
                case DELETING:
                    coordinatorFail("Invalid table state, waiting for ACTIVE table but table is in DELETING state.");
                default:
                    coordinatorFail("Invalid table state, not in one of ACTIVE, UPDATING, CREATING or DELETING.");
            }
            try {
                Thread.sleep(SLEEP_LENGTH);
            } catch (InterruptedException e) {
                LOGGER.debug("Thread was interrupted while polling status of table: " + memberTableName);
            }
            timeRemaining -= System.currentTimeMillis() - startTime;
            if (timeRemaining <= 0) {
                coordinatorFail("Timeout waiting for table " + memberTableName + " to become active.");
            }
        }
    }

    /**
     * Wait for the KCL checkpoint table to become active and the lease to be taken, indicating KCL worker is ready
     *
     * @param ddbClient
     *            The DynamoDB client used to scan the KCL checkpoint table
     * @param kclTableName
     *            The name of the KCL checkpoint table
     * @param timeOut
     *            The amount of time to check for before returning an error
     */
    public static void waitForKCLToBecomeActive(AmazonDynamoDB ddbClient, String kclTableName, long timeOut) throws Exception {
        long timeRemaining = timeOut;
        while (true) {
            long startTime = System.currentTimeMillis();
            try {
                ScanResult result = ddbClient.scan(new ScanRequest(kclTableName));
                if (!result.getItems().isEmpty()) {
                    int counter = Integer.parseInt(result.getItems().get(0).get(KCL_LEASE_KEY).getN());
                    if (counter != 0) {
                        break; // KCL checkpoint table is ready
                    }
                }
            } catch (ResourceNotFoundException re) {
                LOGGER.debug("KCL checkpoint table not created yet.");
                try {
                    Thread.sleep(SLEEP_LENGTH);
                } catch (InterruptedException ie) {
                    LOGGER.debug("Thread interrupted while waiting for KCL checkpoint table to create.");
                }
            }
            timeRemaining -= System.currentTimeMillis() - startTime;
            if (timeRemaining <= 0) {
                coordinatorFail("Timeout waiting for table " + kclTableName + " to become active.");
            }
        }
    }

    /**
     * Get the ECS endpoint for a given region
     *
     * @param region
     *            input region to form as a part of the endpoint
     * @return ECS endpoint formed from the given region
     */
    public static String getECSEndpointFromRegion(String region) {
        return ECS_SERVICE_NAME + DOT_DELIMITER + region + DOT_DELIMITER + AMAZONAWS_SUFFIX;
    }

    /**
     * Get the MD5 hashed service name given the ARN of the source and destination member
     *
     * @param sourceMemberArn
     *            ARN of the source replication member
     * @param destinationMemberArn
     *            ARN of the destination replication member
     * @param prefix
     *            application prefix to prepend to the hashed service name
     * @return the full hashed service name prepended by the given prefix
     * @throws Exception
     *             any exception encountered while creating the hash string
     */
    public static String getHashedServiceName(String sourceMemberArn, String destinationMemberArn, String prefix) throws Exception {
        // construct ARN object from arn string
        DynamoDBArn sourceArnObject = new DynamoDBArn().withArnString(sourceMemberArn);
        DynamoDBArn destinationArnObject = new DynamoDBArn().withArnString(destinationMemberArn);

        // return the hashed string by extracting the region and name of the source and destination tables
        return getHashedServiceName(sourceArnObject.getRegion(), sourceArnObject.getTableName(), destinationArnObject.getRegion(),
            destinationArnObject.getTableName(), prefix);
    }

    /**
     * Get the MD5 hashed service name given the region and name of the source and destination table
     *
     * @param sourceRegion
     *            Region of the source replication member
     * @param sourceTable
     *            Table name of the source replication member
     * @param destinationRegion
     *            Region of the destination replication member
     * @param destinationTable
     *            Table name of the destination replication member
     * @param prefix
     *            application prefix to prepend to the hashed service name
     * @return the full hashed service name prepended by the given prefix
     * @throws Exception
     */
    public static String getHashedServiceName(String sourceRegion, String sourceTable, String destinationRegion, String destinationTable, String prefix)
        throws Exception {
        // form unhashed stack name by concatenating source region, source table, destination region and destination table
        String unhashedStackName = sourceRegion + sourceTable + destinationRegion + destinationTable;

        // hash stack name using MD5
        if (MD5_DIGEST == null) {
            throw new IllegalArgumentException("MD5 digest object not properly initialized!");
        }
        byte hashByteData[] = MD5_DIGEST.digest(unhashedStackName.getBytes(Constants.ENCODING));

        // return the hashed stackname in hexadecimal format, with a specified prefix
        return prefix + new String(Hex.encodeHex(hashByteData));
    }

    /**
     * Checks whether the given enum value is defined in the given enum class
     *
     * @param value
     *            given enum value to search for in the enum class
     * @param enumClass
     *            the enum class to search in for the given enum value
     * @return true if given enum is found within the given enum class, false otherwise
     */
    public static <E extends Enum<E>> boolean isInEnum(String value, Class<E> enumClass) {
        for (E e : enumClass.getEnumConstants()) {
            if (e.name().equals(value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Convert a given endpoint into its corresponding region
     *
     * @param endpoint
     *            given endpoint URL
     * @return the extracted region corresponding to the given endpoint
     */
    public static String getRegionFromEndpoint(String endpoint) {
        return AwsHostNameUtils.parseRegionName(endpoint, null /* no service hints */);
    }

    /**
     * Convert a given DynamoDB endpoint into its corresponding Streams endpoint
     *
     * @param endpoint
     *            given endpoint URL
     * @return the extracted Streams endpoint corresponding to the given DynamoDB endpoint
     */
    public static String getStreamsEndpoint(String endpoint) {
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

    /**
     * Compares {@link DynamoDBReplicationGroupMember} based on endpoint, and then member table name.
     */
    public static final Comparator<DynamoDBReplicationGroupMember> SAME_GROUP_MEMBER_COMPARATOR = new Comparator<DynamoDBReplicationGroupMember>() {
        @Override
        public int compare(DynamoDBReplicationGroupMember m0, DynamoDBReplicationGroupMember m1) {
            return m0.getArn().compareTo(m1.getArn());
        }
    };

    /**
     * Comparator that prioritizes creating candidates without a bootstrap task, or without a connector.
     */
    public static final Comparator<? super DynamoDBReplicationGroupMember> CREATING_PRIORITY_GROUP_MEMBER_COMPARATOR = new Comparator<DynamoDBReplicationGroupMember>() {
        @Override
        public int compare(DynamoDBReplicationGroupMember m0, DynamoDBReplicationGroupMember m1) {
            if (m0.getTableCopyTask() == null && m1.getTableCopyTask() != null) {
                return -1;
            } else if (m0.getTableCopyTask() != null && m1.getTableCopyTask() == null) {
                return 1;
            } else {
                if ((m0.getConnectors() == null || m0.getConnectors().isEmpty()) && m1.getConnectors() != null && !m1.getConnectors().isEmpty()) {
                    return -1;
                } else if (m0.getConnectors() != null && !m0.getConnectors().isEmpty() && (m1.getConnectors() == null || m1.getConnectors().isEmpty())) {
                    return 1;
                } else {
                    return SAME_GROUP_MEMBER_COMPARATOR.compare(m0, m1);
                }
            }
        }
    };

    /**
     * Launch ECS service for table copy job
     *
     * @param awsAccess
     *            AWS access map used to store all clients for AWS services
     * @param group
     *            replication group of the replication group member for which the resources are being launched
     * @param member
     *            the replication group member that the resources being launched belongs to
     * @param metadata
     *            the metadata object used to access the DynamoDB metadata table the coordinator is associated with
     * @throws Exception
     *             any exceptions thrown when attempting to launch the ECS service
     */
    public static void launchTableCopyService(AccountMapToAwsAccess awsAccess, DynamoDBReplicationGroup group, DynamoDBReplicationGroupMember member,
        DynamoDBMetadataStorage metadata) throws Exception {
        launchService(awsAccess, group, member, null /* no connector parameter for table copy */, metadata);
    }

    /**
     * Launch ECS service for DynamoDB connector
     *
     * @param awsAccess
     *            AWS access map used to store all clients for AWS services
     * @param group
     *            replication group of the replication group member for which the resources are being launched
     * @param member
     *            the replication group member that the resources being launched belongs to
     * @param connector
     *            the connector being launched
     * @throws Exception
     *             any exceptions thrown when attempting to launch the ECS service
     */
    public static void launchConnectorService(AccountMapToAwsAccess awsAccess, DynamoDBReplicationGroup group, DynamoDBReplicationGroupMember member,
        DynamoDBConnectorDescription connector) throws Exception {
        launchService(awsAccess, group, member, connector, null /* no metadata parameter required for connector launch */);
    }

    /**
     * Launch an ECS service on the default cluster, using the given replication group, member, and one of connector or metadata depending on the type of
     * service to be launched
     *
     * @param awsAccess
     *            AWS access map used to store all clients for AWS services
     * @param group
     *            replication group of the replication group member for which the resources are being launched
     * @param member
     *            the replication group member that the resources being launched belongs to
     * @param connector
     *            the connector being launched
     * @param metadata
     *            the metadata object used to access the DynamoDB metadata table the coordinator is associated with
     * @throws Exception
     *             any exceptions thrown when attempting to launch the ECS service
     */
    private static void launchService(AccountMapToAwsAccess awsAccess, DynamoDBReplicationGroup group, DynamoDBReplicationGroupMember member,
        DynamoDBConnectorDescription connector, DynamoDBMetadataStorage metadata) throws Exception {

        // generate service name based on the type of service being launched
        String serviceName;
        if (connector == null) {
            // find service name from the given member and member table copy task
            serviceName = getHashedServiceName(member.getTableCopyTask().getSourceTableArn(), member.getArn(), TABLECOPY_TASK_DEFINITION_NAME);
        } else {
            // find service name from the given member and connector
            serviceName = getHashedServiceName(connector.getSourceTableArn(), member.getArn(), CONNECTOR_TASK_DEFINITION_NAME);
        }

        // retrieve AwsAccess object for given account
        AwsAccess memberAwsAccess = awsAccess.getAccessAccount(new DynamoDBArn().withArnString(member.getArn()).getAccountNumber());

        // search for existing active services with the same name across all ECS-supported regions
        List<AmazonECS> ecsClientList = searchForServiceInCluster(memberAwsAccess, serviceName);

        // sanity check for existing services with the same name
        if (!ecsClientList.isEmpty()) {
            LOGGER.info("An existing ACTIVE service with the same name already exists in one of the regions, not launching a new one.");
            if (ecsClientList.size() > 1) {
                LOGGER
                    .error("More than one service with the same name found across multiple regions, this most likely causes incorrect behavior in replication.");
                return;
            }
            // poll to make sure service is ACTIVE and running desired number of tasks
            waitForServiceStatus(ecsClientList.get(0), serviceName, ECS_STATUS_ACTIVE, TimeUnit.MINUTES.toMillis(DEFAULT_SERVICE_STATUS_TIMEOUT));
        } else {
            // determine ECS region (since ECS is not available in all regions) and get appropriate client for it
            String region = determineECSRegion(getRegionFromEndpoint(member.getEndpoint()));
            AmazonECS memberECSClient = memberAwsAccess.getECS(getECSEndpointFromRegion(region));

            // check whether a regional ECS cluster exists for the region member is in
            if (!checkIfDefaultClusterExists(memberECSClient)) {
                // use the first default cluster we find in any of the ECS supported regions (us-east-1 should be checked first)
                memberECSClient = searchForDefaultCluster(memberAwsAccess);
                if (memberECSClient == null) {
                    coordinatorFail("Default cluster named " + Constants.DEFAULT_CLUSTER_NAME
                        + " does not exist in any regions, cannot use EC2 Container Service for replication!");
                    return; // Unreachable, for safety and correctness
                }
            }

            // generate the task definition based on the type of service being launched
            String taskDefinitionArn;
            if (connector == null) {
                taskDefinitionArn = createAndRegisterTableCopyTaskDefinition(memberECSClient, group, member, metadata);
            } else {
                // create and register new task definition with given member
                taskDefinitionArn = createAndRegisterConnectorTaskDefinition(memberECSClient, group, member, connector);
            }

            // create service with the given task definition arn
            CreateServiceRequest serviceRequest = new CreateServiceRequest().withCluster(Constants.DEFAULT_CLUSTER_NAME).withDesiredCount(INITIAL_DESIRED_TASK)
                .withServiceName(serviceName).withTaskDefinition(taskDefinitionArn);
            memberECSClient.createService(serviceRequest);
            // poll to make sure service is ACTIVE and running desired number of tasks
            waitForServiceStatus(memberECSClient, serviceName, ECS_STATUS_ACTIVE, TimeUnit.MINUTES.toMillis(DEFAULT_SERVICE_STATUS_TIMEOUT));
        }

    }

    /**
     * Delete ECS service that is running DynamoDB connector
     *
     * @param awsAccess
     *            AWS access map used to store all clients for AWS services
     * @param member
     *            the replication group member that the resources being deleted belongs to
     * @param connector
     *            the connector being deleted
     * @throws Exception
     *             any exceptions thrown when attempting to delete the ECS service
     */
    public static void deleteConnectorService(AccountMapToAwsAccess awsAccess, DynamoDBReplicationGroupMember member, DynamoDBConnectorDescription connector)
        throws Exception {
        deleteService(awsAccess, member, connector);
    }

    /**
     * Delete ECS service that is running DynamoDB table copy task
     *
     * @param awsAccess
     *            AWS access map used to store all clients for AWS services
     * @param member
     *            the replication group member that the resources being deleted belongs to
     * @throws Exception
     *             any exceptions thrown when attempting to delete the ECS service
     */
    public static void deleteTableCopyService(AccountMapToAwsAccess awsAccess, DynamoDBReplicationGroupMember member) throws Exception {
        deleteService(awsAccess, member, null /* no connector parameter needed for deleting table copy service */);
    }

    /**
     * Delete an ECS service on the default cluster, using the given replication group, member and connector
     *
     * @param awsAccess
     *            AWS access map used to store all clients for AWS services
     * @param member
     *            the replication group member that the resources being deleted belongs to
     * @param connector
     *            the connector being deleted if deleting connector service
     * @throws Exception
     *             any exceptions thrown when attempting to delete the ECS service
     */
    private static void deleteService(AccountMapToAwsAccess awsAccess, DynamoDBReplicationGroupMember member, DynamoDBConnectorDescription connector)
        throws Exception {

        // generate service name based on the type of service being launched
        String serviceName;
        if (connector == null) {
            // find service name from the given member and member table copy task
            serviceName = getHashedServiceName(member.getTableCopyTask().getSourceTableArn(), member.getArn(), TABLECOPY_TASK_DEFINITION_NAME);
        } else {
            // find service name from the given member and connector
            serviceName = getHashedServiceName(connector.getSourceTableArn(), member.getArn(), CONNECTOR_TASK_DEFINITION_NAME);
        }

        // retrieve AwsAccess object for given account
        AwsAccess memberAwsAccess = awsAccess.getAccessAccount(new DynamoDBArn().withArnString(member.getArn()).getAccountNumber());

        // find all existing active services with the same name across all ECS-supported regions
        List<AmazonECS> ecsClientList = searchForServiceInCluster(memberAwsAccess, serviceName);

        // sanity check for existing services with the same name
        if (ecsClientList.isEmpty()) {
            LOGGER.info("Service named " + serviceName + " cannot be found in default cluster named " + Constants.DEFAULT_CLUSTER_NAME
                + " in any regions, so it may have already been deleted.");
            return;
        } else if (ecsClientList.size() > 1) {
            LOGGER
                .warn("More than one service with the same name found across multiple regions, all of them will be deleted as this likely causes incorrect behavior in replication.");
        }

        // update all services to desired count zero first, then delete all of them
        for (AmazonECS memberECSClient : ecsClientList) {
            memberECSClient.updateService(new UpdateServiceRequest().withCluster(Constants.DEFAULT_CLUSTER_NAME).withService(serviceName)
                .withDesiredCount(ZERO_DESIRED_TASK));
            memberECSClient.deleteService(new DeleteServiceRequest().withCluster(Constants.DEFAULT_CLUSTER_NAME).withService(serviceName));
            waitForServiceStatus(memberECSClient, serviceName, ECS_STATUS_INACTIVE, TimeUnit.MINUTES.toMillis(15));
        }
    }

    public static boolean checkIfTableCopyServiceExists(AccountMapToAwsAccess awsAccess, DynamoDBReplicationGroupMember member,
        DynamoDBTableCopyDescription tableCopyTask) throws Exception {
        // find service name from the given member and connector
        String serviceName = getHashedServiceName(tableCopyTask.getSourceTableArn(), member.getArn(), TABLECOPY_TASK_DEFINITION_NAME);

        // retrieve AwsAccess object for given account
        AwsAccess memberAwsAccess = awsAccess.getAccessAccount(new DynamoDBArn().withArnString(member.getArn()).getAccountNumber());

        // search for the service with the given name in clusters of all regions
        return !searchForServiceInCluster(memberAwsAccess, serviceName).isEmpty();
    }

    /**
     * Wait for ECS service to reach expected status, and time out with an error if it does not do so within the given length of time
     *
     * @param ecsClient
     *            The ECS client used to check the status of the service
     * @param serviceName
     *            The name of the service to check status for
     * @param expectedStatus
     *            The expected status of the service to wait for
     * @param timeOut
     *            The length of time to wait for expected status before giving an error
     * @throws Exception
     *             All exceptions encountered during the polling of status process
     */
    private static void waitForServiceStatus(AmazonECS ecsClient, String serviceName, String expectedStatus, Long timeOut) throws Exception {
        long timeRemaining = timeOut;
        while (true) {
            long startTime = System.currentTimeMillis();

            // get service objects for all services with the given name in the given region and cluster
            DescribeServicesResult result = ecsClient.describeServices(new DescribeServicesRequest().withCluster(Constants.DEFAULT_CLUSTER_NAME).withServices(
                serviceName));

            // if there are no services found with the given name
            if (result.getServices().isEmpty()) {
                // if expected status is INACTIVE or DRAINING, it should be acceptable that this service no longer exists
                if (!expectedStatus.equals(ECS_STATUS_ACTIVE)) {
                    return; // expected status achieved
                } else {
                    coordinatorFail("Expecting service to be active but no services found with the given name.");
                }
            }

            // check the status of existing service with the same name
            for (Service service : result.getServices()) {
                if (service.getStatus().equals(expectedStatus)) {
                    if (!expectedStatus.equals(ECS_STATUS_ACTIVE) || service.getRunningCount().equals(service.getDesiredCount())) {
                        return; // expected status achieved, or number of running tasks match desired count
                    }
                }
            }

            // expected status not achieved, wait a bit longer
            try {
                Thread.sleep(SLEEP_LENGTH);
            } catch (InterruptedException ie) {
                LOGGER.debug("Thread interrupted while waiting for service status to change to expected status.");
            }

            // count down time remaining
            timeRemaining -= System.currentTimeMillis() - startTime;
            if (timeRemaining <= 0) {
                // timed out waiting for current status to change to expected status, delete the service first
                if (expectedStatus.equals(ECS_STATUS_ACTIVE)) {
                    ecsClient.updateService(new UpdateServiceRequest().withCluster(Constants.DEFAULT_CLUSTER_NAME).withService(serviceName)
                        .withDesiredCount(ZERO_DESIRED_TASK));
                    ecsClient.deleteService(new DeleteServiceRequest().withCluster(Constants.DEFAULT_CLUSTER_NAME).withService(serviceName));
                }
                coordinatorFail("Timeout waiting for service " + serviceName + " to become " + expectedStatus + ".");
            }
        }
    }

    /**
     * ECS is not supported in all available AWS regions, so we must assign the closest supported region if a replication group member's region is unsupported
     *
     * @param originalRegion
     *            The original region of the replication group member
     * @return The resulting ECS-supported region, either the same as or closest to the originalRegion
     */
    private static String determineECSRegion(String originalRegion) {
        // if the region is supported by ECS, use the region directly
        if (ECS_SUPPORTED_REGIONS.contains(originalRegion)) {
            return originalRegion;
        } else { // otherwise, pick the closest region that supports ECS
            if (originalRegion.equals(US_WEST_1)) {
                return US_WEST_2;
            } else if (originalRegion.equals(EU_CENTRAL_1)) {
                return EU_WEST_1;
            } else if (originalRegion.equals(AP_SOUTHEAST_1)) {
                return AP_SOUTHEAST_2;
            } else if (originalRegion.equals(SA_EAST_1)) {
                return US_EAST_1;
            } else {
                return DEFAULT_ECS_REGION;
            }
        }
    }

    /**
     * Find an ECS client that corresponds to a region that contains an ECS cluster with the default name for cross region replication
     *
     * @param awsAccess
     *            The AwsAccess object to use in order to access appropriate ECS clients to check for the existence of the default cluster
     * @return The corresponding ECS client that contains a default cluster for cross region replication
     * @throws Exception
     *             Any exceptions encountered during the process of searching for a default cluster
     */
    private static AmazonECS searchForDefaultCluster(AwsAccess awsAccess) throws Exception {
        for (String region : ECS_SUPPORTED_REGIONS) {
            AmazonECS ecsClient = awsAccess.getECS(getECSEndpointFromRegion(region));
            DescribeClustersResult result = ecsClient.describeClusters(new DescribeClustersRequest().withClusters(Constants.DEFAULT_CLUSTER_NAME));
            for (Cluster cluster : result.getClusters()) {
                if (cluster.getClusterName().equals(Constants.DEFAULT_CLUSTER_NAME) && cluster.getStatus().equals(ECS_STATUS_ACTIVE)) {
                    return ecsClient;
                }
            }
        }
        return null;
    }

    /**
     * Find a list of ECS clients that correspond to regions that contain the given service in the default cluster for cross region replication
     *
     * @param awsAccess
     *            The AwsAccess object to use in order to access appropriate ECS clients to check for the existence of the given service
     * @param serviceName
     *            The name of the service to look for in the default cluster
     * @return A list of ECS clients that correspond to regions where the given service can be found in the default cluster
     * @throws Exception
     *             Any exceptions encountered during the process of searching for the given service in the default cluster
     */
    private static List<AmazonECS> searchForServiceInCluster(AwsAccess awsAccess, String serviceName) throws Exception {
        List<AmazonECS> ecsClientList = new ArrayList<AmazonECS>();
        for (String region : ECS_SUPPORTED_REGIONS) {
            AmazonECS ecsClient = awsAccess.getECS(getECSEndpointFromRegion(region));

            // only check for the existence of the service if the cluster exists
            if (checkIfDefaultClusterExists(ecsClient)) {
                DescribeServicesResult result = ecsClient.describeServices(new DescribeServicesRequest().withCluster(Constants.DEFAULT_CLUSTER_NAME)
                    .withServices(serviceName));
                for (Service service : result.getServices()) {
                    if (service.getServiceName().equals(serviceName) && service.getStatus().equals(ECS_STATUS_ACTIVE)) {
                        ecsClientList.add(ecsClient);
                    }
                }
            }
        }
        return ecsClientList;
    }

    /**
     * Check whether a cluster with the given name exists in the given region
     *
     * @param ecsClient
     *            The ECS client corresponding to the region in which to check for the default cluster
     * @return true if the cluster exists
     */
    private static boolean checkIfDefaultClusterExists(AmazonECS ecsClient) {
        try {
            DescribeClustersResult result = ecsClient.describeClusters(new DescribeClustersRequest().withClusters(Constants.DEFAULT_CLUSTER_NAME));
            for (Cluster cluster : result.getClusters()) {
                if (cluster.getClusterName().equals(Constants.DEFAULT_CLUSTER_NAME) && cluster.getStatus().equals(ECS_STATUS_ACTIVE)) {
                    return true;
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Could not check if regional cluster exists, using default cluster instead.");
        }
        return false;
    }

    /**
     * Create task definition for DynamoDB connector and register it using the given ECS client to the default cluster
     *
     * @param ecsClient
     *            The ECS client to use to register the task definition
     * @param group
     *            The replication group from which to create the task definition
     * @param member
     *            The replication group member from which to create the task definition
     * @param connector
     *            The connector of the replication group member from which to create the task definition
     * @return The ARN of the newly registered task definition
     */
    private static String createAndRegisterConnectorTaskDefinition(AmazonECS ecsClient, DynamoDBReplicationGroup group, DynamoDBReplicationGroupMember member,
        DynamoDBConnectorDescription connector) throws Exception {
        // Get arn object from member arn string
        DynamoDBArn destinationArnObject = new DynamoDBArn().withArnString(member.getArn());
        DynamoDBArn sourceArnObject = new DynamoDBArn().withArnString(connector.getSourceTableArn());

        // Create environment list and elements
        List<KeyValuePair> environment = new ArrayList<KeyValuePair>();
        environment.add(new KeyValuePair().withName(DESTINATION_TABLE_LABEL).withValue(destinationArnObject.getTableName()));
        environment.add(new KeyValuePair().withName(DESTINATION_ENDPOINT_LABEL).withValue(member.getEndpoint()));
        environment.add(new KeyValuePair().withName(SOURCE_TABLE_LABEL).withValue(sourceArnObject.getTableName()));
        environment.add(new KeyValuePair().withName(SOURCE_ENDPOINT_LABEL).withValue(connector.getSourceTableEndpoint()));
        environment.add(new KeyValuePair().withName(CONNECTOR_TYPE_LABEL).withValue(group.getConnectorType().toString()));
        environment.add(new KeyValuePair().withName(CONNECTOR_JAR_BUCKET_NAME_LABEL).withValue(DEFAULT_CONNECTOR_JAR_BUCKET_NAME));
        environment.add(new KeyValuePair().withName(CONNECTOR_JAR_FILE_LABEL).withValue(DEFAULT_CONNECTOR_JAR_FILE));

        // Create container definition element and populate it with values
        ContainerDefinition containerDefinition = new ContainerDefinition();
        containerDefinition.setCommand(Arrays.asList(DEFAULT_CONNECTOR_TASK_COMMAND));
        containerDefinition.setCpu(DEFAULT_CONNECTOR_TASK_CPU);
        containerDefinition.setEntryPoint(Arrays.asList(DEFAULT_ECS_TASK_ENTRYPOINT));
        containerDefinition.setEnvironment(environment);
        containerDefinition.setEssential(DEFAULT_ECS_TASK_ESSENTIAL);
        containerDefinition.setImage(DEFAULT_CONNECTOR_TASK_IMAGE);
        containerDefinition.setMemory(DEFAULT_CONNECTOR_TASK_MEMORY);
        containerDefinition.setName(CONNECTOR_TASK_DEFINITION_NAME);

        // Create task definition request with container definition and name
        RegisterTaskDefinitionRequest request = new RegisterTaskDefinitionRequest().withContainerDefinitions(containerDefinition);
        request.setFamily(CONNECTOR_TASK_DEFINITION_NAME);

        // Send ECS register task definition request
        RegisterTaskDefinitionResult result = ecsClient.registerTaskDefinition(request);
        return result.getTaskDefinition().getTaskDefinitionArn();
    }

    /**
     * Create task definition for DynamoDB Table Copy Task and register it using the given ECS client to the default cluster
     *
     * @param ecsClient
     *            The ECS client to use to register the task definition
     * @param group
     *            The replication group from which to create the task definition
     * @param member
     *            The replication group member from which to create the task definition
     * @return The ARN of the newly registered task definition
     */
    private static String createAndRegisterTableCopyTaskDefinition(AmazonECS ecsClient, DynamoDBReplicationGroup group, DynamoDBReplicationGroupMember member,
        DynamoDBMetadataStorage metadata) throws Exception {
        // Get arn object from member arn string
        DynamoDBArn destinationArnObject = new DynamoDBArn().withArnString(member.getArn());
        DynamoDBArn sourceArnObject = new DynamoDBArn().withArnString(member.getTableCopyTask().getSourceTableArn());

        // Create environment list and elements
        List<KeyValuePair> environment = new ArrayList<KeyValuePair>();
        environment.add(new KeyValuePair().withName(DESTINATION_TABLE_LABEL).withValue(destinationArnObject.getTableName()));
        environment.add(new KeyValuePair().withName(DESTINATION_ENDPOINT_LABEL).withValue(member.getEndpoint()));
        environment.add(new KeyValuePair().withName(SOURCE_TABLE_LABEL).withValue(sourceArnObject.getTableName()));
        environment.add(new KeyValuePair().withName(SOURCE_ENDPOINT_LABEL).withValue(member.getTableCopyTask().getSourceTableEndpoint()));
        environment.add(new KeyValuePair().withName(SOURCE_READ_FRACTION_LABEL).withValue(DEFAULT_SOURCE_READ_FRACTION));
        environment.add(new KeyValuePair().withName(DESTINATION_WRITE_FRACTION_LABEL).withValue(DEFAULT_DESTINATION_WRITE_FRACTION));
        environment.add(new KeyValuePair().withName(REPLICATION_GROUP_MEMBER_LABEL).withValue(member.getArn()));
        environment.add(new KeyValuePair().withName(REPLICATION_GROUP_UUID_LABEL).withValue(group.getReplicationGroupUUID()));
        environment.add(new KeyValuePair().withName(METADATA_TABLE_ENDPOINT_LABEL).withValue(metadata.getMetadataTableEndpoint()));
        environment.add(new KeyValuePair().withName(METADATA_TABLE_NAME_LABEL).withValue(metadata.getMetadataTableName().toString()));
        environment.add(new KeyValuePair().withName(ECS_SERVICE_NAME_LABEL).withValue(
            getHashedServiceName(member.getTableCopyTask().getSourceTableArn(), member.getArn(), TABLECOPY_TASK_DEFINITION_NAME)));

        // Create container definition element and populate it with values
        ContainerDefinition containerDefinition = new ContainerDefinition();
        containerDefinition.setCommand(Arrays.asList(DEFAULT_TABLECOPY_TASK_COMMAND));
        containerDefinition.setCpu(DEFAULT_TABLECOPY_TASK_CPU);
        containerDefinition.setEntryPoint(Arrays.asList(DEFAULT_ECS_TASK_ENTRYPOINT));
        containerDefinition.setEnvironment(environment);
        containerDefinition.setEssential(DEFAULT_ECS_TASK_ESSENTIAL);
        containerDefinition.setImage(DEFAULT_TABLECOPY_TASK_IMAGE);
        containerDefinition.setMemory(DEFAULT_TABLECOPY_TASK_MEMORY);
        containerDefinition.setName(TABLECOPY_TASK_DEFINITION_NAME);

        // Create task definition request with container definition and name
        RegisterTaskDefinitionRequest request = new RegisterTaskDefinitionRequest().withContainerDefinitions(containerDefinition);
        request.setFamily(TABLECOPY_TASK_DEFINITION_NAME);

        // Send ECS register task definition request
        RegisterTaskDefinitionResult result = ecsClient.registerTaskDefinition(request);
        return result.getTaskDefinition().getTaskDefinitionArn();
    }
}
