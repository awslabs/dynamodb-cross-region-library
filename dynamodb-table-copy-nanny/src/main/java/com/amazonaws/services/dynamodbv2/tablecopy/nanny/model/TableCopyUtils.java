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
package com.amazonaws.services.dynamodbv2.tablecopy.nanny.model;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.model.AutoScalingInstanceDetails;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingInstancesRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingInstancesResult;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBMetadataStorage;
import com.amazonaws.util.EC2MetadataUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Utilities methods for the TableCopy Main
 */
public class TableCopyUtils {

    private static final Logger LOG = Logger.getLogger(TableCopyUtils.class);
    private static final Set<DynamoDBReplicationGroupMemberStatus> incompleteTableCopyStatuses = new HashSet<>();
    static {
        incompleteTableCopyStatuses.add(DynamoDBReplicationGroupMemberStatus.WAITING);
        incompleteTableCopyStatuses.add(DynamoDBReplicationGroupMemberStatus.BOOTSTRAPPING);
        incompleteTableCopyStatuses.add(DynamoDBReplicationGroupMemberStatus.DELETING);
    }

    public static String getInstanceId() {
        try {
            return EC2MetadataUtils.getInstanceId();
        } catch (AmazonClientException ace) {
            return null;
        }
    }

    public static String getInstanceRegion() {
        try {
            return EC2MetadataUtils.getEC2InstanceRegion();
        } catch (AmazonClientException ace) {
            return null;
        }
    }

    public static String getAsgName(AmazonAutoScaling asg) {
        DescribeAutoScalingInstancesRequest request = new DescribeAutoScalingInstancesRequest()
                .withInstanceIds(getInstanceId());
        DescribeAutoScalingInstancesResult response = asg.describeAutoScalingInstances(request);

        List<AutoScalingInstanceDetails> details = response.getAutoScalingInstances();
        if (details.size() == 1) {
            for (AutoScalingInstanceDetails detail : details)
                return detail.getAutoScalingGroupName();
        }
        return null;
    }

    public static String calculateSegments(AmazonDynamoDB dynamoDB, String sourceTableName) {
        TableDescription description = dynamoDB.describeTable(sourceTableName).getTable();
        double partitionsBySize = Math.max(
                                Math.ceil(description.getTableSizeBytes() / TableCopyConstants.BYTES_PER_PARTITION),
                                TableCopyConstants.MIN_NUM_OF_PARTITION);

        long rcu = description.getProvisionedThroughput().getReadCapacityUnits();
        long wcu = description.getProvisionedThroughput().getWriteCapacityUnits();
        double partitionEstimateByIops = Math.ceil((rcu + TableCopyConstants.WCU_TO_IOPS * wcu) /
                                    TableCopyConstants.IOPS_PER_PARTITION);

        double partitionsByIops = Math.max(partitionEstimateByIops, TableCopyConstants.MIN_NUM_OF_PARTITION);

        double numOfPartitions = Math.max(partitionsByIops, partitionsBySize);

        int numOfSegments = new Double(numOfPartitions * TableCopyConstants.SEGMENTS_PER_PARTITION).intValue();

        return String.valueOf(numOfSegments);
    }

    /**
     * Update the coordinator metadata table with the provided replication group member status. status must be either
     * BOOTSTRAP_COMPLETE, BOOTSTRAP_FAILED, or BOOTSTRAP_CANCELED. Other processes may be modifying the status at the
     * same time, so we must be sure to handle those cases.
     *
     * The final status will be determined by the following algorithm:
     * 1) If RG is not in WAITING/BOOTSTRAPPING/DELETING, do nothing (some other process must have already wrapped up the task)
     * 2) If RG is in DELETING, set status to BOOTSTRAP_CANCELLED
     * 3) Attempt to write the RG status to status
     * 4) If this fails, then the version number must have changed since reading the status. Reread the status and start from (1)
     *
     * @param status
     * @param repGroupId
     * @param repGroupMember
     * @return wasSuccessful
     */
    public static boolean markReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus status, String repGroupId,
                                                    String repGroupMember) {
        DynamoDBMetadataStorage metadataStorage = DynamoDBMetadataStorage.getInstance();
        if (status == null) {
            throw new IllegalArgumentException("DynamoDBReplicationGroupMemberStatus cannot be null");
        }

        while (true) {
            try {
                DynamoDBReplicationGroup replicationGroup = metadataStorage.readReplicationGroup(repGroupId);
                Map<String, DynamoDBReplicationGroupMember> replicationGroupMembers = replicationGroup.getReplicationGroupMembers();
                DynamoDBReplicationGroupMember replicationGroupMember = replicationGroupMembers.get(repGroupMember);
                if (!incompleteTableCopyStatuses.contains(replicationGroupMember.getReplicationGroupMemberStatus())) {
                    return false;
                }

                DynamoDBReplicationGroup replicationGroupClone = new DynamoDBReplicationGroup(replicationGroup);
                Map<String, DynamoDBReplicationGroupMember> replicationGroupMembersClone = new HashMap<>(replicationGroupMembers);
                DynamoDBReplicationGroupMember replicationGroupMemberClone = new DynamoDBReplicationGroupMember(replicationGroupMember);

                if (DynamoDBReplicationGroupMemberStatus.DELETING.equals(replicationGroupMember.getReplicationGroupMemberStatus())) {
                    replicationGroupMemberClone.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_CANCELLED);
                } else {
                    replicationGroupMemberClone.setReplicationGroupMemberStatus(status);
                }
                replicationGroupMembersClone.put(repGroupMember, replicationGroupMemberClone);

                replicationGroupClone.setReplicationGroupMembers(replicationGroupMembersClone);

                DynamoDBReplicationGroup result = metadataStorage.compareAndWriteReplicationGroup(replicationGroup, replicationGroupClone);
                if (replicationGroupClone.equals(result)) {
                    return true;
                }
            } catch (IOException ioe) {
                LOG.warn(ioe);
                return false;
            }
        }
    }
}
