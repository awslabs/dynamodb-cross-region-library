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
package com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.daemon.nanny;

import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTracker;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.DynamoDBTableCopyNanny;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.TableCopyConstants;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.TableCopyUtils;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBMetadataStorage;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class CancellationDaemon extends NannyDaemon {

    private static final long METADATA_POLLING_PERIOD = 10000L;
    private static final Logger LOG = Logger.getLogger(CancellationDaemon.class);

    private volatile boolean isCancelled = false;

    private final String repGroupId;
    private final String repGroupMember;
    private final DynamoDBMetadataStorage metadataStorage;

    protected int numOfChecks;

    public CancellationDaemon(String repGroupId, final String repGroupMember) {
        this.repGroupId = repGroupId;
        this.repGroupMember = repGroupMember;
        this.metadataStorage = DynamoDBMetadataStorage.getInstance();
        LOG.info("CancellationDaemon initialized with repGroupId:" + repGroupId + " and repGroupMember" + repGroupMember);
        this.numOfChecks = 0;
    }

    /**
     * CancellationDaemon will poll for the ReplicationGroupMember's Status from the metadata table
     * If the status is reported as DELETING, BOOTSTRAP_COMPLETE, or BOOTSTRAP_FAILED, then we signal the main thread.
     * If it's in DELETING, we add extra information about the state on whether or not to perform the callback.
     */
    @Override
    public void run() {
        LOG.debug("CancellationDaemon is " + (isAlive ? "" : "not") + " alive");
        if (isAlive) {
            try {
                DynamoDBReplicationGroup replicationGroup = metadataStorage.readReplicationGroup(repGroupId);
                DynamoDBReplicationGroupMember replicationGroupMember = replicationGroup.getReplicationGroupMembers()
                                                                                .get(repGroupMember);

                DynamoDBReplicationGroupMemberStatus status = replicationGroupMember.getReplicationGroupMemberStatus();
                logStatus(status);
                switch (status) {
                    case DELETING:
                        isCancelled = true;
                    case BOOTSTRAP_COMPLETE:
                    case BOOTSTRAP_FAILED:
                        LOG.info("Cancellation read " + status.toString() + ", CancellationDaemon signaling main thread");
                        pseudoSemaphore.put(this);
                        break;
                    default:
                        DynamoDBTableCopyNanny.threadpool.schedule(this, METADATA_POLLING_PERIOD, TimeUnit.MILLISECONDS);
                }
            } catch (IOException | InterruptedException ie) {
                LOG.warn(ie);
                try {
                    DynamoDBTableCopyNanny.threadpool.schedule(this, METADATA_POLLING_PERIOD, TimeUnit.MILLISECONDS);
                } catch (RejectedExecutionException ree) {
                    LOG.warn("Threadpool has been shutdown, cannot run.", ree);
                }
            } catch (RejectedExecutionException ree) {
                LOG.warn("Threadpool has been shutdown, cannot run.", ree);
            }
        }
    }

    public void logStatus(DynamoDBReplicationGroupMemberStatus status) {
        if (numOfChecks == TableCopyConstants.NUM_OF_CHECKS_BEFORE_LOG) {
            LOG.info("CancellationDaemon read replicationGroupMember with status:"
                    + status.toString());
            numOfChecks = 0;
        } else {
            numOfChecks++;
        }
    }
    /**
     * Cancellation Daemon callback should cancel the task, then mark the ReplicationGroupMember's status as BOOTSTRAP_CANCELLED..
     */
    @Override
    public void callback(TableCopyTracker tracker) {
        if (isCancelled) {
            LOG.info("CancellationDaemon is running callback");
            tracker.cancel();
            boolean updateSucceeded = TableCopyUtils.markReplicationGroupMemberStatus(
                    DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_CANCELLED, repGroupId, repGroupMember);
            if (!updateSucceeded) {
                LOG.warn("CancellationDaemon could not update the metadata table");
            }
        }
    }
}
