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
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.TableCopyUtils;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.log4j.Logger;

public class TimeoutDaemon extends NannyDaemon {

    private static final Logger LOG = Logger.getLogger(TimeoutDaemon.class);
    private final String repGroupId;
    private final String repGroupMember;

    public TimeoutDaemon(String repGroupId, String repGroupMember) {
        this.repGroupId = repGroupId;
        this.repGroupMember = repGroupMember;
    }

    /**
     * TimeoutDaemon should be scheduled to run exactly once after a certain period of time.
     * All the Daemon will do is signal the main thread that the timeout has occurred before invoking the callback.
     */
    @Override
    public void run() {
       LOG.debug("TimeoutDaemon is " + (isAlive ? "" : "not") + "alive");
       if (isAlive) {
           try {
               LOG.info("Timeout reached, TimeoutDaemon signaling main thread");
               pseudoSemaphore.put(this);
           } catch (InterruptedException ie) {
               throw new RuntimeException("TimeoutDaemon interrupted while sleeping, shouldn't happen");
           }
        }
    }

    /**
     * Callback for cleanup actions from the TimeoutDaemon
     * Cancel the table copy task.
     * It should try to mark the ReplicationGroupMemberStatus as BOOTSTRAP_FAILED in the Metadata Table.
     */
    @Override
    public void callback(TableCopyTracker tracker) {
        LOG.info("Running callback for TimeoutDaemon");
        tracker.cancel();
        boolean updateSucceeded = TableCopyUtils.markReplicationGroupMemberStatus(
                DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_FAILED, repGroupId, repGroupMember);
        if (!updateSucceeded) {
            LOG.warn("TimeoutDaemon could not update the metadata table");
        }
    }
}
