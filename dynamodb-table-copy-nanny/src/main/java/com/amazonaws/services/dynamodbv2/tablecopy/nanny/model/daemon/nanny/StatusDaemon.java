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
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TaskStatus;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TaskTracker;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.DynamoDBTableCopyNanny;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.TableCopyConstants;
import org.apache.log4j.Logger;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class StatusDaemon extends NannyDaemon {

    private static final long TASK_TRACKING_PERIOD = 10000L;
    private static final Logger LOG = Logger.getLogger(StatusDaemon.class);

    protected TaskTracker taskTracker;
    protected int numOfChecks;

    public StatusDaemon(TaskTracker taskTracker) {
        this.taskTracker = taskTracker;
        this.numOfChecks = 0;
    }

    /**
     * Status Tracker is scheduled to run on a TASK_TRACKING_PERIOD millisecond interval
     * This will poll the status given by the TaskTracker, which should be updated by the TableCopyClient.
     * Because TableCopyClient runs a callback, we just need to alert the thread there's no more work to be done.
     */
    @Override
    public void run() {
        LOG.debug("StatusDaemon is " + (isAlive ? "" : "not") + " alive");
        if (isAlive) {
            TaskStatus status = taskTracker.getStatus();
            logStatus(status);
            try {
                if (status.isFinished())  {
                    LOG.info("Status has reached terminal state, StatusDaemon signaling main thread");
                    pseudoSemaphore.put(this);
                }
                DynamoDBTableCopyNanny.threadpool.schedule(this, TASK_TRACKING_PERIOD, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                LOG.warn(ie);
                try {
                    DynamoDBTableCopyNanny.threadpool.schedule(this, TASK_TRACKING_PERIOD, TimeUnit.MILLISECONDS);
                } catch (RejectedExecutionException ree) {
                    LOG.warn("Threadpool has been shutdown, cannot run.", ree);
                }
            } catch (RejectedExecutionException ree) {
                LOG.warn("Threadpool has been shutdown, cannot run.", ree);
            }
        }
    }

    public void logStatus(TaskStatus status) {
        if (numOfChecks == TableCopyConstants.NUM_OF_CHECKS_BEFORE_LOG) {
            LOG.info("Task has status " + status.toString());
            numOfChecks = 0;
        } else {
            numOfChecks++;
        }
    }

    @Override
    public void callback(TableCopyTracker tracker) {}
}
