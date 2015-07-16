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

import java.util.concurrent.BlockingQueue;

public abstract class NannyDaemon implements Runnable {

    protected volatile boolean isAlive = true;
    protected BlockingQueue<NannyDaemon> pseudoSemaphore;

    public abstract void callback(TableCopyTracker tracker);

    public NannyDaemon withPseudoSemaphore(BlockingQueue<NannyDaemon> pseudoSemaphore) {
        this.pseudoSemaphore = pseudoSemaphore;
        return this;
    }

    @Override
    public void run() {}

    public void shutdown() {
        isAlive = false;
    }
}
