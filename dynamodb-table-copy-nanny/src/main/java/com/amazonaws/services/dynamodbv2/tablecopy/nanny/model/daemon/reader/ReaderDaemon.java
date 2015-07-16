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
package com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.daemon.reader;

import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTracker;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 *
 */
public abstract class ReaderDaemon implements Runnable {

    private static final Logger LOG = Logger.getLogger(ReaderDaemon.class);

    protected static final long ONE_SEC_IN_MILLIS = 1000L;

    protected volatile boolean isAlive = true;

    protected final TableCopyTracker tracker;

    protected ReaderDaemon(TableCopyTracker tracker) {
       this.tracker = tracker;
    }

    @Override
    public void run() {
        InputStream stream = waitForStream();
        readStream(stream);
    }

    protected void readStream(InputStream stream) {
        BufferedReader br = new BufferedReader(new InputStreamReader(stream));
        try {
            while (isAlive) {
                String line = br.readLine();
                if (line != null) {
                    if (analyzeStream(line)) {
                        outputStream(line);
                    }
                } else {
                    Thread.sleep(ONE_SEC_IN_MILLIS);
                }
            }

            br.close();
        } catch (IOException | InterruptedException ie) {
            LOG.warn(ie);
        }
    }

    protected abstract InputStream waitForStream();

    protected abstract boolean analyzeStream(String stream);

    protected abstract void outputStream(String stream);

    public void shutdown() {
        isAlive = false;
    }
}
