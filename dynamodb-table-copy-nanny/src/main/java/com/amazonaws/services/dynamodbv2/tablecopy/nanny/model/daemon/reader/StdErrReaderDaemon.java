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

import java.io.InputStream;

/**
 *
 */
public class StdErrReaderDaemon extends ReaderDaemon {

    private static final Logger LOG = Logger.getLogger(StdErrReaderDaemon.class);
    private static final boolean PROPAGATE_ALL_ERRORS = true;

    public StdErrReaderDaemon(TableCopyTracker tracker) {
        super(tracker);
    }

    protected boolean analyzeStream(String stream) {
        return PROPAGATE_ALL_ERRORS;
    }

    protected void outputStream(String stream) {
        LOG.fatal(stream);
    }

    protected InputStream waitForStream() {
        InputStream stream = tracker.getStderr();
        while (stream == null)  {
            try {
                LOG.info(this.getClass().getSimpleName() + " waiting for err stream");
                Thread.sleep(ONE_SEC_IN_MILLIS);
            } catch (InterruptedException ie) {
                LOG.warn("Interrupted sleep waiting for stdout stream");
            }
            stream = tracker.getStderr();
        }

        return stream;
    }
}
