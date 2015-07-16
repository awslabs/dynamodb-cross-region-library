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
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.TableCopyConstants;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.daemon.nanny.ProgressDaemon;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.Date;

/**
 *
 */
public class StdOutReaderDaemon extends ReaderDaemon {

    private static final Logger LOG = Logger.getLogger(StdOutReaderDaemon.class);

    private static final String initRegex = "^.* - info: Copying table [a-z][a-z]-[a-z]*-[0-2]:.* to"
            + " [a-z][a-z]-[a-z]*-[0-2]:.* \\(segments worker [0-9]*/[0-9]*\\)$";

    private static final String sampleRegex = "^.* - info: PID [0-9]* wrote [0-9]* items in [0-9]*\\.[0-9]* seconds"
            + " \\(Rate = [0-9]*\\.[0-9]* items/s, # of backoff = [0-9]*\\)$";

    private static final String segmentRegex = "^.* - info: Segment [0-9]* has finished";

    private static final String generalInfoRegex = "^.*info.*$";

    protected Date previousTimestamp;
    public StdOutReaderDaemon(TableCopyTracker tracker) {
        super(tracker);
        previousTimestamp = new Date();
    }

    protected boolean analyzeStream(String stream) {
        if (stream.matches(initRegex)) {
            return true;
        } else if (stream.matches(sampleRegex)) {
            Date now = new Date();
            boolean shouldSample = now.getTime() - previousTimestamp.getTime() > TableCopyConstants.MINUTE_IN_MILLIS;

            if (shouldSample) {
                previousTimestamp = now;
            }
            return shouldSample;
        } else if (stream.matches(segmentRegex)) {
            ProgressDaemon.incrementProgress();
            return true;
        } else if (stream.matches(generalInfoRegex)) {
            return false;
        } else {
            return true;
        }
    }

    protected void outputStream(String stream) {
        LOG.info(stream);
    }

    protected InputStream waitForStream() {
        InputStream stream = tracker.getStdout();
        while (stream == null)  {
            try {
                LOG.info(this.getClass().getSimpleName() + " waiting for out stream");
                Thread.sleep(ONE_SEC_IN_MILLIS);
            } catch (InterruptedException ie) {
                LOG.warn("Interrupted sleep waiting for stdout stream");
            }
            stream = tracker.getStdout();
        }

        return stream;
    }
}
