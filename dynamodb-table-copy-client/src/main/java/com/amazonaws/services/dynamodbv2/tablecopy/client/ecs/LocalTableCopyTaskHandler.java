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
package com.amazonaws.services.dynamodbv2.tablecopy.client.ecs;

import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.request.TableCopyRequest;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTaskHandler;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TaskStatus;

import java.io.IOException;
import java.io.InputStream;

/**
 * Handler for table copy tasks launched in a local process
 */
public class LocalTableCopyTaskHandler extends TableCopyTaskHandler {

    protected String[] commands;
    protected Process tableCopyProcess = null;
    protected InputStream stdout = null;
    protected InputStream stderr = null;

    public LocalTableCopyTaskHandler(TableCopyRequest request, String[] commands) {
        super(request);
        this.commands = commands;
    }

    public void tableCopyImpl() {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(commands);
            tableCopyProcess = processBuilder.start();
            stdout = tableCopyProcess.getInputStream();
            stderr = tableCopyProcess.getErrorStream();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

    }

    @Override
    public TaskStatus getStatus() {
        if (tableCopyProcess == null) {
            return TaskStatus.UNKNOWN;
        }
        try {
            int exitCode = tableCopyProcess.exitValue();
            return exitCode == 0 ? TaskStatus.COMPLETE : TaskStatus.UNRETRYABLE_FAILED;
        } catch (IllegalThreadStateException itse) {
            return TaskStatus.ACTIVE;
        }
    }

    public void shutdownImpl() {
        tableCopyProcess.destroy();
    }

    @Override
    public InputStream getInputStream() {
        if (stdout == null) {
            throw new IllegalStateException("Should only have stdout when process is running");
        }

        return stdout;
    }

    @Override
    public InputStream getErrorStream() {
        if (stderr == null) {
            throw new IllegalStateException("Should only have stderr when process is running");
        }

        return stderr;
    }
}
