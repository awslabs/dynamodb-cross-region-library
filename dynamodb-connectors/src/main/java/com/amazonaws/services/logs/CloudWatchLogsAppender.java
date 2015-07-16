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
package com.amazonaws.services.logs;

import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.Validate;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.streams.connectors.StatusCodes;
import com.amazonaws.services.logs.model.CreateLogGroupRequest;
import com.amazonaws.services.logs.model.CreateLogStreamRequest;
import com.amazonaws.services.logs.model.DataAlreadyAcceptedException;
import com.amazonaws.services.logs.model.DescribeLogGroupsRequest;
import com.amazonaws.services.logs.model.DescribeLogGroupsResult;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.LogStream;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import com.amazonaws.services.logs.model.PutLogEventsResult;
import com.amazonaws.services.logs.model.ResourceNotFoundException;
import com.amazonaws.services.logs.model.ServiceUnavailableException;
import com.amazonaws.util.EC2MetadataUtils;
import com.amazonaws.util.EC2MetadataUtils.InstanceInfo;
import com.google.common.annotations.VisibleForTesting;

/**
 * A Log4J 1.2 Appender that emits LogEvents directly to CloudWatch Logs. LogEvents are batched in a queue until one of the following thresholds are met:
 * <ul>
 * <li>The number of LogEvents queued exceeds {@value #DEFAULT_EVENT_COUNT_THRESHOLD}.</li>
 * <li>The total size of the LogEvents queued exceeds {@value #DEFAULT_EVENTS_SIZE_THRESHOLD} bytes.</li>
 * <li>It has been {@value #DEFAULT_TIME_THRESHOLD} milliseconds since the last time logs were published to CloudWatch</li>
 * </ul>
 * After any of the conditions are met, the appender publishes a batch of logs to CloudWatch.
 */
public class CloudWatchLogsAppender extends AppenderSkeleton {

    private volatile AWSLogs awsLogs;
    private String logGroupName;
    private String logStreamName;
    private String logStreamNameSuffix = LogStreamNameSuffix.NONE.toString();
    private volatile String nextSequenceToken = null;
    private volatile long lastTimePublished = System.nanoTime();
    private final Queue<InputLogEvent> eventQueue = new LinkedList<InputLogEvent>();
    private volatile int eventsQueueSize = 0;
    private volatile boolean shutdown = false;

    /**
     * The format String for an EC2 instance ARN. The substitutions are region, account, and instance id respectively.
     */
    public static final String EC2_INSTANCE_FORMAT = "%s/%s/%s";

    public static final int ONE_MEGABYTE = 1024 * 1024;

    /**
     * The maximum number of log events in a batch is 10,000. http://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
     */
    public static final int DEFAULT_EVENT_COUNT_THRESHOLD = 10000;
    /**
     * The maximum time in milliseconds to wait before publishing logs to CloudWatch.
     */
    public static final long DEFAULT_TIME_THRESHOLD = 1000L;
    /**
     * The maximum batch size is 1,048,576 bytes, and this size is calculated as the sum of all event messages in UTF-8, plus 26 bytes for each log event.
     * http://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
     * */
    public static final int DEFAULT_EVENTS_SIZE_THRESHOLD = ONE_MEGABYTE;
    public static final int CW_LOG_EVENT_OVERHEAD = 26;

    private final ScheduledExecutorService publisherService = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> logPublisherFuture;

    enum LogStreamNameSuffix {
        /**
         * Does not modify the provided log stream name.
         */
        NONE,
        /**
         * Appends a random UUID to the log stream name.
         */
        UUID,
        /**
         * Appends the EC2 instance info to the log stream name.
         */
        EC2_INSTANCE_INFO,
        /**
         * Appends the host IP address to the log stream name.
         */
        IP_ADDRESS
    }

    public CloudWatchLogsAppender(String logGroupName, String logStreamName, LogStreamNameSuffix suffix, Layout layout, AWSLogs awsLogs) {
        Validate.notEmpty(logGroupName, "Must specify a log group name");
        Validate.notEmpty(logStreamName, "Must specify a log stream name");
        Validate.notNull(awsLogs, "Must specify an AWSLog instance");
        Validate.notNull(layout, "Must specify a layout");

        this.awsLogs = awsLogs;
        this.logGroupName = logGroupName;
        this.logStreamName = logStreamName;

        regionalize();
        addLogStreamSuffix();

        setLayout(layout);

        createLogGroupIfNotExists();
        createLogStreamIfNotExists();

        startPublisher();
    }

    private void startPublisher() {
        final CountDownLatch runLatch = new CountDownLatch(1);
        logPublisherFuture = publisherService.scheduleWithFixedDelay(new LogPublisher(runLatch), 0L /* Initial Delay */,
            Math.max(1, DEFAULT_TIME_THRESHOLD / 4) /* period */, TimeUnit.MILLISECONDS); // TODO configurable threshold
        try {
            runLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Updates LogStreamName based on the logStreamNameSuffix option. In the case that an invalid option is selected, the suffix defaults to NONE.
     */
    private void addLogStreamSuffix() {
        LogStreamNameSuffix suffixType = LogStreamNameSuffix.valueOf(logStreamNameSuffix);
        String suffix = "";
        switch (suffixType) {
            case EC2_INSTANCE_INFO:
                try {
                    InstanceInfo instanceInfo = EC2MetadataUtils.getInstanceInfo();
                    suffix += String.format(EC2_INSTANCE_FORMAT, instanceInfo.getRegion(), instanceInfo.getAccountId(), instanceInfo.getInstanceId());
                } catch (AmazonClientException e) {
                    // Not an EC2 instance or EC2, default to NONE
                }
                break;
            case IP_ADDRESS:
                try {
                    suffix += java.net.InetAddress.getLocalHost().toString();
                } catch (UnknownHostException e) {
                    // Cannot get IP address, default to NONE
                }
                break;
            case NONE:
                break;
            case UUID:
                suffix += UUID.randomUUID().toString();
                break;
        }
        setLogStreamName(logStreamName + (suffix.isEmpty() ? suffix : " " + suffix));
    }

    /**
     * If this is running on an EC2 instance, it sets the region for the CloudWatch Logs client to the current region to minimize latency.
     */
    private void regionalize() {
        final Region region = Regions.getCurrentRegion();
        if (null != region) {
            awsLogs.setRegion(region);
        }
    }

    @VisibleForTesting
    private void createLogGroupIfNotExists() {
        DescribeLogGroupsRequest describeLogGroupsRequest = new DescribeLogGroupsRequest().withLogGroupNamePrefix(logGroupName).withLimit(1);
        DescribeLogGroupsResult describeLogGroupsResult = awsLogs.describeLogGroups(describeLogGroupsRequest);
        // Log group names are sorted in ASCII order, so if the results are non-empty, the log group should be first
        if (!describeLogGroupsResult.getLogGroups().isEmpty() && describeLogGroupsResult.getLogGroups().get(0).getLogGroupName().equals(logGroupName)) {
            return; // Log group exists
        }
        CreateLogGroupRequest createLogGroupRequest = new CreateLogGroupRequest().withLogGroupName(logGroupName);
        awsLogs.createLogGroup(createLogGroupRequest);
    }

    @VisibleForTesting
    private void createLogStreamIfNotExists() {
        DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest().withLogGroupName(logGroupName).withLogStreamNamePrefix(
            logStreamName);
        DescribeLogStreamsResult describeLogStreamsResult = awsLogs.describeLogStreams(describeLogStreamsRequest);
        // Log stream names are sorted in ASCII order, so if the results are non-empty, the log stream should be first
        if (!describeLogStreamsResult.getLogStreams().isEmpty() && describeLogStreamsResult.getLogStreams().get(0).getLogStreamName().equals(logStreamName)) {
            nextSequenceToken = describeLogStreamsResult.getLogStreams().get(0).getUploadSequenceToken();
            return; // Log group exists
        }
        CreateLogStreamRequest createLogStreamRequest = new CreateLogStreamRequest().withLogGroupName(logGroupName).withLogStreamName(logStreamName);
        awsLogs.createLogStream(createLogStreamRequest);
    }

    /**
     * Empty constructor to allow log4j property file to define parameters
     */
    public CloudWatchLogsAppender() {
    }

    public CloudWatchLogsAppender(String logGroupName, String logStreamName, LogStreamNameSuffix suffix, Layout layout) {
        this(logGroupName, logStreamName, suffix, layout, new AWSLogsClient());
    }

    public CloudWatchLogsAppender(String logGroupName, String logStreamName, LogStreamNameSuffix suffix, Layout layout, AWSCredentials awsCredentials) {
        this(logGroupName, logStreamName, suffix, layout, new AWSLogsClient(awsCredentials));
    }

    public CloudWatchLogsAppender(String logGroupName, String logStreamName, LogStreamNameSuffix suffix, Layout layout,
        AWSCredentialsProvider awsCredentialsProvider) {
        this(logGroupName, logStreamName, suffix, layout, new AWSLogsClient(awsCredentialsProvider));
    }

    public CloudWatchLogsAppender(String logGroupName, String logStreamName, LogStreamNameSuffix suffix, Layout layout, ClientConfiguration clientConfiguration) {
        this(logGroupName, logStreamName, suffix, layout, new AWSLogsClient(clientConfiguration));
    }

    public CloudWatchLogsAppender(String logGroupName, String logStreamName, LogStreamNameSuffix suffix, Layout layout, AWSCredentials awsCredentials,
        ClientConfiguration clientConfiguration) {
        this(logGroupName, logStreamName, suffix, layout, new AWSLogsClient(awsCredentials, clientConfiguration));
    }

    public CloudWatchLogsAppender(String logGroupName, String logStreamName, LogStreamNameSuffix suffix, Layout layout,
        AWSCredentialsProvider awsCredentialsProvider, ClientConfiguration clientConfiguration) {
        this(logGroupName, logStreamName, suffix, layout, new AWSLogsClient(awsCredentialsProvider, clientConfiguration));
    }

    public CloudWatchLogsAppender(String logGroupName, String logStreamName, LogStreamNameSuffix suffix, Layout layout,
        AWSCredentialsProvider awsCredentialsProvider, ClientConfiguration clientConfiguration, RequestMetricCollector requestMetricCollector) {
        this(logGroupName, logStreamName, suffix, layout, new AWSLogsClient(awsCredentialsProvider, clientConfiguration, requestMetricCollector));
    }

    @Override
    public void activateOptions() {
        // validate parameters
        Validate.notEmpty(logGroupName);
        Validate.notEmpty(logStreamName);

        // default AWSLogsClient
        awsLogs = new AWSLogsClient();

        regionalize();
        addLogStreamSuffix();

        createLogGroupIfNotExists();
        createLogStreamIfNotExists();

        startPublisher();
    }

    @Override
    public void close() {
        synchronized (this) { // After entering this block, all future append calls will be no-ops
            shutdown = true;
            publisherService.shutdown();
            logPublisherFuture.cancel(true /* mayInterruptIfRunning */);
            if (!eventQueue.isEmpty()) {
                publishLogs(copyEventQueue());
            }
        }

        boolean terminated = false;
        boolean interrupted = false;
        try {
            while (!terminated) {
                try {
                    publisherService.shutdownNow();
                    terminated = publisherService.awaitTermination(DEFAULT_TIME_THRESHOLD, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            // flush remaining logs before exiting
            publishLogs(eventQueue);
            awsLogs.shutdown();
        }
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    @Override
    synchronized protected void append(LoggingEvent event) {
        if (shutdown || null == event) {
            return;
        }
        final int previousQueueSize = eventsQueueSize;
        final Layout layout = getLayout();
        if (null == layout) {
            errorHandler.error(getClass().getSimpleName() + " requires a layout");
            return;
        }
        final InputLogEvent logEvent = new InputLogEvent().withMessage(layout.format(event)).withTimestamp(event.getTimeStamp());
        final long eventSize = getLogEventSize(logEvent);
        if (previousQueueSize + eventSize > DEFAULT_EVENTS_SIZE_THRESHOLD) {
            publishLogs(copyEventQueue());
        }
        eventsQueueSize += eventSize;
        eventQueue.add(logEvent);
        // flush right away if log level is greater than or equal to ERROR
        if (event.getLevel().isGreaterOrEqual(Level.ERROR) || eventQueue.size() == DEFAULT_EVENT_COUNT_THRESHOLD) {
            publishLogs(copyEventQueue());
        }
    }

    private Queue<InputLogEvent> copyEventQueue() {
        Queue<InputLogEvent> eventQueueCopy = new LinkedList<InputLogEvent>(eventQueue);
        eventQueue.clear();
        eventsQueueSize = 0;
        return eventQueueCopy;
    }

    private static int getLogEventSize(InputLogEvent logEvent) {
        return CW_LOG_EVENT_OVERHEAD + logEvent.getMessage().getBytes(StandardCharsets.UTF_8).length;
    }

    public class LogPublisher implements Runnable {

        private final CountDownLatch latch;

        public LogPublisher(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            if (latch.getCount() > 0) {
                latch.countDown(); // Only required on first execution
            }
            synchronized (CloudWatchLogsAppender.this) {
                if (!shutdown && !eventQueue.isEmpty() && TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastTimePublished) > DEFAULT_TIME_THRESHOLD) {
                    publishLogs(copyEventQueue());
                }
            }
        }

    }

    private void publishLogs(Queue<InputLogEvent> events) {
        PutLogEventsRequest request = new PutLogEventsRequest().withLogGroupName(logGroupName).withLogStreamName(logStreamName)
            .withSequenceToken(nextSequenceToken).withLogEvents(events);
        while (!Thread.currentThread().isInterrupted()) {
            try {
                PutLogEventsResult result = awsLogs.putLogEvents(request);
                nextSequenceToken = result.getNextSequenceToken();
                break;
            } catch (DataAlreadyAcceptedException e) {
                updateToken();
                continue; // retry
            } catch (ServiceUnavailableException e) {
                continue; // retry
            } catch (AmazonClientException e) {
                errorHandler.error("Exception encountered publishing logs to CloudWatch: " + request, e, StatusCodes.EIO);
                break;
            }
        }
        lastTimePublished = System.nanoTime();
    }

    private void updateToken() {
        DescribeLogStreamsRequest request = new DescribeLogStreamsRequest().withLogGroupName(logGroupName).withLogStreamNamePrefix(logStreamName);
        DescribeLogStreamsResult result = awsLogs.describeLogStreams(request);
        if (!result.getLogStreams().isEmpty()) {
            LogStream logStream = result.getLogStreams().get(0);
            if (logStreamName.equals(logStream.getLogStreamName())) {
                nextSequenceToken = logStream.getUploadSequenceToken();
            } else {
                throw new ResourceNotFoundException(logStreamName + " not found in log group: " + logGroupName);
            }
        }
    }

    public void setLogGroupName(String logGroupName) {
        this.logGroupName = logGroupName;
    }

    public void setLogStreamName(String logStreamName) {
        this.logStreamName = logStreamName;
    }

    public void setLogStreamNameSuffix(String logStreamNameSuffix) {
        this.logStreamNameSuffix = logStreamNameSuffix;
    }

    public String getLogGroupName() {
        return logGroupName;
    }

    public String getLogStreamName() {
        return logStreamName;
    }

    public String getLogStreamNameSuffix() {
        return logStreamNameSuffix;
    }

}
