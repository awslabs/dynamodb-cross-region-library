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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.getCurrentArguments;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.expectLastCall;
import static org.powermock.api.easymock.PowerMock.expectNew;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.easymock.Capture;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.logs.CloudWatchLogsAppender.LogPublisher;
import com.amazonaws.services.logs.CloudWatchLogsAppender.LogStreamNameSuffix;
import com.amazonaws.services.logs.model.CreateLogGroupRequest;
import com.amazonaws.services.logs.model.CreateLogStreamRequest;
import com.amazonaws.services.logs.model.DescribeLogGroupsRequest;
import com.amazonaws.services.logs.model.DescribeLogGroupsResult;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.LogGroup;
import com.amazonaws.services.logs.model.LogStream;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import com.amazonaws.services.logs.model.PutLogEventsResult;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CloudWatchLogsAppender.class, LogPublisher.class, AWSLogsClient.class, Regions.class})
@PowerMockIgnore("org.apache.log4j.*")
public class CloudWatchLogsAppenderTests {

    public static final String FIRST_SEQUENCE_TOKEN = "FIRST_SEQUENCE_TOKEN";
    public static final String SECOND_SEQUENCE_TOKEN = "SECOND_SEQUENCE_TOKEN";
    public static final String THIRD_SEQUENCE_TOKEN = "THIRD_SEQUENCE_TOKEN";
    public static final String LOG_GROUP_NAME = "testLogGroup";
    public static final String LOG_STREAM_NAME = "testLogStream";

    public static final LogGroup NEW_LOG_GROUP = new LogGroup().withArn("arn:aws:logs:us-east-1:123456654321:log-group:testLogGroup:*")
        .withCreationTime(System.currentTimeMillis()).withMetricFilterCount(0).withLogGroupName(LOG_GROUP_NAME).withStoredBytes(0L);
    public static final LogStream NEW_LOG_STREAM = new LogStream()
        .withArn("arn:aws:logs:us-east-1:123456654321:log-group:testLogGroup:log-stream:testLogStream").withCreationTime(System.currentTimeMillis())
        .withLogStreamName(LOG_STREAM_NAME).withStoredBytes(0L);
    public static final LogStream EXISTING_LOG_STREAM = new LogStream()
        .withArn("arn:aws:logs:us-east-1:123456654321:log-group:testLogGroup:log-stream:testLogStream").withCreationTime(System.currentTimeMillis())
        .withLogStreamName(LOG_STREAM_NAME).withStoredBytes(0L).withFirstEventTimestamp(System.currentTimeMillis())
        .withLastEventTimestamp(System.currentTimeMillis()).withLastIngestionTime(System.currentTimeMillis()).withUploadSequenceToken(FIRST_SEQUENCE_TOKEN);
    public static final Layout LAYOUT = new PatternLayout("%m");
    public static final List<LogGroup> EMPTY_LOG_GROUPS = Collections.emptyList();
    public static final List<LogStream> EMPTY_LOG_STREAMS = Collections.emptyList();

    public static final AWSCredentialsProvider MOCK_AWS_CREDENTIALS_PROVIDER = new AWSCredentialsProvider() {
        @Override
        public void refresh() {
        }

        @Override
        public AWSCredentials getCredentials() {
            return new BasicAWSCredentials("DUMMY_ACCESS_KEY", "DUMMY_SECRET_KEY");
        }
    };

    @Before
    public void setup() {
        mockStatic(Regions.class);
        Regions.getCurrentRegion();
        expectLastCall().andReturn(null);
        replay(Regions.class);
    }

    private IAnswer<LogPublisher> getLogPublisherConstructorAnswer(final LogPublisher mockPublisher) {
        return new IAnswer<LogPublisher>() {
            @Override
            public LogPublisher answer() throws Throwable {
                return mockPublisher;
            }
        };
    }

    private IAnswer<DescribeLogGroupsResult> getLogGroupDoesNotExistAnswer() {
        return new IAnswer<DescribeLogGroupsResult>() {
            @Override
            public DescribeLogGroupsResult answer() throws Throwable {
                DescribeLogGroupsRequest request = (DescribeLogGroupsRequest) getCurrentArguments()[0];
                assertTrue(LOG_GROUP_NAME.equals(request.getLogGroupNamePrefix()));
                return new DescribeLogGroupsResult().withLogGroups(EMPTY_LOG_GROUPS).withNextToken(null);
            }
        };
    }

    private IAnswer<Void> getCreateLogGroupAnswer() {
        return new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                CreateLogGroupRequest request = (CreateLogGroupRequest) getCurrentArguments()[0];
                assertTrue(LOG_GROUP_NAME.equals(request.getLogGroupName()));
                return null;
            }
        };
    }

    private IAnswer<? extends Object> getMockPublisherRunAnswer(final Capture<CountDownLatch> latch) {
        return new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                latch.getValue().countDown();
                return null;
            }
        };
    }

    private IAnswer<DescribeLogGroupsResult> getLogGroupExistsAnswer() {
        return new IAnswer<DescribeLogGroupsResult>() {
            @Override
            public DescribeLogGroupsResult answer() throws Throwable {
                DescribeLogGroupsRequest request = (DescribeLogGroupsRequest) getCurrentArguments()[0];
                assertTrue(LOG_GROUP_NAME.equals(request.getLogGroupNamePrefix()));
                return new DescribeLogGroupsResult().withLogGroups(NEW_LOG_GROUP).withNextToken(null);
            }
        };
    }

    private IAnswer<DescribeLogStreamsResult> getLogStreamExistsAnswer() {
        return new IAnswer<DescribeLogStreamsResult>() {
            @Override
            public DescribeLogStreamsResult answer() throws Throwable {
                DescribeLogStreamsRequest request = (DescribeLogStreamsRequest) getCurrentArguments()[0];
                assertTrue(LOG_GROUP_NAME.equals(request.getLogGroupName()));
                assertTrue(LOG_STREAM_NAME.equals(request.getLogStreamNamePrefix()));
                return new DescribeLogStreamsResult().withLogStreams(EXISTING_LOG_STREAM).withNextToken(null);
            }
        };
    }

    private IAnswer<DescribeLogStreamsResult> getLogStreamDoesNotExistAnswer() {
        return new IAnswer<DescribeLogStreamsResult>() {
            @Override
            public DescribeLogStreamsResult answer() throws Throwable {
                DescribeLogStreamsRequest request = (DescribeLogStreamsRequest) getCurrentArguments()[0];
                assertTrue(LOG_GROUP_NAME.equals(request.getLogGroupName()));
                assertTrue(LOG_STREAM_NAME.equals(request.getLogStreamNamePrefix()));
                return new DescribeLogStreamsResult().withLogStreams(EMPTY_LOG_STREAMS).withNextToken(null);
            }
        };
    }

    private IAnswer<Void> getCreateLogStreamAnswer() {
        return new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                CreateLogStreamRequest request = (CreateLogStreamRequest) getCurrentArguments()[0];
                assertTrue(LOG_GROUP_NAME.equals(request.getLogGroupName()));
                assertTrue(LOG_STREAM_NAME.equals(request.getLogStreamName()));
                return null;
            }
        };
    }

    private IAnswer<PutLogEventsResult> getPutLogEventsAnswer() {
        return new IAnswer<PutLogEventsResult>() {
            @Override
            public PutLogEventsResult answer() throws Throwable {
                return new PutLogEventsResult().withNextSequenceToken(SECOND_SEQUENCE_TOKEN);
            }
        };
    }

    @Test
    public void testLogGroupDoesNotExist() throws Exception {
        final LogPublisher mockPublisher = createMock(LogPublisher.class);
        AWSLogsClient mockClient = createMock(AWSLogsClient.class);
        expectNew(AWSLogsClient.class).andStubReturn(mockClient);
        final Capture<CountDownLatch> latch = new Capture<CountDownLatch>();
        expectNew(LogPublisher.class, capture(latch)).andAnswer(getLogPublisherConstructorAnswer(mockPublisher));
        mockClient.describeLogGroups(anyObject(DescribeLogGroupsRequest.class));
        expectLastCall().andAnswer(getLogGroupDoesNotExistAnswer());
        mockClient.createLogGroup(anyObject(CreateLogGroupRequest.class));
        expectLastCall().andAnswer(getCreateLogGroupAnswer());
        mockClient.describeLogStreams(anyObject(DescribeLogStreamsRequest.class));
        expectLastCall().andAnswer(getLogStreamDoesNotExistAnswer());
        mockClient.createLogStream(anyObject(CreateLogStreamRequest.class));
        expectLastCall().andAnswer(getCreateLogStreamAnswer());
        mockClient.shutdown();
        expectLastCall();
        mockPublisher.run();
        expectLastCall().andAnswer(getMockPublisherRunAnswer(latch));
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(getPutLogEventsAnswer());
        replayAll();
        CloudWatchLogsAppender appender = new CloudWatchLogsAppender(LOG_GROUP_NAME, LOG_STREAM_NAME, LogStreamNameSuffix.NONE, LAYOUT);
        appender.close();
        verifyAll();
    }

    @Test
    public void testLogGroupExistsLogStreamDoesNotExist() throws Exception {
        final LogPublisher mockPublisher = createMock(LogPublisher.class);
        AWSLogsClient mockClient = createMock(AWSLogsClient.class);
        expectNew(AWSLogsClient.class).andStubReturn(mockClient);
        final Capture<CountDownLatch> latch = new Capture<CountDownLatch>();
        expectNew(LogPublisher.class, capture(latch)).andAnswer(getLogPublisherConstructorAnswer(mockPublisher));
        mockClient.describeLogGroups(anyObject(DescribeLogGroupsRequest.class));
        expectLastCall().andAnswer(getLogGroupExistsAnswer());
        mockClient.describeLogStreams(anyObject(DescribeLogStreamsRequest.class));
        expectLastCall().andAnswer(getLogStreamDoesNotExistAnswer());
        mockClient.createLogStream(anyObject(CreateLogStreamRequest.class));
        expectLastCall().andAnswer(getCreateLogStreamAnswer());
        mockClient.shutdown();
        expectLastCall();
        mockPublisher.run();
        expectLastCall().andAnswer(getMockPublisherRunAnswer(latch));
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(getPutLogEventsAnswer());
        replayAll();
        CloudWatchLogsAppender appender = new CloudWatchLogsAppender(LOG_GROUP_NAME, LOG_STREAM_NAME, LogStreamNameSuffix.NONE, LAYOUT);
        appender.close();
        verifyAll();
    }

    @Test
    public void testLogGroupExistsLogStreamExists() throws Exception {
        LogPublisher mockPublisher = createMock(LogPublisher.class);
        AWSLogsClient mockClient = createMock(AWSLogsClient.class);
        expectNew(AWSLogsClient.class).andStubReturn(mockClient);
        final Capture<CountDownLatch> latch = new Capture<CountDownLatch>();
        expectNew(LogPublisher.class, capture(latch)).andAnswer(getLogPublisherConstructorAnswer(mockPublisher));
        mockClient.describeLogGroups(anyObject(DescribeLogGroupsRequest.class));
        expectLastCall().andAnswer(getLogGroupExistsAnswer());
        mockClient.describeLogStreams(anyObject(DescribeLogStreamsRequest.class));
        expectLastCall().andAnswer(getLogStreamExistsAnswer());
        mockClient.shutdown();
        expectLastCall();
        mockPublisher.run();
        expectLastCall().andAnswer(getMockPublisherRunAnswer(latch));
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(getPutLogEventsAnswer());

        replayAll();
        CloudWatchLogsAppender appender = new CloudWatchLogsAppender(LOG_GROUP_NAME, LOG_STREAM_NAME, LogStreamNameSuffix.NONE, LAYOUT);
        appender.close();
        verifyAll();
    }

    @Test
    public void testCloudWatchLogsAppenderStringStringLayout() throws Exception {
        LogPublisher mockPublisher = createMock(LogPublisher.class);
        AWSLogsClient mockClient = createMock(AWSLogsClient.class);
        expectNew(AWSLogsClient.class).andStubReturn(mockClient);
        final Capture<CountDownLatch> latch = new Capture<CountDownLatch>();
        expectNew(LogPublisher.class, capture(latch)).andAnswer(getLogPublisherConstructorAnswer(mockPublisher));
        mockClient.describeLogGroups(anyObject(DescribeLogGroupsRequest.class));
        expectLastCall().andAnswer(getLogGroupExistsAnswer());
        mockClient.describeLogStreams(anyObject(DescribeLogStreamsRequest.class));
        expectLastCall().andAnswer(getLogStreamExistsAnswer());
        mockClient.shutdown();
        expectLastCall();
        mockPublisher.run();
        expectLastCall().andAnswer(getMockPublisherRunAnswer(latch));
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(getPutLogEventsAnswer());
        replayAll();
        CloudWatchLogsAppender appender = new CloudWatchLogsAppender(LOG_GROUP_NAME, LOG_STREAM_NAME, LogStreamNameSuffix.NONE, LAYOUT);
        appender.close();
        verifyAll();
    }

    @Test
    public void testCloudWatchLogsAppenderStringStringLayoutAWSCredentials() throws Exception {
        LogPublisher mockPublisher = createMock(LogPublisher.class);
        AWSLogsClient mockClient = createMock(AWSLogsClient.class);
        expectNew(AWSLogsClient.class, new Class<?>[] {AWSCredentials.class}, anyObject(AWSCredentials.class)).andStubReturn(mockClient);
        final Capture<CountDownLatch> latch = new Capture<CountDownLatch>();
        expectNew(LogPublisher.class, capture(latch)).andAnswer(getLogPublisherConstructorAnswer(mockPublisher));
        mockClient.describeLogGroups(anyObject(DescribeLogGroupsRequest.class));
        expectLastCall().andAnswer(getLogGroupExistsAnswer());
        mockClient.describeLogStreams(anyObject(DescribeLogStreamsRequest.class));
        expectLastCall().andAnswer(getLogStreamExistsAnswer());
        mockClient.shutdown();
        expectLastCall();
        mockPublisher.run();
        expectLastCall().andAnswer(getMockPublisherRunAnswer(latch));
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(getPutLogEventsAnswer());
        replayAll();
        CloudWatchLogsAppender appender = new CloudWatchLogsAppender(LOG_GROUP_NAME, LOG_STREAM_NAME, LogStreamNameSuffix.NONE, LAYOUT,
            MOCK_AWS_CREDENTIALS_PROVIDER.getCredentials());
        appender.close();
        verifyAll();
    }

    @Test
    public void testCloudWatchLogsAppenderStringStringLayoutAWSCredentialsProvider() throws Exception {
        LogPublisher mockPublisher = createMock(LogPublisher.class);
        AWSLogsClient mockClient = createMock(AWSLogsClient.class);
        expectNew(AWSLogsClient.class, new Class<?>[] {AWSCredentialsProvider.class}, anyObject(AWSCredentialsProvider.class)).andStubReturn(mockClient);
        final Capture<CountDownLatch> latch = new Capture<CountDownLatch>();
        expectNew(LogPublisher.class, capture(latch)).andAnswer(getLogPublisherConstructorAnswer(mockPublisher));
        mockClient.describeLogGroups(anyObject(DescribeLogGroupsRequest.class));
        expectLastCall().andAnswer(getLogGroupExistsAnswer());
        mockClient.describeLogStreams(anyObject(DescribeLogStreamsRequest.class));
        expectLastCall().andAnswer(getLogStreamExistsAnswer());
        mockClient.shutdown();
        expectLastCall();
        mockPublisher.run();
        expectLastCall().andAnswer(getMockPublisherRunAnswer(latch));
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(getPutLogEventsAnswer());
        replayAll();
        CloudWatchLogsAppender appender = new CloudWatchLogsAppender(LOG_GROUP_NAME, LOG_STREAM_NAME, LogStreamNameSuffix.NONE, LAYOUT,
            MOCK_AWS_CREDENTIALS_PROVIDER);
        appender.close();
        verifyAll();
    }

    @Test
    public void testCloudWatchLogsAppenderStringStringLayoutClientConfiguration() throws Exception {
        LogPublisher mockPublisher = createMock(LogPublisher.class);
        AWSLogsClient mockClient = createMock(AWSLogsClient.class);
        expectNew(AWSLogsClient.class, new Class<?>[] {ClientConfiguration.class}, anyObject(ClientConfiguration.class)).andStubReturn(mockClient);
        final Capture<CountDownLatch> latch = new Capture<CountDownLatch>();
        expectNew(LogPublisher.class, capture(latch)).andAnswer(getLogPublisherConstructorAnswer(mockPublisher));
        mockClient.describeLogGroups(anyObject(DescribeLogGroupsRequest.class));
        expectLastCall().andAnswer(getLogGroupExistsAnswer());
        mockClient.describeLogStreams(anyObject(DescribeLogStreamsRequest.class));
        expectLastCall().andAnswer(getLogStreamExistsAnswer());
        mockClient.shutdown();
        expectLastCall();
        mockPublisher.run();
        expectLastCall().andAnswer(getMockPublisherRunAnswer(latch));
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(getPutLogEventsAnswer());
        replayAll();
        CloudWatchLogsAppender appender = new CloudWatchLogsAppender(LOG_GROUP_NAME, LOG_STREAM_NAME, LogStreamNameSuffix.NONE, LAYOUT,
            new ClientConfiguration());
        appender.close();
        verifyAll();
    }

    @Test
    public void testCloudWatchLogsAppenderStringStringLayoutAWSCredentialsClientConfiguration() throws Exception {
        LogPublisher mockPublisher = createMock(LogPublisher.class);
        AWSLogsClient mockClient = createMock(AWSLogsClient.class);
        expectNew(AWSLogsClient.class, new Class<?>[] {AWSCredentials.class, ClientConfiguration.class}, anyObject(AWSCredentials.class),
            anyObject(ClientConfiguration.class)).andStubReturn(mockClient);
        final Capture<CountDownLatch> latch = new Capture<CountDownLatch>();
        expectNew(LogPublisher.class, capture(latch)).andAnswer(getLogPublisherConstructorAnswer(mockPublisher));
        mockClient.describeLogGroups(anyObject(DescribeLogGroupsRequest.class));
        expectLastCall().andAnswer(getLogGroupExistsAnswer());
        mockClient.describeLogStreams(anyObject(DescribeLogStreamsRequest.class));
        expectLastCall().andAnswer(getLogStreamExistsAnswer());
        mockClient.shutdown();
        expectLastCall();
        mockPublisher.run();
        expectLastCall().andAnswer(getMockPublisherRunAnswer(latch));
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(getPutLogEventsAnswer());
        replayAll();
        CloudWatchLogsAppender appender = new CloudWatchLogsAppender(LOG_GROUP_NAME, LOG_STREAM_NAME, LogStreamNameSuffix.NONE, LAYOUT,
            MOCK_AWS_CREDENTIALS_PROVIDER.getCredentials(), new ClientConfiguration());
        appender.close();
        verifyAll();
    }

    @Test
    public void testCloudWatchLogsAppenderStringStringLayoutAWSCredentialsProviderClientConfiguration() throws Exception {
        LogPublisher mockPublisher = createMock(LogPublisher.class);
        AWSLogsClient mockClient = createMock(AWSLogsClient.class);
        expectNew(AWSLogsClient.class, new Class<?>[] {AWSCredentialsProvider.class, ClientConfiguration.class}, anyObject(AWSCredentialsProvider.class),
            anyObject(ClientConfiguration.class)).andStubReturn(mockClient);
        final Capture<CountDownLatch> latch = new Capture<CountDownLatch>();
        expectNew(LogPublisher.class, capture(latch)).andAnswer(getLogPublisherConstructorAnswer(mockPublisher));
        mockClient.describeLogGroups(anyObject(DescribeLogGroupsRequest.class));
        expectLastCall().andAnswer(getLogGroupExistsAnswer());
        mockClient.describeLogStreams(anyObject(DescribeLogStreamsRequest.class));
        expectLastCall().andAnswer(getLogStreamExistsAnswer());
        mockClient.shutdown();
        expectLastCall();
        mockPublisher.run();
        expectLastCall().andAnswer(getMockPublisherRunAnswer(latch));
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(getPutLogEventsAnswer());
        replayAll();
        CloudWatchLogsAppender appender = new CloudWatchLogsAppender(LOG_GROUP_NAME, LOG_STREAM_NAME, LogStreamNameSuffix.NONE, LAYOUT,
            MOCK_AWS_CREDENTIALS_PROVIDER, new ClientConfiguration());
        appender.close();
        verifyAll();
    }

    @Test
    public void testCloudWatchLogsAppenderStringStringLayoutAWSCredentialsProviderClientConfigurationRequestMetricCollector() throws Exception {
        LogPublisher mockPublisher = createMock(LogPublisher.class);
        AWSLogsClient mockClient = createMock(AWSLogsClient.class);
        expectNew(AWSLogsClient.class, new Class<?>[] {AWSCredentialsProvider.class, ClientConfiguration.class}, anyObject(AWSCredentialsProvider.class),
            anyObject(ClientConfiguration.class), anyObject(RequestMetricCollector.class)).andStubReturn(mockClient);
        final Capture<CountDownLatch> latch = new Capture<CountDownLatch>();
        expectNew(LogPublisher.class, capture(latch)).andAnswer(getLogPublisherConstructorAnswer(mockPublisher));
        mockClient.describeLogGroups(anyObject(DescribeLogGroupsRequest.class));
        expectLastCall().andAnswer(getLogGroupExistsAnswer());
        mockClient.describeLogStreams(anyObject(DescribeLogStreamsRequest.class));
        expectLastCall().andAnswer(getLogStreamExistsAnswer());
        mockClient.shutdown();
        expectLastCall();
        mockPublisher.run();
        expectLastCall().andAnswer(getMockPublisherRunAnswer(latch));
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(getPutLogEventsAnswer());
        replayAll();
        CloudWatchLogsAppender appender = new CloudWatchLogsAppender(LOG_GROUP_NAME, LOG_STREAM_NAME, LogStreamNameSuffix.NONE, LAYOUT,
            MOCK_AWS_CREDENTIALS_PROVIDER, new ClientConfiguration(), RequestMetricCollector.NONE);
        appender.close();
        verifyAll();
    }

    @Test
    public void testRequiresLayout() throws Exception {
        LogPublisher mockPublisher = createMock(LogPublisher.class);
        AWSLogsClient mockClient = createMock(AWSLogsClient.class);
        expectNew(AWSLogsClient.class).andStubReturn(mockClient);
        final Capture<CountDownLatch> latch = new Capture<CountDownLatch>();
        expectNew(LogPublisher.class, capture(latch)).andAnswer(getLogPublisherConstructorAnswer(mockPublisher));
        mockClient.describeLogGroups(anyObject(DescribeLogGroupsRequest.class));
        expectLastCall().andAnswer(getLogGroupExistsAnswer());
        mockClient.describeLogStreams(anyObject(DescribeLogStreamsRequest.class));
        expectLastCall().andAnswer(getLogStreamExistsAnswer());
        mockClient.shutdown();
        expectLastCall();
        mockPublisher.run();
        expectLastCall().andAnswer(getMockPublisherRunAnswer(latch));
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(getPutLogEventsAnswer());
        replayAll();
        CloudWatchLogsAppender appender = new CloudWatchLogsAppender(LOG_GROUP_NAME, LOG_STREAM_NAME, LogStreamNameSuffix.NONE, LAYOUT);
        assertTrue(appender.requiresLayout());
        appender.close();
        verifyAll();
    }

    @Test
    public void testAppendLoggingEventSize() throws Exception {
        LogPublisher mockPublisher = createMock(LogPublisher.class);
        AWSLogsClient mockClient = createMock(AWSLogsClient.class);
        expectNew(AWSLogsClient.class).andStubReturn(mockClient);
        final Capture<CountDownLatch> latch = new Capture<CountDownLatch>();
        expectNew(LogPublisher.class, capture(latch)).andAnswer(getLogPublisherConstructorAnswer(mockPublisher));
        mockClient.describeLogGroups(anyObject(DescribeLogGroupsRequest.class));
        expectLastCall().andAnswer(getLogGroupExistsAnswer());
        mockClient.describeLogStreams(anyObject(DescribeLogStreamsRequest.class));
        expectLastCall().andAnswer(getLogStreamExistsAnswer());
        int eventSize = Math.max(4096, CloudWatchLogsAppender.DEFAULT_EVENTS_SIZE_THRESHOLD / 8);
        final int numEvents = (int) Math.ceil(1.0D * CloudWatchLogsAppender.DEFAULT_EVENTS_SIZE_THRESHOLD / eventSize);
        char msg[] = new char[eventSize];
        Arrays.fill(msg, 'A');
        final String message = new String(msg);
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(new IAnswer<PutLogEventsResult>() {
            @Override
            public PutLogEventsResult answer() throws Throwable {
                PutLogEventsRequest request = (PutLogEventsRequest) getCurrentArguments()[0];
                assertNotNull(request);
                assertTrue(LOG_GROUP_NAME.equals(request.getLogGroupName()));
                assertTrue(LOG_STREAM_NAME.equals(request.getLogStreamName()));
                assertTrue(FIRST_SEQUENCE_TOKEN.equals(request.getSequenceToken()));
                assertNotNull(request.getLogEvents());
                assertEquals(numEvents - 1, request.getLogEvents().size());
                for (InputLogEvent event : request.getLogEvents()) {
                    assertTrue(message.equals(event.getMessage()));
                }
                return new PutLogEventsResult().withNextSequenceToken(SECOND_SEQUENCE_TOKEN);
            }
        });
        mockClient.shutdown();
        expectLastCall();
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class)); // Flushing the extra log event
        expectLastCall().andAnswer(new IAnswer<PutLogEventsResult>() {
            @Override
            public PutLogEventsResult answer() throws Throwable {
                PutLogEventsRequest request = (PutLogEventsRequest) getCurrentArguments()[0];
                assertNotNull(request);
                assertTrue(LOG_GROUP_NAME.equals(request.getLogGroupName()));
                assertTrue(LOG_STREAM_NAME.equals(request.getLogStreamName()));
                assertTrue(SECOND_SEQUENCE_TOKEN.equals(request.getSequenceToken()));
                assertNotNull(request.getLogEvents());
                assertEquals(1, request.getLogEvents().size());
                assertTrue(message.equals(request.getLogEvents().get(0).getMessage()));
                return new PutLogEventsResult().withNextSequenceToken(THIRD_SEQUENCE_TOKEN);
            }
        });
        mockPublisher.run();
        expectLastCall().andAnswer(getMockPublisherRunAnswer(latch));
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(getPutLogEventsAnswer());
        replayAll();
        CloudWatchLogsAppender appender = new CloudWatchLogsAppender(LOG_GROUP_NAME, LOG_STREAM_NAME, LogStreamNameSuffix.NONE, LAYOUT);
        appender.setLayout(new PatternLayout("%m"));

        Logger logger = Logger.getLogger("testLogger");
        logger.addAppender(appender);

        for (int i = 0; i < numEvents; i++) {
            logger.info(message);
        }

        appender.close();
        verifyAll();
    }

    @Test
    public void testAppendLoggingEventSizeMultiPage() throws Exception {
        LogPublisher mockPublisher = createMock(LogPublisher.class);
        AWSLogsClient mockClient = createMock(AWSLogsClient.class);
        expectNew(AWSLogsClient.class).andStubReturn(mockClient);
        final Capture<CountDownLatch> latch = new Capture<CountDownLatch>();
        expectNew(LogPublisher.class, capture(latch)).andAnswer(getLogPublisherConstructorAnswer(mockPublisher));
        mockClient.describeLogGroups(anyObject(DescribeLogGroupsRequest.class));
        expectLastCall().andAnswer(getLogGroupExistsAnswer());
        mockClient.describeLogStreams(anyObject(DescribeLogStreamsRequest.class));
        expectLastCall().andAnswer(getLogStreamExistsAnswer());
        int eventSize = Math.max(4096, CloudWatchLogsAppender.DEFAULT_EVENTS_SIZE_THRESHOLD / 8);
        final int numEvents = (int) Math.ceil(1.0D * CloudWatchLogsAppender.DEFAULT_EVENTS_SIZE_THRESHOLD / eventSize);
        char msg[] = new char[eventSize];
        Arrays.fill(msg, 'A');
        final String message = new String(msg);
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(new IAnswer<PutLogEventsResult>() {
            @Override
            public PutLogEventsResult answer() throws Throwable {
                PutLogEventsRequest request = (PutLogEventsRequest) getCurrentArguments()[0];
                assertNotNull(request);
                assertTrue(LOG_GROUP_NAME.equals(request.getLogGroupName()));
                assertTrue(LOG_STREAM_NAME.equals(request.getLogStreamName()));
                assertTrue(FIRST_SEQUENCE_TOKEN.equals(request.getSequenceToken()));
                assertNotNull(request.getLogEvents());
                assertEquals(numEvents - 1, request.getLogEvents().size());
                for (InputLogEvent event : request.getLogEvents()) {
                    assertTrue(message.equals(event.getMessage()));
                }
                return new PutLogEventsResult().withNextSequenceToken(SECOND_SEQUENCE_TOKEN);
            }
        });
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(new IAnswer<PutLogEventsResult>() {
            @Override
            public PutLogEventsResult answer() throws Throwable {
                PutLogEventsRequest request = (PutLogEventsRequest) getCurrentArguments()[0];
                assertNotNull(request);
                assertTrue(LOG_GROUP_NAME.equals(request.getLogGroupName()));
                assertTrue(LOG_STREAM_NAME.equals(request.getLogStreamName()));
                assertTrue(SECOND_SEQUENCE_TOKEN.equals(request.getSequenceToken()));
                assertNotNull(request.getLogEvents());
                assertEquals(numEvents - 1, request.getLogEvents().size());
                for (InputLogEvent event : request.getLogEvents()) {
                    assertTrue(message.equals(event.getMessage()));
                }
                return new PutLogEventsResult().withNextSequenceToken(THIRD_SEQUENCE_TOKEN);
            }
        });
        mockClient.shutdown();
        expectLastCall();
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class)); // Flushing the extra log event
        expectLastCall().andAnswer(new IAnswer<PutLogEventsResult>() {
            @Override
            public PutLogEventsResult answer() throws Throwable {
                PutLogEventsRequest request = (PutLogEventsRequest) getCurrentArguments()[0];
                assertNotNull(request);
                assertTrue(LOG_GROUP_NAME.equals(request.getLogGroupName()));
                assertTrue(LOG_STREAM_NAME.equals(request.getLogStreamName()));
                assertTrue(THIRD_SEQUENCE_TOKEN.equals(request.getSequenceToken()));
                assertNotNull(request.getLogEvents());
                assertEquals(1, request.getLogEvents().size());
                assertTrue(message.equals(request.getLogEvents().get(0).getMessage()));
                return new PutLogEventsResult().withNextSequenceToken(THIRD_SEQUENCE_TOKEN);
            }
        });
        mockPublisher.run();
        expectLastCall().andAnswer(getMockPublisherRunAnswer(latch));
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(getPutLogEventsAnswer());
        replayAll();
        CloudWatchLogsAppender appender = new CloudWatchLogsAppender(LOG_GROUP_NAME, LOG_STREAM_NAME, LogStreamNameSuffix.NONE, LAYOUT);
        appender.setLayout(new PatternLayout("%m"));

        Logger logger = Logger.getLogger("testLogger");
        logger.addAppender(appender);

        for (int i = 0; i < 2 * numEvents - 1; i++) {
            logger.info(message);
        }

        appender.close();
        verifyAll();
    }

    @Test
    public void testAppendLoggingEventCount() throws Exception {
        LogPublisher mockPublisher = createMock(LogPublisher.class);
        AWSLogsClient mockClient = createMock(AWSLogsClient.class);
        expectNew(AWSLogsClient.class).andStubReturn(mockClient);
        final Capture<CountDownLatch> latch = new Capture<CountDownLatch>();
        expectNew(LogPublisher.class, capture(latch)).andAnswer(getLogPublisherConstructorAnswer(mockPublisher));
        mockClient.describeLogGroups(anyObject(DescribeLogGroupsRequest.class));
        expectLastCall().andAnswer(getLogGroupExistsAnswer());
        mockClient.describeLogStreams(anyObject(DescribeLogStreamsRequest.class));
        expectLastCall().andAnswer(getLogStreamExistsAnswer());
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        final String message = "Hi";
        expectLastCall().andAnswer(new IAnswer<PutLogEventsResult>() {
            @Override
            public PutLogEventsResult answer() throws Throwable {
                PutLogEventsRequest request = (PutLogEventsRequest) getCurrentArguments()[0];
                assertNotNull(request);
                assertTrue(LOG_GROUP_NAME.equals(request.getLogGroupName()));
                assertTrue(LOG_STREAM_NAME.equals(request.getLogStreamName()));
                assertTrue(FIRST_SEQUENCE_TOKEN.equals(request.getSequenceToken()));
                assertNotNull(request.getLogEvents());
                assertEquals(CloudWatchLogsAppender.DEFAULT_EVENT_COUNT_THRESHOLD, request.getLogEvents().size());
                for (InputLogEvent event : request.getLogEvents()) {
                    assertTrue(message.equals(event.getMessage()));
                }
                return new PutLogEventsResult().withNextSequenceToken(SECOND_SEQUENCE_TOKEN);
            }
        });
        mockClient.shutdown();
        expectLastCall();
        mockPublisher.run();
        expectLastCall().andAnswer(getMockPublisherRunAnswer(latch));
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(getPutLogEventsAnswer());
        replayAll();
        CloudWatchLogsAppender appender = new CloudWatchLogsAppender(LOG_GROUP_NAME, LOG_STREAM_NAME, LogStreamNameSuffix.NONE, LAYOUT);
        appender.setLayout(new PatternLayout("%m"));

        Logger logger = Logger.getLogger("testLogger");
        logger.addAppender(appender);

        for (int i = 0; i < CloudWatchLogsAppender.DEFAULT_EVENT_COUNT_THRESHOLD; i++) {
            logger.info(message);
        }

        appender.close();
        verifyAll();
    }

    @Test
    public void testAppendLoggingEventCountMultiPage() throws Exception {
        LogPublisher mockPublisher = createMock(LogPublisher.class);
        AWSLogsClient mockClient = createMock(AWSLogsClient.class);
        expectNew(AWSLogsClient.class).andStubReturn(mockClient);
        final Capture<CountDownLatch> latch = new Capture<CountDownLatch>();
        expectNew(LogPublisher.class, capture(latch)).andAnswer(getLogPublisherConstructorAnswer(mockPublisher));
        mockClient.describeLogGroups(anyObject(DescribeLogGroupsRequest.class));
        expectLastCall().andAnswer(getLogGroupExistsAnswer());
        mockClient.describeLogStreams(anyObject(DescribeLogStreamsRequest.class));
        expectLastCall().andAnswer(getLogStreamExistsAnswer());
        final String message = "Hi";
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(new IAnswer<PutLogEventsResult>() {
            @Override
            public PutLogEventsResult answer() throws Throwable {
                PutLogEventsRequest request = (PutLogEventsRequest) getCurrentArguments()[0];
                assertNotNull(request);
                assertTrue(LOG_GROUP_NAME.equals(request.getLogGroupName()));
                assertTrue(LOG_STREAM_NAME.equals(request.getLogStreamName()));
                assertTrue(FIRST_SEQUENCE_TOKEN.equals(request.getSequenceToken()));
                assertNotNull(request.getLogEvents());
                assertEquals(CloudWatchLogsAppender.DEFAULT_EVENT_COUNT_THRESHOLD, request.getLogEvents().size());
                for (InputLogEvent event : request.getLogEvents()) {
                    assertTrue(message.equals(event.getMessage()));
                }
                return new PutLogEventsResult().withNextSequenceToken(SECOND_SEQUENCE_TOKEN);
            }
        });
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(new IAnswer<PutLogEventsResult>() {
            @Override
            public PutLogEventsResult answer() throws Throwable {
                PutLogEventsRequest request = (PutLogEventsRequest) getCurrentArguments()[0];
                assertNotNull(request);
                assertTrue(LOG_GROUP_NAME.equals(request.getLogGroupName()));
                assertTrue(LOG_STREAM_NAME.equals(request.getLogStreamName()));
                assertTrue(SECOND_SEQUENCE_TOKEN.equals(request.getSequenceToken()));
                assertNotNull(request.getLogEvents());
                assertEquals(CloudWatchLogsAppender.DEFAULT_EVENT_COUNT_THRESHOLD, request.getLogEvents().size());
                for (InputLogEvent event : request.getLogEvents()) {
                    assertTrue(message.equals(event.getMessage()));
                }
                return new PutLogEventsResult().withNextSequenceToken(SECOND_SEQUENCE_TOKEN);
            }
        });
        mockClient.shutdown();
        expectLastCall();
        mockPublisher.run();
        expectLastCall().andAnswer(getMockPublisherRunAnswer(latch));
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(getPutLogEventsAnswer());
        replayAll();
        CloudWatchLogsAppender appender = new CloudWatchLogsAppender(LOG_GROUP_NAME, LOG_STREAM_NAME, LogStreamNameSuffix.NONE, LAYOUT);
        appender.setLayout(new PatternLayout("%m"));

        Logger logger = Logger.getLogger("testLogger");
        logger.addAppender(appender);

        for (int i = 0; i < 2 * CloudWatchLogsAppender.DEFAULT_EVENT_COUNT_THRESHOLD; i++) {
            logger.info(message);
        }

        appender.close();
        verifyAll();
    }

    @Test
    public void testAppendLoggingEventLevel() throws Exception {
        LogPublisher mockPublisher = createMock(LogPublisher.class);
        AWSLogsClient mockClient = createMock(AWSLogsClient.class);
        expectNew(AWSLogsClient.class).andStubReturn(mockClient);
        final Capture<CountDownLatch> latch = new Capture<CountDownLatch>();
        expectNew(LogPublisher.class, capture(latch)).andAnswer(getLogPublisherConstructorAnswer(mockPublisher));
        mockClient.describeLogGroups(anyObject(DescribeLogGroupsRequest.class));
        expectLastCall().andAnswer(getLogGroupExistsAnswer());
        mockClient.describeLogStreams(anyObject(DescribeLogStreamsRequest.class));
        expectLastCall().andAnswer(getLogStreamExistsAnswer());
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        final String message = "Hi";
        expectLastCall().andAnswer(new IAnswer<PutLogEventsResult>() {
            @Override
            public PutLogEventsResult answer() throws Throwable {
                PutLogEventsRequest request = (PutLogEventsRequest) getCurrentArguments()[0];
                assertNotNull(request);
                assertTrue(LOG_GROUP_NAME.equals(request.getLogGroupName()));
                assertTrue(LOG_STREAM_NAME.equals(request.getLogStreamName()));
                assertTrue(FIRST_SEQUENCE_TOKEN.equals(request.getSequenceToken()));
                assertNotNull(request.getLogEvents());
                assertEquals(1, request.getLogEvents().size());
                assertEquals(message, request.getLogEvents().get(0).getMessage());
                return new PutLogEventsResult().withNextSequenceToken(SECOND_SEQUENCE_TOKEN);
            }
        });
        mockClient.shutdown();
        expectLastCall();
        mockPublisher.run();
        expectLastCall().andAnswer(getMockPublisherRunAnswer(latch));
        mockClient.putLogEvents(anyObject(PutLogEventsRequest.class));
        expectLastCall().andAnswer(getPutLogEventsAnswer());
        replayAll();
        CloudWatchLogsAppender appender = new CloudWatchLogsAppender(LOG_GROUP_NAME, LOG_STREAM_NAME, LogStreamNameSuffix.NONE, LAYOUT);
        appender.setLayout(new PatternLayout("%m"));

        Logger logger = Logger.getLogger("testLogger");
        logger.addAppender(appender);

        logger.error(message, new RuntimeException("TestException"));

        appender.close();
        verifyAll();
    }

}
