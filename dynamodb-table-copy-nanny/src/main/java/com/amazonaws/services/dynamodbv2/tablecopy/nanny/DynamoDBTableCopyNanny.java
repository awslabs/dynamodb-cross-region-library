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
package com.amazonaws.services.dynamodbv2.tablecopy.nanny;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.tablecopy.client.DynamoDBTableCopyClient;
import com.amazonaws.services.dynamodbv2.tablecopy.client.ecs.LocalTableCopyTaskHandlerFactory;
import com.amazonaws.services.dynamodbv2.tablecopy.client.metadataaccess.impl.RestartMetadataAccess;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.request.TableCopyRequest;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTaskHandlerFactory;
import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.trackers.TableCopyTracker;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.config.CommandLineArgs;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.config.TableCopyConfigs;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.TableCopyConstants;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.TableCopyUtils;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.daemon.nanny.*;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.daemon.reader.ReaderDaemon;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.daemon.reader.StdErrReaderDaemon;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.daemon.reader.StdOutReaderDaemon;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.timeout.ECSTimeoutCalculator;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.timeout.TimeoutCalculator;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMemberStatus;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBMetadataStorage;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationUtilities;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClient;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.log4j.Logger;

import java.util.concurrent.*;


public class DynamoDBTableCopyNanny {

    private static final Logger LOG = Logger.getLogger(DynamoDBTableCopyNanny.class);
    public static final int MAX_THREADPOOL_SIZE = 7;
    public static ScheduledThreadPoolExecutor threadpool;

    static AmazonDynamoDB dynamoDB;
    static AmazonECS ecs;
    static AmazonCloudWatch cloudWatch;

    public static void main(String[] args) {

        CommandLineArgs cmdArgs = new CommandLineArgs();
        JCommander cmd = new JCommander(cmdArgs);
        boolean missingRequiredArg = false;

        try {
            cmd.parse(args);
        } catch (ParameterException pe) {
            LOG.warn(pe);
            missingRequiredArg = true;
        }

        if (cmdArgs.needUsage() || missingRequiredArg) {
            cmd.usage();
        } else {
            final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
            LOG.info("Creating configs");
            final TableCopyConfigs configs = new TableCopyConfigs(cmdArgs);

            DynamoDBMetadataStorage.init(credentialsProvider, configs.metadataEndpoint, configs.metadataTable);

            BlockingQueue<NannyDaemon> pseudoSemaphore = new LinkedBlockingQueue<>(MAX_THREADPOOL_SIZE);
            threadpool = new ScheduledThreadPoolExecutor(MAX_THREADPOOL_SIZE);


            DynamoDBTableCopyClient client;
            TableCopyRequest request;
            TableCopyTracker taskTracker;
            TimeoutCalculator timeoutCalculator;


            client = setupLocalTableCopy(configs, credentialsProvider, threadpool);
            request = new TableCopyRequest(configs.sourceTable, configs.sourceEndpoint,
                    configs.readFraction, configs.destinationTable, configs.destinationEndpoint,
                    configs.writeFraction);

            taskTracker = client.launchTableCopy(request, configs.callback);

            LOG.info("TimeoutCalculator is using ecsServiceName:" + configs.ecsServiceName);
            timeoutCalculator = new ECSTimeoutCalculator(ecs, configs.ecsServiceName);

            int exitCode = 0;
            long timeout = (configs.customTimeout == null) ? timeoutCalculator.calculateTimeoutInMillis() :
                            Long.valueOf(configs.customTimeout);

            try {
                NannyDaemon cDaemon = new CancellationDaemon(configs.replicationGroupId, configs.replicationGroupMember)
                                            .withPseudoSemaphore(pseudoSemaphore);

                NannyDaemon sDaemon = new StatusDaemon(taskTracker).withPseudoSemaphore(pseudoSemaphore);

                NannyDaemon tDaemon = new TimeoutDaemon(configs.replicationGroupId, configs.replicationGroupMember)
                                        .withPseudoSemaphore(pseudoSemaphore);

                cloudWatch = new AmazonCloudWatchClient(credentialsProvider);

                NannyDaemon pDaemon = new ProgressDaemon(cloudWatch, configs);

                NannyDaemon[] nannyDaemons = {cDaemon, sDaemon, tDaemon, pDaemon};

                ReaderDaemon stdoutReader = new StdOutReaderDaemon(taskTracker);

                ReaderDaemon stderrReader = new StdErrReaderDaemon(taskTracker);

                ReaderDaemon[] readerDaemons = {stdoutReader, stderrReader};

                threadpool.submit(stdoutReader);
                threadpool.submit(stderrReader);

                threadpool.submit(pDaemon);
                threadpool.submit(sDaemon);
                threadpool.submit(cDaemon);
                threadpool.schedule(tDaemon, timeout, TimeUnit.MILLISECONDS);

                // Blocks until one of the daemons finish executing
                NannyDaemon finishedDaemon = pseudoSemaphore.take();

                threadpool.shutdownNow();
                for (NannyDaemon nannyDaemon : nannyDaemons) {
                    nannyDaemon.shutdown();
                }

                for (ReaderDaemon readerDaemon : readerDaemons) {
                    readerDaemon.shutdown();
                }

                finishedDaemon.callback(taskTracker);
            } catch (InterruptedException ie) {
                exitCode = 1;
                LOG.fatal(ie);
                tryToNotifyMetadata(configs.replicationGroupId, configs.replicationGroupMember);
            } catch (IllegalStateException ise) {
                exitCode = 1;
                LOG.fatal(ise);
                tryToNotifyMetadata(configs.replicationGroupId, configs.replicationGroupMember);
            } catch (RuntimeException re) {
                exitCode = 1;
                LOG.fatal("Failed to clean up and mark metadatatable", re);
            } finally {
                LOG.info("System exiting with exit code: "+ exitCode);
                System.exit(exitCode);
            }
        }
    }

    protected static DynamoDBTableCopyClient setupLocalTableCopy(final TableCopyConfigs configs,
                                                                 AWSCredentialsProvider credentialsProvider,
                                                                 ExecutorService tableCopyThreadpool) {
        final String instanceRegion = TableCopyUtils.getInstanceRegion();

        dynamoDB = new AmazonDynamoDBClient(credentialsProvider);
        dynamoDB.setEndpoint(configs.sourceEndpoint);

        configs.setNumOfSegments(TableCopyUtils.calculateSegments(dynamoDB, configs.sourceTable));

        ecs = new AmazonECSClient(credentialsProvider);
        if (instanceRegion != null) {
            ecs.setRegion(Region.getRegion(Regions.fromName(instanceRegion)));
        }

        RestartMetadataAccess metadataAccess = new RestartMetadataAccess();
        final TableCopyTaskHandlerFactory factory = new LocalTableCopyTaskHandlerFactory() {
            @Override
            public String generateLocalCommand(TableCopyRequest tableCopyRequest) {
                String command = TableCopyConstants.PATH_TO_TABLECOPY_BIN + " --sourceEndpoint " + tableCopyRequest.srcEndpoint
                        + " --destinationEndpoint " + tableCopyRequest.dstEndpoint + " --totalSegments "
                        + configs.numOfSegments + " "
                        + DynamoDBReplicationUtilities.getRegionFromEndpoint(tableCopyRequest.srcEndpoint)
                        + ":" + tableCopyRequest.srcTableName + " "
                        + DynamoDBReplicationUtilities.getRegionFromEndpoint(tableCopyRequest.dstEndpoint)
                        + ":" + tableCopyRequest.dstTableName;

                LOG.info("Overriding LocalTableCopy command to: " + command);
                return command;
            }
        };

        return new DynamoDBTableCopyClient(metadataAccess, factory, tableCopyThreadpool);
    }

    protected static void tryToNotifyMetadata(String replicationGroupId, String replicationGroupMember) {
        int retries = 0;
        boolean success;
        do {
            success = TableCopyUtils.markReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.BOOTSTRAP_FAILED,
                    replicationGroupId, replicationGroupMember);
        } while(!success && retries < TableCopyConstants.MAX_METADATA_RETRIES);

        if (!success) {
            throw new RuntimeException("Unable to mark the metadata table");
        }
    }
}
