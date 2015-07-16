/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.replication.server;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.replication.CommandLineArgs;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBMetadataStorage;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBReplicationCoordinator;
import com.amazonaws.services.dynamodbv2.replication.server.api.DynamoDBReplicationGroupResource;
import com.amazonaws.services.dynamodbv2.streams.connectors.StatusCodes;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

/**
 * A replication server instance for the DynamoDB cross region replication application that receives requests from the front end user interface, each request is
 * processed and reflected in the metadata table, detected by {@link DynamoDBReplicationCoordinator}.
 *
 */
public class DynamoDBReplicationServer {

    /*
     * Logger for the class
     */
    private static final Logger LOGGER = Logger.getLogger(DynamoDBReplicationServer.class);

    /*
     * Countdown latch to signal shutdown
     */
    public static final CountDownLatch shutdownLatch = new CountDownLatch(1);

    /**
     * Command line main method entry point
     *
     * @param args
     *            command line arguments
     */
    public static void main(String[] args) {
        // Initialize command line arguments and JCommander parser
        CommandLineArgs params = new CommandLineArgs();
        JCommander cmd = new JCommander(params);

        try {
            // parse given arguments
            cmd.parse(args);

            // show usage information if help flag exists
            if (params.getHelp()) {
                cmd.usage();
                System.exit(StatusCodes.EINVAL);
            }

            // use default credential provider chain to locate appropriate credentials
            AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

            // initialize DynamoDB metadata storage on the metadata table
            DynamoDBMetadataStorage.init(credentialsProvider, params.getMetadataTableEndpoint(), params.getMetadataTableName());

            // initialize DynamoDB client and set the endpoint properly
            AmazonDynamoDBClient dynamodbClient = new AmazonDynamoDBClient(credentialsProvider).withEndpoint(params.getMetadataTableEndpoint());

            // set up the metadata table
            DynamoDBReplicationCoordinator.setUpMetadataTable(params, dynamodbClient);

            // run the API server
            runCoordinatorServer(params.getPort());
        } catch (ParameterException e) {
            LOGGER.error(e.getMessage());
            cmd.usage();
            System.exit(StatusCodes.EINVAL);
        } catch (Exception e) {
            LOGGER.fatal(e.getMessage());
            System.exit(StatusCodes.EINVAL);
        }
    }

    /**
     * Launches the coordinator API server to receive commands from the console UI.
     *
     * @param serverPort
     *            the port from which the coordinator server will be hosted at
     * @throws Exception
     *             throws an exception when server cannot be successfully launched.
     */
    private static void runCoordinatorServer(int serverPort) throws Exception {
        // create servlet context handler at the root URL
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        // create jetty server host at given port value, then set the servlet context handler
        final Server jettyServer = new Server(serverPort);
        jettyServer.setHandler(context);

        // direct all requests to the given servlet
        final ServletHolder jerseyServlet = context.addServlet(org.glassfish.jersey.servlet.ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);

        // Tells the Jersey Servlet which REST service/class to load.
        jerseyServlet.setInitParameter("jersey.config.server.provider.classnames", DynamoDBReplicationGroupResource.class.getCanonicalName());

        try {
            // start a thread to monitor shutdown signal
            new Thread() {
                @Override
                public void run() {
                    boolean shutdownComplete = false;
                    while (!shutdownComplete) {
                        try {
                            // wait for a shutdown signal
                            shutdownLatch.await();

                            // teardown everything
                            jerseyServlet.stop();
                            context.stop();
                            jettyServer.stop();
                            jettyServer.destroy();
                            shutdownComplete = true;
                            LOGGER.info("Finished shutting down DynamoDB Replication Server.");
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        } catch (Exception e) {
                            LOGGER.error("Failed to stop DynamoDB Replication Server gracefully, force exiting...");
                            System.exit(StatusCodes.EINVAL);
                        }
                    }
                }
            }.start();

            // start the jetty server
            jettyServer.start();
            jettyServer.join();
        } finally {
            // shutdown logger
            LogManager.shutdown();
        }
    }
}
