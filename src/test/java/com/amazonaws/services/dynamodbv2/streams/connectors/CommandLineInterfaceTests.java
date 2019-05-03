/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streams.connectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Optional;

public class CommandLineInterfaceTests {
    private final String destinationSigningRegion = Regions.EU_WEST_1.getName();
    private final String destinationTableValue = "SingleShardMaxReplica";
    private final String sourceSigningRegion = Regions.US_WEST_1.getName();
    private final String sourceTableValue = "SingleShardMax2";

    private final String[] sampleArgs = {
            CommandLineArgs.DESTINATION_SIGNING_REGION,
            destinationSigningRegion,
            CommandLineArgs.DESTINATION_TABLE,
            destinationTableValue,
            CommandLineArgs.SOURCE_SIGNING_REGION,
            sourceSigningRegion,
            CommandLineArgs.SOURCE_TABLE,
            sourceTableValue
    };
    private CommandLineArgs args = null;
    private JCommander cmd = null;

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();

    @Before
    public void setUp() throws Exception {
        args = new CommandLineArgs();
        cmd = new JCommander(args);
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
    }

    @After
    public void tearDown() throws Exception {
        System.setOut(null);
        System.setErr(null);
    }

    @Test
    public void testCreateEndpointConfiguration() {
        final AwsClientBuilder.EndpointConfiguration config = CommandLineInterface.createEndpointConfiguration(RegionUtils.getRegion(Regions.US_EAST_1.getName()),
                Optional.<String>absent(), AmazonDynamoDB.ENDPOINT_PREFIX);
        assertTrue(config.getServiceEndpoint().contains("https"));
        assertTrue(config.getServiceEndpoint().contains(AmazonDynamoDB.ENDPOINT_PREFIX));
        assertEquals(Regions.US_EAST_1.getName(), config.getSigningRegion());
    }

    @Test(expected = ParameterException.class)
    public void noOptionsTest() {
        cmd.parse();
    }

    @Test(expected = ParameterException.class)
    public void missingOptionTest() {
        List<String> incompleteArgs = new ArrayList<>(Arrays.asList(sampleArgs));
        incompleteArgs.remove(CommandLineArgs.DESTINATION_SIGNING_REGION);
        incompleteArgs.remove(destinationSigningRegion);
        cmd.parse(incompleteArgs.toArray(new String[incompleteArgs.size()]));
    }

    @Test(expected = ParameterException.class)
    public void missingValueTest() {
        List<String> incompleteArgs = new ArrayList<String>(Arrays.asList(sampleArgs));
        incompleteArgs.remove(sourceTableValue);
        cmd.parse(incompleteArgs.toArray(new String[incompleteArgs.size()]));
    }

    @Test
    public void helpOptionTest() {
        CommandLineInterface.main(new String[] {CommandLineArgs.HELP});
        StringBuilder strBuilder = new StringBuilder();
        cmd.usage(strBuilder);
        assertEquals(outContent.toString().trim(), strBuilder.toString().trim());
    }

    @Test
    public void completeArgsTest() {
        cmd.parse(sampleArgs);
        assertEquals(args.getDestinationSigningRegion(), destinationSigningRegion);
        assertEquals(args.getDestinationTable(), destinationTableValue);
        assertEquals(args.getSourceSigningRegion(), sourceSigningRegion);
        assertEquals(args.getSourceTable(), sourceTableValue);
        assertEquals(args.getKclSigningRegion(), null);
    }

    @Test
    public void testKclDynamoDbClientDefault() {
        cmd.parse(sampleArgs);
        CommandLineInterface cli = new CommandLineInterface(args);
        EndpointConfiguration config = cli.createKclDynamoDbEndpointConfiguration();
        assertEquals(cli.getSourceRegion().getName(), config.getSigningRegion());
    }
}
