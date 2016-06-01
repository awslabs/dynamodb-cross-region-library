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
package com.amazonaws.services.dynamodbv2.streams.connectors;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

public class CommandLineInterfaceTests {
    private final String destinationEndpointValue = "dynamodb.eu-west-1.amazonaws.com";
    private final String destinationTableValue = "SingleShardMaxReplica";
    private final String sourceEndpointValue = "dynamodb.us-west-1.amazonaws.com";
    private final String sourceTableValue = "SingleShardMax2";

    private final String[] sampleArgs = {CommandLineArgs.DESTINATION_ENDPOINT,
        destinationEndpointValue, CommandLineArgs.DESTINATION_TABLE, destinationTableValue,
        CommandLineArgs.SOURCE_ENDPOINT, sourceEndpointValue, CommandLineArgs.SOURCE_TABLE, sourceTableValue};
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

    @Test(expected = ParameterException.class)
    public void noOptionsTest() {
        cmd.parse();
    }

    @Test(expected = ParameterException.class)
    public void missingOptionTest() {
        List<String> incompleteArgs = new ArrayList<String>(Arrays.asList(sampleArgs));
        incompleteArgs.remove(CommandLineArgs.DESTINATION_ENDPOINT);
        incompleteArgs.remove(destinationEndpointValue);
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
        assertEquals(args.getDestinationEndpoint(), destinationEndpointValue);
        assertEquals(args.getDestinationTable(), destinationTableValue);
        assertEquals(args.getSourceEndpoint(), sourceEndpointValue);
        assertEquals(args.getSourceTable(), sourceTableValue);
    }
}
