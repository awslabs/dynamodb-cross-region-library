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
package com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.config;

import com.amazonaws.services.dynamodbv2.tablecopy.nanny.config.CommandLineArgs;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.config.TableCopyConfigs;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(TableCopyConfigs.class)
public class TableCopyConfigsTest {

    static final int delta = 0;

    static final String SRC_TABLE = "srcTable";
    static final String SRC_ENDPOINT = "srcEndpoint";
    static final String READ_FRACTION = "1";
    static final double READ = 1;

    static final String DST_TABLE = "dstTable";
    static final String DST_ENDPOINT = "dstEndpoint";
    static final String WRITE_FRACTION = "0";
    static final double WRITE = 0;

    static final String METADATA_TABLE = "metadataTable";
    static final String METADATA_ENDPOINT = "metadataEndpoint";

    static final String REP_GROUP_ID = "repGroupId";
    static final String REP_GROUP_MEMBER = "repGroupMember";

    static final String ECS_SERVICE_NAME = "ecsServiceName";

    static final String CUSTOM_TIMEOUT = "1000";
    static CommandLineArgs cmdArgs;

    @Before
    public void setup() {
        cmdArgs = PowerMock.createMock(CommandLineArgs.class);
        EasyMock.expect(cmdArgs.getSourceTable()).andReturn(SRC_TABLE);
        EasyMock.expect(cmdArgs.getSourceEndpoint()).andReturn(SRC_ENDPOINT);
        EasyMock.expect(cmdArgs.getReadFraction()).andReturn(READ_FRACTION);

        EasyMock.expect(cmdArgs.getDestinationTable()).andReturn(DST_TABLE);
        EasyMock.expect(cmdArgs.getDestinationEndpoint()).andReturn(DST_ENDPOINT);
        EasyMock.expect(cmdArgs.getWriteFraction()).andReturn(WRITE_FRACTION);

        EasyMock.expect(cmdArgs.getMetadataTable()).andReturn(METADATA_TABLE);
        EasyMock.expect(cmdArgs.getMetadataEndpoint()).andReturn(METADATA_ENDPOINT);

        EasyMock.expect(cmdArgs.getReplicationGroupId()).andReturn(REP_GROUP_ID);
        EasyMock.expect(cmdArgs.getReplicationGroupMember()).andReturn(REP_GROUP_MEMBER);

        EasyMock.expect(cmdArgs.getCustomTimeout()).andReturn(CUSTOM_TIMEOUT);

        EasyMock.expect(cmdArgs.getEcsServiceName()).andReturn(ECS_SERVICE_NAME);
    }

    @Test
    public void testTableCopyConfigs() {
        PowerMock.replayAll();
        TableCopyConfigs configs = new TableCopyConfigs(cmdArgs);
        PowerMock.verifyAll();

        assertEquals(SRC_TABLE, configs.sourceTable);
        assertEquals(SRC_ENDPOINT, configs.sourceEndpoint);
        assertEquals(READ, configs.readFraction, delta);

        assertEquals(DST_TABLE, configs.destinationTable);
        assertEquals(DST_ENDPOINT, configs.destinationEndpoint);
        assertEquals(WRITE, configs.writeFraction, delta);

        assertEquals(METADATA_TABLE, configs.metadataTable);
        assertEquals(METADATA_ENDPOINT, configs.metadataEndpoint);

        assertEquals(REP_GROUP_ID, configs.replicationGroupId);
        assertEquals(REP_GROUP_MEMBER, configs.replicationGroupMember);

        assertEquals(CUSTOM_TIMEOUT, configs.customTimeout);
        assertEquals(ECS_SERVICE_NAME, configs.ecsServiceName);
    }
}
