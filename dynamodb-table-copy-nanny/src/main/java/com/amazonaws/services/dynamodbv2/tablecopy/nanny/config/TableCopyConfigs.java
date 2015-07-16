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
package com.amazonaws.services.dynamodbv2.tablecopy.nanny.config;

import com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.request.TableCopyCallback;
import com.amazonaws.services.dynamodbv2.tablecopy.nanny.model.callback.UpdateMetadataCallback;
import org.apache.log4j.Logger;

/**
 * Configs class that reads in the the cmd line args
 */
public class TableCopyConfigs {

    private static final Logger LOG = Logger.getLogger(TableCopyConfigs.class);

    public String sourceTable;
    public String sourceEndpoint;
    public double readFraction;

    public String destinationTable;
    public String destinationEndpoint;
    public double writeFraction;

    public String metadataTable;
    public String metadataEndpoint;

    public String replicationGroupId;
    public String replicationGroupMember;

    public String ecsServiceName;

    public TableCopyCallback callback;

    public String customTimeout;

    public String numOfSegments;


    public TableCopyConfigs(CommandLineArgs cmdArgs) {
        sourceTable = cmdArgs.getSourceTable();
        sourceEndpoint = cmdArgs.getSourceEndpoint();
        readFraction = Double.parseDouble(cmdArgs.getReadFraction());

        destinationTable = cmdArgs.getDestinationTable();
        destinationEndpoint = cmdArgs.getDestinationEndpoint();
        writeFraction = Double.parseDouble(cmdArgs.getWriteFraction());

        metadataTable = cmdArgs.getMetadataTable();
        metadataEndpoint = cmdArgs.getMetadataEndpoint();
        replicationGroupId = cmdArgs.getReplicationGroupId();
        replicationGroupMember = cmdArgs.getReplicationGroupMember();

        ecsServiceName = cmdArgs.getEcsServiceName();

        customTimeout = cmdArgs.getCustomTimeout();

        LOG.info("srcTable:" + sourceTable + ", srcEndpoint:" + sourceEndpoint + ", readFraction:" + readFraction);
        LOG.info("dstTable:" + destinationTable + ", dstEndpoint:" + destinationEndpoint + ", writeFraction:" + writeFraction);
        LOG.info("metadataTable:" + metadataTable + ", metadataEndpoint:" + metadataEndpoint + ", replicationGroupId:"
            + replicationGroupId + ", replicationGroupMember:" + replicationGroupMember);
        callback = new UpdateMetadataCallback(replicationGroupId, replicationGroupMember);
    }

    public void setNumOfSegments(String numOfSegments) {
        this.numOfSegments = numOfSegments;
    }
}
