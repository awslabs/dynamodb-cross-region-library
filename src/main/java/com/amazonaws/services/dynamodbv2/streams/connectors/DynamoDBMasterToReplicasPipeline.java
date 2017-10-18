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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;

import java.util.Map;

/**
 * The Pipeline used when there is only one single master replicating to multiple replicas. Uses:
 * <ul>
 * <li>{@link DynamoDBReplicationEmitter}</li>
 * <li>{@link DynamoDBBuffer}</li>
 * <li>{@link DynamoDBStreamsRecordTransformer}</li>
 * <li>{@link AllPassFilter}</li>
 * </ul>
 */

public class DynamoDBMasterToReplicasPipeline implements IKinesisConnectorPipeline<Record, Record> {

    @Override
    public IEmitter<Record> getEmitter(final KinesisConnectorConfiguration configuration) {
        if (configuration instanceof DynamoDBStreamsConnectorConfiguration) {
            return new DynamoDBReplicationEmitter((DynamoDBStreamsConnectorConfiguration) configuration);
        } else {
            throw new IllegalArgumentException(this + " needs a DynamoDBStreamsConnectorConfiguration argument.");
        }

    }

    @Override
    public IBuffer<Record> getBuffer(final KinesisConnectorConfiguration configuration) {
        if (configuration instanceof DynamoDBStreamsConnectorConfiguration) {
            return new DynamoDBBuffer((DynamoDBStreamsConnectorConfiguration) configuration);
        } else {
            throw new IllegalArgumentException(this + " needs a DynamoDBStreamsConnectorConfiguration argument.");
        }
    }

    @Override
    public ITransformer<Record, Record> getTransformer(final KinesisConnectorConfiguration configuration) {
        return new DynamoDBStreamsRecordTransformer();
    }

    @Override
    public IFilter<Record> getFilter(final KinesisConnectorConfiguration configuration) {
        //return new AllPassFilter<Record>();
        return new IFilter<Record>() {
            @Override
            public boolean keepRecord(Record record) {
                DynamoDBStreamsConnectorConfiguration config = (DynamoDBStreamsConnectorConfiguration) configuration;

                if (isNotBlank(config.REPLICATED_FLAG)){
                    Map<String, AttributeValue> image = null;
                    if (record.getDynamodb() != null) {
                        image = record.getDynamodb().getNewImage();
                    }
                    if (image != null) {
                        AttributeValue flagValue = image.get(config.REPLICATED_FLAG);
                        if (flagValue == null) {
                            image.put(config.REPLICATED_FLAG, new AttributeValue().withBOOL(true));
                            return true;
                        } else {
                            Boolean flag = flagValue.getBOOL();
                            if (flag !=null && flag) {
                                return false;
                            }
                        }
                    }
                }
                return true;
            }
        };
    }

}
