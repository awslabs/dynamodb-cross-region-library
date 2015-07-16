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

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshaller;

/**
 * Specifies the replication mode for DynamoDB Stream processing.
 */
public enum DynamoDBConnectorType {
    /**
     * Mode where writes are to a single master table in the replication group and changes are applied to read replica tables.
     */
    SINGLE_MASTER_TO_READ_REPLICA;

    /**
     * Marshaller for {@link DynamoDBMapper}.
     */
    public static class DynamoDBConnectorTypeMarshaller implements DynamoDBMarshaller<DynamoDBConnectorType> {

        @Override
        public String marshall(DynamoDBConnectorType getterReturnResult) {
            return getterReturnResult.toString();
        }

        @Override
        public DynamoDBConnectorType unmarshall(Class<DynamoDBConnectorType> clazz, String obj) {
            return DynamoDBConnectorType.valueOf(obj);
        }

    }
}
