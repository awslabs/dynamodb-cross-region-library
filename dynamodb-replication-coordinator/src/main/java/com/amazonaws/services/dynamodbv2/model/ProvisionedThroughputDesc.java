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
package com.amazonaws.services.dynamodbv2.model;

import lombok.Data;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBDocument;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A representation of DynamoDB {@link ProvisionedThroughput}.
 */
@Data
@DynamoDBDocument
public class ProvisionedThroughputDesc {

    @DynamoDBAttribute(attributeName = Constants.READ_CAPACITY_UNITS)
    @JsonProperty(Constants.READ_CAPACITY_UNITS)
    private Long readCapacityUnits;
    @DynamoDBAttribute(attributeName = Constants.WRITE_CAPACITY_UNITS)
    @JsonProperty(Constants.WRITE_CAPACITY_UNITS)
    private Long writeCapacityUnits;

    /**
     * Default constructor for Jackson.
     */
    public ProvisionedThroughputDesc() {
    }

    /**
     * Constructor with parameterized read and write capacity units
     *
     * @param readCapacityUnits
     *            provisioned read capacity units
     * @param writeCapacityUnits
     *            provisioned write capacity units
     */
    public ProvisionedThroughputDesc(Long readCapacityUnits, Long writeCapacityUnits) {
        this.readCapacityUnits = readCapacityUnits;
        this.writeCapacityUnits = writeCapacityUnits;
    }

    /**
     * Copy constructor for a DynamoDB {@link ProvisionedThroughput}.
     *
     * @param provisionedThroughput
     *            The DynamoDB {@link ProvisionedThroughput} to copy
     */
    public ProvisionedThroughputDesc(ProvisionedThroughput provisionedThroughput) {
        if (null == provisionedThroughput) {
            return;
        }
        setReadCapacityUnits(provisionedThroughput.getReadCapacityUnits());
        setWriteCapacityUnits(provisionedThroughput.getWriteCapacityUnits());
    }

    /**
     * Copy constructor.
     *
     * @param toCopy
     *            The {@link ProvisionedThroughputDesc} to copy
     */
    public ProvisionedThroughputDesc(ProvisionedThroughputDesc toCopy) {
        if (null == toCopy) {
            return;
        }
        setReadCapacityUnits(toCopy.getReadCapacityUnits());
        setWriteCapacityUnits(toCopy.getWriteCapacityUnits());
    }

    /**
     * Converts this instance to a DynamoDB {@link ProvisionedThroughput}.
     *
     * @return The DynamoDB form of this {@link ProvisionedThroughput}
     */
    public ProvisionedThroughput toProvisionedThroughput() {
        return new ProvisionedThroughput(readCapacityUnits, writeCapacityUnits);
    }

    /**
     * Sets the read capacity units and returns a reference to this instance for chained calls.
     *
     * @param readCapacityUnits
     *            Number of read capacity units
     * @return A reference to this instance for chained calls
     */
    public ProvisionedThroughputDesc withReadCapacityUnits(Long readCapacityUnits) {
        setReadCapacityUnits(readCapacityUnits);
        return this;
    }

    /**
     * Sets the write capacity units and returns a reference to this instance for chained calls.
     *
     * @param writeCapacityUnits
     *            Number of write capacity units
     * @return A reference to this instance for chained calls
     */
    public ProvisionedThroughputDesc withWriteCapacityUnits(Long writeCapacityUnits) {
        setWriteCapacityUnits(writeCapacityUnits);
        return this;
    }
}
