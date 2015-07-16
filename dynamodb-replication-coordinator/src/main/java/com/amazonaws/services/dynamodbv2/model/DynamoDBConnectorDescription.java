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

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBDocument;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Description and configuration of a DynamoDB Connector.
 */
@Data
@DynamoDBDocument
public class DynamoDBConnectorDescription {

    /**
     * Makes a deep copy of a list of connector descriptions.
     *
     * @param toCopy
     *            List of connector descriptions to deep copy
     * @return A deep copy of the list of connector descriptions
     */
    public static List<DynamoDBConnectorDescription> copyConnectors(List<DynamoDBConnectorDescription> toCopy) {
        if (null == toCopy) {
            return null;
        }
        List<DynamoDBConnectorDescription> connectors = new ArrayList<DynamoDBConnectorDescription>(toCopy.size());
        for (DynamoDBConnectorDescription connector : toCopy) {
            connectors.add(new DynamoDBConnectorDescription(connector));
        }
        return connectors;
    }

    /**
     * Source DynamoDB table endpoint.
     */
    @DynamoDBAttribute(attributeName = Constants.SOURCE_TABLE_ENDPOINT)
    @JsonProperty(Constants.SOURCE_TABLE_ENDPOINT)
    private String sourceTableEndpoint;

    /**
     * Source DynamoDB table ARN.
     *
     * @see http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#arn-syntax-ddb
     */
    @DynamoDBAttribute(attributeName = Constants.SOURCE_TABLE_ARN)
    @JsonProperty(Constants.SOURCE_TABLE_ARN)
    private String sourceTableArn;

    /**
     * Default constructor for Jackson.
     */
    public DynamoDBConnectorDescription() {
    }

    /**
     * Copy constructor. Makes a deep copy of the provided {@link DynamoDBConnectorDescription}.
     *
     * @param toCopy
     *            The connector description to make a deep copy of
     */
    public DynamoDBConnectorDescription(DynamoDBConnectorDescription toCopy) {
        if (null == toCopy) {
            return;
        }
        // Immutable
        setSourceTableArn(toCopy.getSourceTableArn());
        // Immutable
        setSourceTableEndpoint(toCopy.getSourceTableEndpoint());
    }

    /**
     * Returns true if all required fields are specified.
     *
     * @return True if all required fields are specified, otherwise false
     */
    @DynamoDBIgnore
    @JsonIgnore
    public boolean isValid() {
        return null != sourceTableEndpoint && null != sourceTableArn;
    }

    /**
     * Sets the source table endpoint and returns a reference to this instance for chained calls.
     *
     * @param sourceTableEndpoint
     *            The source table endpoint
     * @return A reference to this instance for chained calls
     */
    public DynamoDBConnectorDescription withSourceTableEndpoint(String sourceTableEndpoint) {
        setSourceTableEndpoint(sourceTableEndpoint);
        return this;
    }

    /**
     * Sets the source table arn and returns a reference to this instance for chained calls.
     *
     * @param sourceTableArn
     *            The source table arn string
     * @return A reference to this instance for chained calls
     */
    public DynamoDBConnectorDescription withSourceTableArn(String sourceTableArn) {
        setSourceTableArn(sourceTableArn);
        return this;
    }

}
