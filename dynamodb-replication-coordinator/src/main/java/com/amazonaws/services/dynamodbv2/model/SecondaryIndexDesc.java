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
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A representation of DynamoDB {@link LocalSecondaryIndex} and {@link GlobalSecondaryIndex}.
 */
@Data
@DynamoDBDocument
public class SecondaryIndexDesc {

    /**
     * Makes a deep copy of a list of secondary indexes.
     *
     * @param toCopy
     *            List of secondary index
     * @return A deep copy of the provided secondary index
     */
    public static List<SecondaryIndexDesc> copySecondaryIndexes(List<SecondaryIndexDesc> toCopy) {
        if (null == toCopy) {
            return null;
        }
        List<SecondaryIndexDesc> secondaryIndexes = new ArrayList<SecondaryIndexDesc>(toCopy.size());
        for (SecondaryIndexDesc secondaryIndex : toCopy) {
            secondaryIndexes.add(new SecondaryIndexDesc(secondaryIndex));
        }
        return secondaryIndexes;
    }

    public static List<GlobalSecondaryIndex> toGSIList(List<SecondaryIndexDesc> toConvert) {
        if (null == toConvert) {
            return null;
        }
        List<GlobalSecondaryIndex> gsiList = new ArrayList<GlobalSecondaryIndex>();
        for (SecondaryIndexDesc secondaryIndex : toConvert) {
            gsiList.add(secondaryIndex.toGSI());
        }
        return gsiList;
    }

    public static List<LocalSecondaryIndex> toLSIList(List<SecondaryIndexDesc> toConvert) {
        if (null == toConvert) {
            return null;
        }
        List<LocalSecondaryIndex> lsiList = new ArrayList<LocalSecondaryIndex>();
        for (SecondaryIndexDesc secondaryIndex : toConvert) {
            lsiList.add(secondaryIndex.toLSI());
        }
        return lsiList;
    }

    public static List<SecondaryIndexDesc> fromGSIList(List<GlobalSecondaryIndex> toConvert) {
        if (null == toConvert) {
            return null;
        }
        List<SecondaryIndexDesc> indexList = new ArrayList<SecondaryIndexDesc>();
        for (GlobalSecondaryIndex gsi : toConvert) {
            indexList.add(new SecondaryIndexDesc(gsi));
        }
        return indexList;
    }

    public static List<SecondaryIndexDesc> fromLSIList(List<LocalSecondaryIndex> toConvert) {
        if (null == toConvert) {
            return null;
        }
        List<SecondaryIndexDesc> indexList = new ArrayList<SecondaryIndexDesc>();
        for (LocalSecondaryIndex lsi : toConvert) {
            indexList.add(new SecondaryIndexDesc(lsi));
        }
        return indexList;
    }

    @DynamoDBAttribute(attributeName = Constants.INDEX_NAME)
    @JsonProperty(Constants.INDEX_NAME)
    private String indexName;

    @DynamoDBAttribute(attributeName = Constants.KEY_SCHEMA)
    @JsonProperty(Constants.KEY_SCHEMA)
    private List<KeySchemaElementDescription> keySchema;

    @DynamoDBAttribute(attributeName = Constants.PROJECTION)
    @JsonProperty(Constants.PROJECTION)
    private ProjectionDescription projection;

    @DynamoDBAttribute(attributeName = Constants.PROVISIONED_THROUGHPUT)
    @JsonProperty(Constants.PROVISIONED_THROUGHPUT)
    private ProvisionedThroughputDesc provisionedThroughput;

    /**
     * Default constructor for Jackson.
     */
    public SecondaryIndexDesc() {
    }

    /**
     * Copy constructor for a DynamoDB {@link GlobalSecondaryIndex}.
     *
     * @param globalSecondaryIndex
     *            The DynamoDB {@link GlobalSecondaryIndex} to copy
     */
    public SecondaryIndexDesc(GlobalSecondaryIndex globalSecondaryIndex) {
        if (null == globalSecondaryIndex) {
            return;
        }
        // Immutable
        setIndexName(globalSecondaryIndex.getIndexName());
        // Deep copy
        setKeySchema(KeySchemaElementDescription.convertToKeySchemaElementDescriptions(globalSecondaryIndex.getKeySchema()));
        // Deep Copy
        setProjection(new ProjectionDescription(globalSecondaryIndex.getProjection()));
        // Deep Copy
        setProvisionedThroughput(new ProvisionedThroughputDesc(globalSecondaryIndex.getProvisionedThroughput()));
    }

    /**
     * Copy constructor for a DynamoDB {@link LocalSecondaryIndex}.
     *
     * @param localSecondaryIndex
     *            The DynamoDB {@link LocalSecondaryIndex} to copy
     */
    public SecondaryIndexDesc(LocalSecondaryIndex localSecondaryIndex) {
        if (null == localSecondaryIndex) {
            return;
        }
        // Immutable
        setIndexName(localSecondaryIndex.getIndexName());
        // Deep copy
        setKeySchema(KeySchemaElementDescription.convertToKeySchemaElementDescriptions(localSecondaryIndex.getKeySchema()));
        // Deep Copy
        setProjection(new ProjectionDescription(localSecondaryIndex.getProjection()));
    }

    /**
     * Copy constructor.
     *
     * @param toCopy
     *            The {@link SecondaryIndexDesc} to copy.
     */
    public SecondaryIndexDesc(SecondaryIndexDesc toCopy) {
        if (null == toCopy) {
            return;
        }
        // Immutable
        setIndexName(toCopy.getIndexName());
        // Deep copy
        setKeySchema(KeySchemaElementDescription.copyKeySchemaDescriptions(toCopy.getKeySchema()));
        // Deep copy
        setProjection(new ProjectionDescription(toCopy.getProjection()));
        // Deep copy
        setProvisionedThroughput(new ProvisionedThroughputDesc(toCopy.getProvisionedThroughput()));
    }

    /**
     * Converts this {@link SecondaryIndexDesc} to a DynamoDB {@link GlobalSecondaryIndex}.
     *
     * @return The DynamoDB {@link GlobalSecondaryIndex} representation of this {@link SecondaryIndexDesc}
     */
    public GlobalSecondaryIndex toGSI() {
        return new GlobalSecondaryIndex().withIndexName(indexName).withKeySchema(KeySchemaElementDescription.convertToKeySchemaElements(keySchema))
            .withProjection(projection.toProjection()).withProvisionedThroughput(provisionedThroughput.toProvisionedThroughput());
    }

    /**
     * Converts this {@link SecondaryIndexDesc} to a DynamoDB {@link LocalSecondaryIndex}. Please note that the value {@link #getProvisionedThroughput()} is
     * ignored for local secondary indexes.
     *
     * @return The DynamoDB {@link LocalSecondaryIndex} representation of this {@link SecondaryIndexDesc}
     */
    public LocalSecondaryIndex toLSI() {
        return new LocalSecondaryIndex().withIndexName(indexName).withKeySchema(KeySchemaElementDescription.convertToKeySchemaElements(keySchema))
            .withProjection(projection.toProjection());
    }

    /**
     * Sets the index name and returns a reference to this instance for chained calls.
     *
     * @param indexName
     *            The index name
     * @return A reference to this instance for chained calls
     */
    public SecondaryIndexDesc withIndexName(String indexName) {
        setIndexName(indexName);
        return this;
    }

    /**
     * Sets the key schema and returns a reference to this instance for chained calls.
     *
     * @param keySchema
     *            The key schema
     * @return A reference to this instance for chained calls
     */
    public SecondaryIndexDesc withKeySchema(List<KeySchemaElementDescription> keySchema) {
        setKeySchema(keySchema);
        return this;
    }

    /**
     * Sets the projection and returns a reference to this instance for chained calls.
     *
     * @param projection
     *            The projection
     * @return A reference to this instance for chained calls
     */
    public SecondaryIndexDesc withProjection(ProjectionDescription projection) {
        setProjection(projection);
        return this;
    }

    /**
     * Sets the provisioned throughput and returns a reference to this instance for chained calls. This only applies for modeling {@link GlobalSecondaryIndex};
     * for {@link LocalSecondaryIndex}, this field is ignored.
     *
     * @param provisionedThroughput
     *            The provisioned throughput
     * @return A reference to this instance for chained calls
     */
    public SecondaryIndexDesc withProvisionedThroughput(ProvisionedThroughputDesc provisionedThroughput) {
        setProvisionedThroughput(provisionedThroughput);
        return this;
    }

}
