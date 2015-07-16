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
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshaller;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshalling;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Representation of a DynamoDB {@link Projection}.
 */
@Data
@DynamoDBDocument
public class ProjectionDescription {

    /**
     * Marshaller for the {@link DynamoDBMapper}.
     */
    public static class ProjectionTypeMarshaller implements DynamoDBMarshaller<ProjectionType> {

        @Override
        public String marshall(ProjectionType getterReturnResult) {
            return getterReturnResult.toString();
        }

        @Override
        public ProjectionType unmarshall(Class<ProjectionType> clazz, String obj) {
            return ProjectionType.valueOf(obj);
        }

    }

    @DynamoDBAttribute(attributeName = Constants.NON_KEY_ATTRIBUTES)
    @JsonProperty(Constants.NON_KEY_ATTRIBUTES)
    private List<String> nonKeyAttributes;

    @DynamoDBAttribute(attributeName = Constants.PROJECTION_TYPE)
    @DynamoDBMarshalling(marshallerClass = ProjectionTypeMarshaller.class)
    @JsonProperty(Constants.PROJECTION_TYPE)
    private String projectionType;

    /**
     * Default constructor for Jackson.
     */
    public ProjectionDescription() {
    }

    /**
     * Copy constructor. Makes a deep copy of a DynamoDB {@link Projection}.
     *
     * @param projection
     *            A DynamoDB {@link Projection}
     */
    public ProjectionDescription(Projection projection) {
        if (null == projection) {
            return;
        }
        setNonKeyAttributes(projection.getNonKeyAttributes());
        setProjectionType(projection.getProjectionType());
    }

    /**
     * Copy constructor. Makes a deep copy.
     *
     * @param toCopy
     *            {@link ProjectionDescription} to copy
     */
    public ProjectionDescription(ProjectionDescription toCopy) {
        if (null == toCopy) {
            return;
        }
        setNonKeyAttributes(toCopy.getNonKeyAttributes());
        setProjectionType(toCopy.getProjectionType());
    }

    /**
     * Converts this {@link ProjectionDescription} to a DynamoDB {@link Projection}.
     *
     * @return A DynamoDB {@link Projection} form of this {@link ProjectionDescription}
     */
    public Projection toProjection() {
        // Deep copy
        if (nonKeyAttributes != null) {
            return new Projection().withNonKeyAttributes(new ArrayList<String>(nonKeyAttributes)).withProjectionType(projectionType);
        }
        return new Projection().withProjectionType(projectionType);
    }

    /**
     * Sets the non-key attributes of the projection and returns a reference to this instance for chained calls.
     *
     * @param nonKeyAttributes
     *            The non-key attributes of this projection
     * @return A reference to this instance for chained calls
     */
    public ProjectionDescription withNonKeyAttributes(List<String> nonKeyAttributes) {
        setNonKeyAttributes(nonKeyAttributes);
        return this;
    }

    /**
     * Sets the projection type and returns a reference to this instance for chained calls.
     *
     * @param projectionType
     *            The projection type
     * @return A reference to this instance for chained calls
     */
    public ProjectionDescription withProjectionType(String projectionType) {
        setProjectionType(projectionType);
        return this;
    }
}
