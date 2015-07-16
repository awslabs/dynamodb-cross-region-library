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
 * A description of a DynamoDB {@link AttributeDefinition}.
 */
@Data
@DynamoDBDocument
public class AttributeDefinitionDescription {
    /**
     * Converts a list of {@link AttributeDefinitionDescription} to a list of DynamoDB {@link AttributeDefinition} by a deep copy.
     *
     * @param toConvert
     *            The list of {@link AttributeDefinition} to convert
     * @return A deep copy of the list of {@link AttributeDefinition} as a list of {@link AttributeDefinitionDescription}
     */
    public static List<AttributeDefinition> convertToAttributeDefinitions(List<AttributeDefinitionDescription> toConvert) {
        if (null == toConvert) {
            return null;
        }
        List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>(toConvert.size());
        for (AttributeDefinitionDescription add : toConvert) {
            attributeDefinitions.add(add.toAttributeDefinition());
        }
        return attributeDefinitions;
    }

    /**
     * Converts a list of DynamoDB {@link AttributeDefinition} to a list of {@link AttributeDefinitionDescription} by a deep copy.
     *
     * @param toConvert
     *            The list of {@link AttributeDefinition} to convert
     * @return A deep copy of the list of {@link AttributeDefinition} as a list of {@link AttributeDefinitionDescription}.
     */
    public static List<AttributeDefinitionDescription> convertToAttributeDefintionDescriptions(List<AttributeDefinition> toConvert) {
        if (null == toConvert) {
            return null;
        }
        List<AttributeDefinitionDescription> attributeDefinitionDescriptions = new ArrayList<AttributeDefinitionDescription>(toConvert.size());
        for (AttributeDefinition ad : toConvert) {
            attributeDefinitionDescriptions.add(new AttributeDefinitionDescription(ad));
        }
        return attributeDefinitionDescriptions;
    }

    /**
     * Makes a deep copy of a list of {@link AttributeDefinitionDescription}.
     *
     * @param toCopy
     *            The list of {@link AttributeDefinitionDescription} to copy
     * @return A deep copy of the list.
     */
    public static List<AttributeDefinitionDescription> copyAttributeDefinitionDescriptions(List<AttributeDefinitionDescription> toCopy) {
        if (null == toCopy) {
            return null;
        }
        List<AttributeDefinitionDescription> copy = new ArrayList<AttributeDefinitionDescription>(toCopy.size());
        for (AttributeDefinitionDescription add : toCopy) {
            copy.add(new AttributeDefinitionDescription(add));
        }
        return copy;
    }

    /**
     * Name of the DynamoDB attribute.
     */
    @DynamoDBAttribute(attributeName = Constants.ATTRIBUTE_NAME)
    @JsonProperty(Constants.ATTRIBUTE_NAME)
    private String attributeName;

    /**
     * Type of the DynamoDB attribute.
     */
    @DynamoDBAttribute(attributeName = Constants.ATTRIBUTE_TYPE)
    @JsonProperty(Constants.ATTRIBUTE_TYPE)
    private String attributeType;

    /**
     * Default constructor for Jackson.
     */
    public AttributeDefinitionDescription() {
    }

    /**
     * Constructor with parameterized attribute name and attribute type
     *
     * @param attributeName
     *            name of the attribute
     * @param attributeType
     *            type for the attribute
     */
    public AttributeDefinitionDescription(String attributeName, String attributeType) {
        this.attributeName = attributeName;
        this.attributeType = attributeType;
    }

    /**
     * Copy constructor for converting from a DynamoDB {@link AttributeDefinition}.
     *
     * @param attributeDefinition
     *            The DynamoDB {@link AttributeDefinition} to copy
     */
    public AttributeDefinitionDescription(AttributeDefinition attributeDefinition) {
        if (null == attributeDefinition) {
            return;
        }
        setAttributeName(attributeDefinition.getAttributeName());
        setAttributeType(attributeDefinition.getAttributeType());
    }

    /**
     * Copy constructor.
     *
     * @param toCopy
     *            The {@link AttributeDefinitionDescription} to copy
     */
    public AttributeDefinitionDescription(AttributeDefinitionDescription toCopy) {
        if (null == toCopy) {
            return;
        }
        setAttributeName(toCopy.getAttributeName());
        setAttributeType(toCopy.getAttributeType());
    }

    /**
     * Converts an {@link AttributeDefinitionDescription} to a DynamoDB {@link AttributeDefinition}.
     *
     * @return The DynamoDB {@link AttributeDefinition} representation of this {@link AttributeDefinitionDescription}
     */
    public AttributeDefinition toAttributeDefinition() {
        return new AttributeDefinition(attributeName, attributeType);
    }

    /**
     * Sets the attribute name and returns a reference to this for chained calls.
     *
     * @param attributeName
     *            Name of the attribute
     * @return A reference to this instance for chained calls
     */
    public AttributeDefinitionDescription withAttributeName(String attributeName) {
        setAttributeName(attributeName);
        return this;
    }

    /**
     * Sets the attribute type and returns a reference to this for chained calls.
     *
     * @param attributeType
     *            Type of the attribute
     * @return A reference to this instance for chained calls
     */
    public AttributeDefinitionDescription withAttributeType(String attributeType) {
        setAttributeType(attributeType);
        return this;
    }

}
