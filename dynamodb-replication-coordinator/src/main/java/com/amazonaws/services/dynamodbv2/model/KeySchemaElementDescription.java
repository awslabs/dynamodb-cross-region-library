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

/**
 * A description of a DynamoDB {@link KeySchemaElement}.
 */
@Data
@DynamoDBDocument
public class KeySchemaElementDescription {

    /**
     * Converts a list of DynamoDB {@link KeySchemaElement} to a list of {@link KeySchemaElementDescription} by deep copy.
     *
     * @param keySchema
     *            The list of {@link KeySchemaElement} to convert
     * @return A deep copy of the list converted to {@link KeySchemaElementDescription}
     */
    public static List<KeySchemaElementDescription> convertToKeySchemaElementDescriptions(List<KeySchemaElement> keySchema) {
        if (null == keySchema) {
            return null;
        }
        List<KeySchemaElementDescription> keySchemaDescriptions = new ArrayList<KeySchemaElementDescription>(keySchema.size());
        for (KeySchemaElement kse : keySchema) {
            keySchemaDescriptions.add(new KeySchemaElementDescription(kse));
        }
        return keySchemaDescriptions;
    }

    /**
     * Converts a list of {@link KeySchemaElementDescription} to a list of {@link KeySchemaElement} by deep copy.
     *
     * @param keySchemaDescriptions
     *            The list of {@link KeySchemaElementDescription} to convert
     * @return A deep copy of the list converted to {@link KeySchemaElement}
     */
    public static List<KeySchemaElement> convertToKeySchemaElements(List<KeySchemaElementDescription> keySchemaDescriptions) {
        if (null == keySchemaDescriptions) {
            return null;
        }
        List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>(keySchemaDescriptions.size());
        for (KeySchemaElementDescription ksed : keySchemaDescriptions) {
            keySchema.add(new KeySchemaElement().withAttributeName(ksed.getAttributeName()).withKeyType(ksed.getKeyType()));
        }
        return keySchema;
    }

    /**
     * Performs a deep copy of the list of {@link KeySchemaElementDescription}.
     *
     * @param toCopy
     *            The list of {@link KeySchemaElementDescription} to copy
     * @return A deep copy of the list
     */
    public static List<KeySchemaElementDescription> copyKeySchemaDescriptions(List<KeySchemaElementDescription> toCopy) {
        if (null == toCopy) {
            return null;
        }
        List<KeySchemaElementDescription> copy = new ArrayList<KeySchemaElementDescription>(toCopy.size());
        for (KeySchemaElementDescription ksed : toCopy) {
            copy.add(new KeySchemaElementDescription(ksed));
        }
        return copy;
    }

    /**
     * Name of the key attribute.
     */
    @DynamoDBAttribute(attributeName = Constants.ATTRIBUTE_NAME)
    private String attributeName;

    /**
     * Type of the key.
     */
    @DynamoDBAttribute(attributeName = Constants.KEY_TYPE)
    private String keyType;

    /**
     * Default constructor.
     */
    public KeySchemaElementDescription() {

    }

    /**
     * Constructor with parameterized attribute name and key type.
     *
     * @param attributeName
     *            attribute name of the key schema
     * @param keyType
     *            key type of the key schema
     */
    public KeySchemaElementDescription(String attributeName, String keyType) {
        this.attributeName = attributeName;
        this.keyType = keyType;
    }

    /**
     * Copy constructor for converting from a DynamoDB {@link KeySchemaElement}.
     *
     * @param keySchemaElement
     *            The DynamoDB {@link KeySchemaElement} to copy.
     */
    public KeySchemaElementDescription(KeySchemaElement keySchemaElement) {
        if (null == keySchemaElement) {
            return;
        }
        setAttributeName(keySchemaElement.getAttributeName());
        setKeyType(keySchemaElement.getKeyType());
    }

    /**
     * Copy constructor.
     *
     * @param toCopy
     *            {@link KeySchemaElementDescription} to copy.
     */
    public KeySchemaElementDescription(KeySchemaElementDescription toCopy) {
        if (null == toCopy) {
            return;
        }
        setAttributeName(toCopy.getAttributeName());
        setKeyType(toCopy.getKeyType());
    }

    /**
     * Converts a {@link KeySchemaElementDescription} to a DynamoDB {@link KeySchemaElement}.
     *
     * @return DynamoDB {@link KeySchemaElement} form of the {@link KeySchemaElementDescription}
     */
    public KeySchemaElement toKeySchemaElement() {
        return new KeySchemaElement(attributeName, keyType);
    }

    /**
     * Sets the key name and returns a reference to this for chained calls.
     *
     * @param attributeName
     *            Name of the key.
     * @return A reference to this instance for chained calls
     */
    public KeySchemaElementDescription withAttributeName(String attributeName) {
        setAttributeName(attributeName);
        return this;
    }

    /**
     * Sets the key type and returns a references to this instance for chained calls.
     *
     * @param keyType
     *            Key type
     * @return A reference to this instance for chained calls
     */
    public KeySchemaElementDescription withKeyType(String keyType) {
        setKeyType(keyType);
        return this;
    }
}
