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
package com.amazonaws.services.dynamodbv2.replication.server.api;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Custom Object Mapper class for replication server so that conflicts between attribute setters can be resolved.
 *
 */
public class DynamoDBReplicationObjectMapper {

    /**
     * Static instance of an object mapper for parsing replication server requests
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.addMixInAnnotations(KeySchemaElement.class, IgnoreSetKeyTypeMixIn.class);
        MAPPER.addMixInAnnotations(AttributeDefinition.class, IgnoreSetAttributeTypeMixIn.class);
        MAPPER.addMixInAnnotations(Projection.class, IgnoreSetProjectionTypeMixIn.class);
        MAPPER.addMixInAnnotations(ProvisionedThroughput.class, setProvisionedThroughputMixIn.class);
    }

    /**
     * Getter for private ObjectMapper instance
     * @return ObjectMapper with necessary mixins for parsing replication server requests
     */
    public static ObjectMapper getInstance() {
        return MAPPER;
    }

    /*
     * Class to mixin to CreateReplicationGroupRequest so that one of the conflicting setKeyType methods can be ignored.
     */
    private abstract class IgnoreSetKeyTypeMixIn {
        @JsonIgnore
        public abstract void setKeyType(KeyType keyType);

        @JsonProperty("KeyType")
        public abstract void setKeyType(String keyType);

        @JsonProperty("AttributeName")
        public abstract void setAttributeName(String attributeName);
    }

    /*
     * Class to mixin so that one of the conflicting setAttributeType and setAttributeName methods can be ignored.
     */
    private abstract class IgnoreSetAttributeTypeMixIn {
        @JsonIgnore
        public abstract void setAttributeType(ScalarAttributeType attributeType);

        @JsonProperty("AttributeType")
        public abstract void setAttributeType(String attributeType);

        @JsonProperty("AttributeName")
        public abstract void setAttributeName(String attributeName);
    }

    /*
     * Class to mixin so that one of the conflicting setProjectionType methods can be ignored.
     */
    private abstract class IgnoreSetProjectionTypeMixIn {
        @JsonIgnore
        public abstract void setProjectionType(ProjectionType projectionType);

        @JsonProperty("ProjectionType")
        public abstract void setProjectionType(String attributeType);
    }

    /*
     * Class to mixin so that one of the conflicting setReadCapacityUnits and setWriteCapacityUnits methods can be ignored.
     */
    private abstract class setProvisionedThroughputMixIn {
        @JsonProperty("ReadCapacityUnits")
        public abstract void setReadCapacityUnits(Long readCapacityUnits);

        @JsonProperty("WriteCapacityUnits")
        public abstract void setWriteCapacityUnits(Long writeCapacityUnits);
    }
}
