/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streams.connectors;

import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Custom Object Mapper class for consuming DynamoDB Streams records so that conflicts between parameter setters can be resolved.
 */
public class DynamoDBStreamsRecordObjectMapper {

    /*
     * Singleton instance of the mapper
     */
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .addMixIn(com.amazonaws.services.dynamodbv2.model.Record.class, IgnoreSetEventNameMixIn.class)
            .addMixIn(com.amazonaws.services.dynamodbv2.model.StreamRecord.class, IgnoreSetStreamViewTypeMixIn.class);

    /**
     * Getter for private ObjectMapper instance
     *
     * @return ObjectMapper with necessary mixins for parsing replication server requests
     */
    public static ObjectMapper getInstance() {
        return MAPPER;
    }

    /*
     * Class to mixin to DynamoDB record so that one of the conflicting setEventName methods can be ignored.
     */
    private abstract class IgnoreSetEventNameMixIn {
        @JsonIgnore
        public abstract void setEventName(OperationType eventName);

        @JsonProperty("eventName")
        public abstract void setEventName(String eventName);
    }

    /*
     * Class to mixin to DynamoDB Stream record so that one of the conflicting setStreamViewType methods can be ignored
     */
    private abstract class IgnoreSetStreamViewTypeMixIn {
        @JsonIgnore
        public abstract void setStreamViewType(StreamViewType streamViewType);

        @JsonProperty("streamViewType")
        public abstract void setStreamViewType(String streamViewType);
    }
}
