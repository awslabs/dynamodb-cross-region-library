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
