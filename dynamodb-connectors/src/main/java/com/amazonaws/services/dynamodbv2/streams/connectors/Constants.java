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

/**
 * Constants for the DynamoDB Connectors framework.
 */
public final class Constants {
    /**
     * The user write flag used for multi-master replication to determine if a write was made by the user or the
     * replication logic.
     */
    public static final String USER_WRITE = "user_write";

    /**
     * Private constructor for a utility class.
     */
    private Constants() {
    }
}
