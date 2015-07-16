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
package com.amazonaws.services.dynamodbv2.tablecopy.client.tablecopy.workflow;

/**
 * Table copy steps
 */
public class TableCopySteps {

    public static final String START = "START";
    public static final String WAIT = "WAITING";
    public static final String CLEANUP = "CLEANUP";
    public static final String CALLBACK = "CALLBACK";
    public static final String COMPLETE = "COMPLETED";
    public static final String FAILED = "FAILED";

}
