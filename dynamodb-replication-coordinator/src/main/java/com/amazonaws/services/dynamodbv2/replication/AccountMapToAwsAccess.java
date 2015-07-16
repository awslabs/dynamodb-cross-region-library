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
package com.amazonaws.services.dynamodbv2.replication;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class to allow access to AWS resources such as DynamoDB and CloudWatch, across different accounts
 *
 */
public class AccountMapToAwsAccess {
    /*
     * Map of account numbers to AwsAccess classes
     */
    private final Map<String, AwsAccess> accountsToAwsAccess;

    public AccountMapToAwsAccess() {
        accountsToAwsAccess = new ConcurrentHashMap<String, AwsAccess>();
    }

    public AccountMapToAwsAccess(Map<String, AwsAccess> accounts) {
        this.accountsToAwsAccess = new ConcurrentHashMap<String, AwsAccess>(accounts);
    }

    public AwsAccess getAccessAccount(String accountNumber) {
        return accountsToAwsAccess.get(accountNumber);
    }

    public void addAwsAccessAccount(String accountNumber, AwsAccess awsAccessAccount) {
        accountsToAwsAccess.put(accountNumber, awsAccessAccount);
    }
}
