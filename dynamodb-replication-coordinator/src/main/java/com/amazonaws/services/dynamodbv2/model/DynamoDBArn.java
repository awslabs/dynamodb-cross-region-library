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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.Data;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBDocument;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Representation of an arn for a DynamoDB table
 *
 * @see http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#arn-syntax-ddb
 */
@Data
@DynamoDBDocument
public class DynamoDBArn implements Comparable<DynamoDBArn> {

    /*
     * Constants for parsing
     */
    private static final String TABLE_RESOURCE_TYPE = "table";
    private static final String SERVICE = "dynamodb";
    private static final String PARTITION = "aws";
    private static final String ARN = "arn";
    private static final String COLON = ":";
    private static final String SLASH = "/";

    private static final String REGION_GROUP = "region";
    private static final String ACCOUNT_NUMBER_GROUP = "acctNo";
    private static final String TABLE_NAME_GROUP = "tableName";
    private static final String TABLE_NAME_REGEX = "(?<" + TABLE_NAME_GROUP + ">[a-zA-Z0-9-_\\.]{3,255})";
    private static final String ACCOUNT_NUMBER_REGEX = "(?<" + ACCOUNT_NUMBER_GROUP + ">\\d{12})";
    private static final String REGION_REGEX = "(?<" + REGION_GROUP + ">[a-z]+-[a-z]+-[0-9]+)";

    private static final Pattern ARN_REGEX = Pattern.compile("^" + ARN + COLON + PARTITION + COLON + SERVICE + COLON + REGION_REGEX + COLON
        + ACCOUNT_NUMBER_REGEX + COLON + TABLE_RESOURCE_TYPE + SLASH + TABLE_NAME_REGEX + "$");

    @DynamoDBIgnore
    @JsonIgnore
    private String accountNumber;
    @DynamoDBIgnore
    @JsonIgnore
    private String region;
    @DynamoDBIgnore
    @JsonIgnore
    private String tableName;

    /**
     * Default constructor for Jackson Processor
     */
    public DynamoDBArn() {

    }

    /**
     * Constructor with existing arn parameter
     *
     * @param arn
     *            existing arn to copy from
     */
    public DynamoDBArn(DynamoDBArn arn) {
        accountNumber = arn.getAccountNumber();
        region = arn.getRegion();
        tableName = arn.getTableName();
    }

    @DynamoDBAttribute(attributeName = Constants.ARN)
    @JsonProperty(Constants.ARN)
    public String getArnString() {
        return ARN + COLON + PARTITION + COLON + SERVICE + COLON + region + COLON + accountNumber + COLON + TABLE_RESOURCE_TYPE + SLASH + tableName;
    }

    @DynamoDBAttribute(attributeName = Constants.ARN)
    @JsonProperty(Constants.ARN)
    public void setArnString(String arn) {
        Matcher matcher = ARN_REGEX.matcher(arn);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid ARN string, does not match standard format: " + arn);
        }
        region = matcher.group(REGION_GROUP);
        accountNumber = matcher.group(ACCOUNT_NUMBER_GROUP);
        tableName = matcher.group(TABLE_NAME_GROUP);
    }

    @Override
    public int compareTo(DynamoDBArn compareARN) {
        if (accountNumber.equals(compareARN.accountNumber)) {
            if (region.equals(compareARN.region)) {
                return tableName.compareTo(compareARN.tableName);
            } else {
                return region.compareTo(compareARN.region);
            }
        } else {
            return accountNumber.compareTo(compareARN.accountNumber);
        }
    }

    public DynamoDBArn withArnString(String arnString) {
        setArnString(arnString);
        return this;
    }

    public DynamoDBArn withAccountNumber(String accountNumber) {
        setAccountNumber(accountNumber);
        return this;
    }

    public DynamoDBArn withRegion(String region) {
        setRegion(region);
        return this;
    }

    public DynamoDBArn withTableName(String tableName) {
        setTableName(tableName);
        return this;
    }
}
