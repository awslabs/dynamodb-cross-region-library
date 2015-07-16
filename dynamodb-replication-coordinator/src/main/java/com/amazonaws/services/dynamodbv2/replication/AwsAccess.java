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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClient;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClient;

/**
 * Provides AWS access to different regions for the coordinator (e.g. DynamoDB, CloudWatch etc.)
 *
 */
public class AwsAccess {
    /*
     * Credentials provider for the account
     */
    private final AWSCredentialsProvider credentialsProvider;

    /*
     * DynamoDB client map, with the endpoint as the key
     */
    private final Map<String, AmazonDynamoDB> dynamoDBClients = new HashMap<String, AmazonDynamoDB>();

    /*
     * CloudWatch client map, with the endpoint as the key
     */
    private final Map<String, AmazonCloudWatch> cloudWatchClients = new HashMap<String, AmazonCloudWatch>();

    /*
     * CloudFormation client map, with the endpoint as the key
     */
    private final Map<String, AmazonCloudFormation> cloudFormationClients = new HashMap<String, AmazonCloudFormation>();

    /*
     * ECS client map, with the endpoint as the key
     */
    private final Map<String, AmazonECS> ecsClients = new HashMap<String, AmazonECS>();

    /**
     * Default constructor
     *
     * @param credentialsProvider
     *            credentials provider for the specific account
     */
    public AwsAccess(AWSCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }

    /**
     * Return corresponding DynamoDB client for the endpoint, create it and add it to the map if it does not exist
     *
     * @param endpoint
     *            DynamoDB endpoint for the client
     * @return DynamoDB client with access to the given endpoint
     */
    public synchronized AmazonDynamoDB getDynamoDB(String endpoint) throws Exception {
        if (!dynamoDBClients.containsKey(endpoint)) {
            AmazonDynamoDB dynamoDB = new AmazonDynamoDBClient(credentialsProvider);
            dynamoDB.setEndpoint(endpoint);
            dynamoDBClients.put(endpoint, dynamoDB);
        }
        return dynamoDBClients.get(endpoint);
    }

    /**
     * Return corresponding CloudWatch client for the endpoint, create it and add it to the map if it does not exist
     *
     * @param endpoint
     *            CloudWatch endpoint for the client
     * @return CloudWatch client with access to the given endpoint
     */
    public synchronized AmazonCloudWatch getCloudWatch(String endpoint) throws Exception {
        if (!cloudWatchClients.containsKey(endpoint)) {
            AmazonCloudWatch cloudWatch = new AmazonCloudWatchClient(credentialsProvider);
            cloudWatch.setEndpoint(endpoint);
            cloudWatchClients.put(endpoint, cloudWatch);
        }
        return cloudWatchClients.get(endpoint);
    }

    /**
     * Return corresponding CloudFormation client for the endpoint, create it and add it to the map if it does not exist
     *
     * @param endpoint
     *            CloudFormation endpoint for the client
     * @return CloudFormation client with access to the given endpoint
     */
    public synchronized AmazonCloudFormation getCloudFormation(String endpoint) throws Exception {
        if (!cloudFormationClients.containsKey(endpoint)) {
            AmazonCloudFormation cloudFormation = new AmazonCloudFormationClient(credentialsProvider);
            cloudFormation.setEndpoint(endpoint);
            cloudFormationClients.put(endpoint, cloudFormation);
        }
        return cloudFormationClients.get(endpoint);
    }

    /**
     * Return corresponding ECS client for the endpoint, create it and add it to the map if it does not exist
     *
     * @param endpoint
     *            ECS endpoint for the client
     * @return ECS client with access to the given endpoint
     */
    public synchronized AmazonECS getECS(String endpoint) throws Exception {
        if (!ecsClients.containsKey(endpoint)) {
            AmazonECS ecsClient = new AmazonECSClient(credentialsProvider);
            ecsClient.setEndpoint(endpoint);
            ecsClients.put(endpoint, ecsClient);
        }
        return ecsClients.get(endpoint);
    }

    /**
     * Shutdown all the clients and remove them as the shutdown happens
     */
    public synchronized void shutdown() {
        Iterator<Entry<String, AmazonDynamoDB>> ddbIt = dynamoDBClients.entrySet().iterator();
        while (ddbIt.hasNext()) {
            Entry<String, AmazonDynamoDB> entry = ddbIt.next();
            entry.getValue().shutdown();
            ddbIt.remove();
        }
        Iterator<Entry<String, AmazonCloudWatch>> cwIt = cloudWatchClients.entrySet().iterator();
        while (cwIt.hasNext()) {
            Entry<String, AmazonCloudWatch> entry = cwIt.next();
            entry.getValue().shutdown();
            cwIt.remove();
        }
        Iterator<Entry<String, AmazonCloudFormation>> cfIt = cloudFormationClients.entrySet().iterator();
        while (cfIt.hasNext()) {
            Entry<String, AmazonCloudFormation> entry = cfIt.next();
            entry.getValue().shutdown();
            cfIt.remove();
        }
        Iterator<Entry<String, AmazonECS>> ecsIt = ecsClients.entrySet().iterator();
        while (ecsIt.hasNext()) {
            Entry<String, AmazonECS> entry = ecsIt.next();
            entry.getValue().shutdown();
            ecsIt.remove();
        }
    }
}
