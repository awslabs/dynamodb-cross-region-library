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

 'use strict';
/* global AWS, DynamoDB */
/**
 * @ngdoc function
 * @name IoTHackDayDashboard.service:AWS
 * @description
 * # AWS
 * Service of the IoTHackDayDashboard. It initializes AWS SDK with providing credentials
 * either from Cognito Identity service or environment variables as specified in ENV service.
 * It initializes a DynamoDB client instance. The instance is exposed as AWS.dynamoDB. 
 */
angular.module('IoTHackDayDashboard').
    service('AWS', function ($log, ENV){
		if (ENV.useCognitoIdentity) { // Uses Cognito Idenity to get AWS credentials
  		    // Needs to set us-east-1 as the default to get credentials by Cognito
  		    AWS.config.update({region: 'us-east-1'});
  		    AWS.config.credentials = new AWS.CognitoIdentityCredentials({
  		    	AccountId: ENV.awsAccountId,
  		    	IdentityPoolId: ENV.identityPoolId,
  		    	RoleArn: ENV.unauthRoleArn
  		    });

  		    AWS.config.credentials.getId(function(){
                localStorage.setItem('userid', AWS.config.credentials.params.IdentityId);
                $log.info('AWS credentials initialized with Cognito Identity');
            });
        } else { // Uses environment variables to get AWS credentials.
            AWS.config.credentials = new AWS.Credentials({
                accessKeyId: ENV.accessKeyId,
                secretAccessKey: ENV.secretAccessKey
            });
            $log.info('AWS credentials initialized with environment variables');
            $log.debug('AWS Accesss Key is ' + ENV.accessKeyId);
            localStorage.setItem('userid', ENV.userId);
            $log.debug('User ID is set to ' + localStorage.getItem('userid'));
        }
 
        /**
        * DynamoDB client object.
        */
        AWS.dynamoDB = new DynamoDB(
            new AWS.DynamoDB({
                region: ENV.dynamoDBRegion,
                endpoint: ENV.dynamoDBEndpoint
            }));

        return AWS;
    });
