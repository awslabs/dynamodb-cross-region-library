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

module.exports.getConfigJS = function(host){
   return "angular.module('config', []).constant('ENV', " + getConfig(host) + ");";
}; 

var getConfig = function(host){
  var table = 'master';
  var region = 'us-east-1';
  if (process.env.PORT_OFFSET > 0){
    table = 'replica';
  }
  if (process.env.REGION){
    region = process.env.REGION;
  }
    
  var config = {
        name: 'production',
        useCognitoIdentity: false,
        userId: 'localUser',
        accessKeyId: 'DummyAccessKey',
        secretAccessKey: 'DummySecretKey',
        dynamoDBRegion: region,
        commonTable: table,
        dynamoDBEndpoint: getUrl(host)
   };
   return JSON.stringify(config);
};

var getUrl = function(host){
   return 'http://' + host + '/dynamodb';
};
