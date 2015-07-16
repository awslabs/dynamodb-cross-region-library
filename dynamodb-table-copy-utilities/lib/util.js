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
var AWS = require('aws-sdk');
var DOC = require('dynamodb-doc');
var http = require('http');
var https = require('https');
var dynamoDBClients = {};
var s3Client;
var Constants = require('./constants');
var async = require('async');
var winston = require('winston');
var logger;

var getLogger = function(){
    if (!logger){
        var options = {
            colorize: true,
            timestamp: true,
            level: process.env['LOG_LEVEL']
        };
        winston.remove(winston.transports.Console)
        winston.add(winston.transports.Console, options)
        logger = winston;
    }
    return logger;
};

logger = getLogger();

var getS3Client = function(){
    if (!s3Client){
        var keepAliveAgent = new https.Agent({ keepAlive: true });
        s3Client = new AWS.S3({
            httpOptions: { agent: keepAliveAgent },
        });
    }
    return s3Client;
};


var getDynamoDBClient = function(region, endpoint){
    if (!region){
        region = Constants.DYNAMODB_DEFAULT_REGION
    }
    if (!dynamoDBClients[region+endpoint]){
        var params = {
            region: region,
        };
        var useSsl = true;
        if (endpoint){
            params.endpoint = endpoint;
            if (endpoint.indexOf("http://") == 0){
                useSsl = false;
            }
        }
        if (useSsl){
            params.httpOptions = {
                agent: new https.Agent({ keepAlive: true})
            };
        } else {
            params.httpOptions = {
                agent: new http.Agent({ keepAlive: true})
            };            
        }
        dynamoDBClients[region] = new DOC.DynamoDB(new AWS.DynamoDB(params));
    }
    return dynamoDBClients[region];
};

var parseS3Url = function(url){
    logger.debug(url);
    if(url && url.match(/s3:\/\/([^\/]+)\/{0,1}(.*)/)){
        return {
            bucketName: RegExp.$1,
            objectPrefix: RegExp.$2
        };
    } else {
        return null;
    }
};

var parseRedisUrl = function(url){
    logger.debug(url);
    var redisInfo = {
        host: 'localhosst',
        port: 6379,
        database: 0
    };
    if(url && url.match(/redis:\/\/([^\/:]+):{0,1}([0-9]*)\/{0,1}([0-9]*)/)){
        if (RegExp.$1){
            redisInfo.host = RegExp.$1;
        }
        if (RegExp.$2){
            redisInfo.port = RegExp.$2;
        }
        if (RegExp.$3){
            redisInfo.database = RegExp.$3;
        }
        return redisInfo;
    } else {
        return null;
    }
};


var parseDynamoDBTableName = function(table){
    if(table && table.match(/([a-z]+-[a-z]+-[0-9]?):(.+)/)){
        return {
            region: RegExp.$1,
            tableName: RegExp.$2
        };
    } else {
        return {
            region: Constants.DYNAMODB_DEFAULT_REGION,
            tableName: table
        };
    }
};

var getSegments = function(workerId, nWorkers, totalSegments){
    var segments = [];
    for (var i = workerId; i < totalSegments; i += nWorkers){
        segments.push(i);
    }
    return segments;
};

var getContentMetadata = function(bucket, key, callback){
    var self = this;
    var s3 = getS3Client();
    var metadata = {};

    if (endsWith(key, Constants.SUFFIX_GZIP)){
        metadata.contentEncoding = Constants.ENCODING_GZIP;
    } 
    var params = {
        Bucket: bucket,
        Key: key
    };
    s3.headObject(params, function(err, data){
        if (!err){
            if (data.ContentEncoding){
                metadata.contentEncoding = data.ContentEncoding;
            }
            metadata.contentLength = data.ContentLength;
            logger.debug('Content Encoding is %s', metadata.contentEncoding);
            logger.debug('Content Length is %d', metadata.contentLength);
            callback(null, metadata);
        } else {
            callback(err);
        }
    });
};

var describeTable = function(target, callback){
    if (target && target.tableName){
        var dynamodb = getDynamoDBClient(target.region);
        var params = {
            TableName: target.tableName
        };
        dynamodb.describeTable(params, callback);
    } else {
        callback('Table name is not set');
    }
};

var endsWith = function(string, suffix) {
    return string.indexOf(suffix, string.length - suffix.length) !== -1;
};

var ExponentialBackoff = function(defaultBackoffTime, maxBackoffTime){
    this.defaultBackoffTime = defaultBackoffTime;
    this.backoffTime = defaultBackoffTime;
    this.maxBackoffTime = maxBackoffTime;
};

ExponentialBackoff.prototype.execute = function(callback) {
    var self = this;
    self.backoffTime *= 2;
    if (self.backoffTime > self.maxBackoffTime){
        self.backoffTime = self.maxBackoffTime;
    }
    logger.debug('Backoff for for %d ms', self.backoffTime);
    setTimeout(function(){
        callback(null);
    }, self.backoffTime * Math.random());        
};

ExponentialBackoff.prototype.skip = function(callback) {
    var self = this;
    if (self.backoffTime > self.defaultBackoffTime){
        logger.debug('Reducing backoff time to %d ms', self.backoffTime / 2);
        self.backoffTime /= 2;
    }
    callback(null);
};


module.exports = {
    logger: logger,
    getLogger: getLogger,
    getS3Client: getS3Client,
    getDynamoDBClient: getDynamoDBClient,
    parseS3Url: parseS3Url,
    parseRedisUrl: parseRedisUrl,
    describeTable: describeTable,
    parseDynamoDBTableName: parseDynamoDBTableName,
    getSegments: getSegments,
    getContentMetadata: getContentMetadata,
    ExponentialBackoff: ExponentialBackoff
};
