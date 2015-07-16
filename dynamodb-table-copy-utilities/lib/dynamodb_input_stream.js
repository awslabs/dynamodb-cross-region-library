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
var async = require('async');
var util = require('./util');
var logger = util.getLogger();
var Readable = require('stream').Readable;
var inherits = require('util').inherits;

inherits(DynamoDBInputStream, Readable);

DynamoDBInputStream.DEFAULT_HIGH_WATERMARK = 100;
DynamoDBInputStream.MAX_BATCH_SIZE = 200;
DynamoDBInputStream.DEFAULT_BACKOFF_TIME = 64;
DynamoDBInputStream.MAX_BACKOFF_TIME = 4096;

function DynamoDBInputStream(source, segment, totalSegments, options){
    if (!options){
        options = {};
    }
    options.objectMode = true; // This stream takes DynamoDB item as a chunk
    if (!options.highWaterMark){
        options.highWaterMark = DynamoDBInputStream.DEFAULT_HIGH_WATERMARK;
    }
    Readable.call(this, options);
    this.dynamodb = util.getDynamoDBClient(source.region, source.endpoint);
    this.tableName = source.tableName;
    this.segment = segment;
    this.totalSegments = totalSegments;
    this.backoff = new util.ExponentialBackoff(
        DynamoDBInputStream.DEFAULT_BACKOFF_TIME, 
        DynamoDBInputStream.MAX_BACKOFF_TIME);

    this.lastEvaluatedKey = null;
    this.numRead = 0;
};

DynamoDBInputStream.prototype._read = function(size){
    var self = this;
    var scanResultHandler = function(err, items){
        if (!err){
            var readable = true;
            for (var i in items){
                readable &= self.push(items[i]);
            }
            if (self.eofReached){
                logger.debug('No more key');
                self.push(null);
            } else {
                if (readable){
                    self.scan(size, scanResultHandler);
                } else {
                    logger.debug('Entering idle until next _read() call');
                }
            }
        } else {
            self.emit('error', err);
        }
    }
    self.scan(size, scanResultHandler);
};

DynamoDBInputStream.prototype.scan = function(size, callback) {
    var self = this;
    var limit = size;
    var consistentRead = true;

    if (self.scanning){
        return;
    }
    if (self.eofReached){
        return;
    }

    if (limit > DynamoDBInputStream.MAX_BATCH_SIZE){
        limit = DynamoDBInputStream.MAX_BATCH_SIZE;
    }
    var params = {
        TableName: self.tableName,
        Segment: self.segment,
        TotalSegments: self.totalSegments,
        Limit: limit,
        ConsistentRead: consistentRead
    };
    if (self.lastEvaluatedKey){
        params.ExclusiveStartKey = self.lastEvaluatedKey;
    }
    self.scanning = true;
    self.dynamodb.scan(params, function(err, data){
        self.scanning = false;
        if (!err){
            self.lastEvaluatedKey = data.LastEvaluatedKey;
            self.eofReached = !data.LastEvaluatedKey;
            self.backoff.skip(function(){
                callback(null, data.Items, self.eofReached);
            });
        } else {
            if (err.code === 'ProvisionedThroughputExceededException'
                || err.code === 'InternalServerError'){
                logger.warn('Retry-able exception encountered: ' + err.code);
                self.backoff.execute(function(){
                    self.scan(size, callback);
                });
            } else {
                callback(err);
            }
        }
    });
};

module.exports = DynamoDBInputStream;

