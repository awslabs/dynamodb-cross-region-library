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
var Writable = require('stream').Writable;
var inherits = require('util').inherits;

inherits(DynamoDBOutputStream, Writable);

DynamoDBOutputStream.DEFAULT_HIGH_WATERMARK = 50;
DynamoDBOutputStream.MAX_BATCH_SIZE = 25;
DynamoDBOutputStream.DEFAULT_BACKOFF_TIME = 16;
DynamoDBOutputStream.MAX_BACKOFF_TIME = 1024;

function DynamoDBOutputStream(dest, options){
    if (!options){
        options = {};
    }
    options.objectMode = true; // This stream takes DynamoDB item as a chunk
    if (!options.highWaterMark){
        options.highWaterMark = DynamoDBOutputStream.DEFAULT_HIGH_WATERMARK;
    }
    Writable.call(this, options);
    this.dynamodb = util.getDynamoDBClient(dest.region, dest.endpoint);
    this.tableName = dest.tableName;
    this.backoff = new util.ExponentialBackoff(
        DynamoDBOutputStream.DEFAULT_BACKOFF_TIME, 
        DynamoDBOutputStream.MAX_BACKOFF_TIME);

    this.numInFlightRequests = 0;
    this.numWritten = 0;
    this.numBackoff = 0;
};


DynamoDBOutputStream.prototype._write = function(item, encoding, callback) {
    var self = this;
    var params = {
        TableName: self.tableName,
        Item: item
    };
    self.numInFlightRequests++;
    this.dynamodb.putItem(params, function(err, data){
        self.numInFlightRequests--;
        if (!err){
            self.numWritten++;
            self.backoff.skip(function(){
                callback(null);
            });
        } else {
            if (err.code === 'ProvisionedThroughputExceededException'
                || err.code === 'InternalServerError'){
                logger.warn('Retry-able exception encountered: ' + err.code);
                self.backoff.execute(function(){
                    self._write(item, encoding, callback);
                });
            } else {
                self.emit('error', err);
            }
        }
    });
};

DynamoDBOutputStream.prototype._writev = function(chunks, callback) {
    var self = this;
    var items = [];
    for (var i in chunks){
        items.push(chunks[i].chunk);
    }

    self.writeItems(items, callback);
};

DynamoDBOutputStream.prototype.writeItems = function(items, callback) {
    var self = this;
    var nItems = items.length;
    var batches = self.splitIntoPutRequestBatches(self.tableName, items);
    self.numInFlightRequests += nItems;
    async.map(
        batches,
        function(batch, done){
            self.writeBatch.call(self, batch, done);
        },
        function(err){
            if (!err){
                self.numWritten += nItems;
                self.numInFlightRequests -= nItems;
                callback(null);
            } else {
                logger.error('Failed to write %d items: %s', nItems, err);
                callback(err);
            }
        }
        );
};

DynamoDBOutputStream.prototype.splitIntoPutRequestBatches = function(tableName, items){
    var self = this;
    var batches = [];
    var i = 0;
    var requestItems = {};
    requestItems[tableName] = [];
    while(items.length > 0){ 
        requestItems[tableName].push({
            PutRequest: {
                Item: items.pop()
            }
        });
        if (++i === DynamoDBOutputStream.MAX_BATCH_SIZE){
            batches.push(requestItems);
            requestItems = {};
            requestItems[tableName] = [];
            i = 0;
        }
    }
    if (requestItems[tableName].length > 0){
        batches.push(requestItems);
    }
    return batches;
};


DynamoDBOutputStream.prototype.writeBatch = function(batch, callback){
    var self = this;
    this.dynamodb.batchWriteItem({
        RequestItems: batch
    }, function(err, resp){
        if (!err){
            if (Object.keys(resp.UnprocessedItems).length === 0){
                self.backoff.skip(callback);
            } else {
                logger.debug('Found unprocessed items. Retrying after backoff...');
                self.numBackoff++;
                self.backoff.execute(function(){
                    self.writeBatch(resp.UnprocessedItems, callback);
                });
            }
        } else {
            if (err.code === 'ProvisionedThroughputExceededException'
                || err.code === 'InternalServerError'){
                logger.warn('Retry-able exception encountered: ' + err.code);
                self.backoff.execute(function(){
                    self.writeBatch(batch, callback);
                });
            } else {
                self.emit('error', err);
            }
        }
    });
};

module.exports = DynamoDBOutputStream;
