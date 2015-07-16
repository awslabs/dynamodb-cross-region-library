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
var Redis = require('redis');
var util = require('./util');
var logger = util.getLogger();
var async = require('async');
var Writable = require('stream').Writable;
var inherits = require('util').inherits;

inherits(RedisOutputStream, Writable);

RedisOutputStream.DEFAULT_HIGH_WATERMARK = 200;

function RedisOutputStream(host, port, database, srcTableDescription, options){
    if (!options){
        options = {};
    }
    options.objectMode = true; // This stream takes DynamoDB item as a chunk
    if (!options.highWaterMark){
        options.highWaterMark = RedisOutputStream.DEFAULT_HIGH_WATERMARK;
    }
    Writable.call(this, options);

    this.redis = Redis.createClient(port, host);
    this.database = database;
    for (var i in srcTableDescription.KeySchema){
        if (srcTableDescription.KeySchema[i].KeyType == 'HASH'){
            this.srcTableHashKey = srcTableDescription.KeySchema[i].AttributeName;
        }
        if (srcTableDescription.KeySchema[i].KeyType == 'RANGE'){
            this.srcTableRangeKey = srcTableDescription.KeySchema[i].AttributeName;
        }
    }
    logger.debug('Key schema is Hash: %s, Range: %s', this.srcTableHashKey, this.srcTableRangeKey);

    this.redis.on('ready', function(){
        logger.debug('Connected to redis');
        if(this.database){
            this.redis.select(this.database, function(){
                logger.debug('Database is set to %d', this.database);
            });
        }
    });

    var self = this;
    this.redis.on('error', function(err){
        self.emit('error', err);
    });

    this.numInFlightRequests = 0;
    this.numWritten = 0;
};

RedisOutputStream.prototype._write = function(item, encoding, callback) {
    var self = this;
    self.writeItem(item, callback);
};

RedisOutputStream.prototype._writev = function(chunks, callback) {
    var self = this;
    async.each(
        chunks,
        function(chunk, done){
            var item = chunk.chunk;
            self.writeItem(item, done);
        },
        callback
    );
};

RedisOutputStream.prototype.writeItem = function(item, callback) {
    var self = this;
    self.redis.hmset(self.getHashKey(item), item, function(err, reply){
        if (!err){
            self.numWritten++;
            callback(null);
        } else {
            callback(err);
        }
    });
};

RedisOutputStream.prototype.getHashKey = function(item, callback) {
    var key = item[this.srcTableHashKey];
    if(this.srcTableRangeKey){
        key += ':' + item[this.srcTableRangeKey];
    }
    return key;
};

module.exports = RedisOutputStream;