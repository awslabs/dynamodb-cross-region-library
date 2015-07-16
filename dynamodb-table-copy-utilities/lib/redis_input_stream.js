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
require("redis-scanstreams")(Redis)
var Transform = require('stream').Transform;
var inherits = require('util').inherits;

inherits(RedisInputStream, Transform);

RedisInputStream.DEFAULT_HIGH_WATERMARK = 100;

function RedisInputStream(host, port, database, dstTableDescription, keyPattern, options){
    if (!options){
        options = {};
    }
    options.objectMode = true; // This stream takes DynamoDB item as a chunk
    if (!options.highWaterMark){
        options.highWaterMark = RedisInputStream.DEFAULT_HIGH_WATERMARK;
    }
    Transform.call(this, options);

	this.redis = Redis.createClient(port, host, database);
	this.attrTypeMap = {}
	for (var i in dstTableDescription.AttributeDefinitions){
		var attr = dstTableDescription.AttributeDefinitions[i];
		this.attrTypeMap[attr.AttributeName] = attr.AttributeType;
	}
	this.dstTableDescription = dstTableDescription;

	if (keyPattern){
		this.keyPattern = keyPattern;
	} else {
		this.keyPattern = '*'
	}

	this.redis.scan({pattern: this.keyPattern, count: 1000}).pipe(this);
};
RedisInputStream.prototype._transform = function(chunk, encoding, callback){
	var self = this;
	self.redis.hgetall(chunk, function(err, item){
		if (!err){
			for (var name in self.attrTypeMap){
				if (item[name] && self.attrTypeMap[name] === 'N'){
					item[name] = parseFloat(item[name]);
				}
			}
			self.push(item);
			callback(null);
		} else {
			callback(err);
		}
	});
};

module.exports = RedisInputStream;