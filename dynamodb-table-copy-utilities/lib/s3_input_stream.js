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
var zlib = require('zlib');
var util = require('./util');
var logger = util.getLogger();
var JSONStream = require('JSONStream');
var Constants = require('./constants');

S3InputStream = function(bucket, key, metadata){
    var self = this;
    var params = {
        Bucket: bucket,
        Key: key
    };
    this.input = util.getS3Client().getObject(params).createReadStream();

    var parser = JSONStream.parse('*');
    if (metadata.contentEncoding === Constants.ENCODING_GZIP){
        this.output = this.input.pipe(zlib.createGunzip()).pipe(parser);
    } else {
        logger.warn('Unknown encoding type %s. Assuming plain text', metadata.contentEncoding);
        this.output = this.input.pipe(parser);
    }
};
S3InputStream.prototype.on = function(event, callback){
    this.output.on(event, callback);
};
S3InputStream.prototype.pipe = function(writable){
    this.output.pipe(writable);
};

module.exports = S3InputStream;