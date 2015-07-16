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
var Writable = require('stream').Writable;
var inherits = require('util').inherits;

inherits(S3OutputStream, Writable);

S3OutputStream.DEFAULT_HIGH_WATERMARK = 50;

function S3OutputStream(bucket, key, encoding, options){
    var self = this;
    if (!options){
        options = {};
    }
    options.objectMode = true; // This stream takes DynamoDB item as a chunk
    if (!options.highWaterMark){
        options.highWaterMark = S3OutputStream.DEFAULT_HIGH_WATERMARK;
    }
    Writable.call(this, options);

    // Creates the input end of the stream
    this.input = JSONStream.stringify();


    if (encoding === Constants.ENCODING_GZIP){
        // Creates an intermediate encoder 
        logger.info('Streaming to s3://%s/%s with encoding', bucket, key, encoding);
        this.encoder = zlib.createGzip();
        //this.input.pipe(this.encoder).pipe(this.output);
        this.output = this.input.pipe(this.encoder);
    } else {
        logger.warn('Unkonwn encoding %s. No encoder is added', encoding);
        this.output = this.input;
    }

    // Rename addListener call for internal use
    this._on = this.on;

    // Listens on 'finish' event to tell the input end of stream to finish
    this._on('finish', function(){
        logger.debug('End of input stream reached.');
        self.input.end();
    });

    // Replace addListener call to intercept finish event listener to listen to 
    // 'uploaded' event of the output end of stream
    this.on = function(event, listener){
        if (event === 'finish'){
            this._on('uploaded', listener);
        } else {
            this._on(event, listener);
        }
    };


    // Creates the output end of the stream
    var params = {
        Bucket: bucket,
        Key: key,
        Body: this.output
    };

    util.getS3Client().upload(params)
        .on('httpUploadProgress', function(evt) {
            logger.debug('Progress: part %d of %s loaded %d in total.', evt.part, key, evt.loaded); 
        })
        .send(function(err, data){
            if (!err){
                logger.debug(data);
                self.emit('uploaded', data);
            } else {
                self.emit('error', err);
            }
        });

    this.numWritten = 0;
    this.numBackoff = 0;
};

S3OutputStream.prototype._write = function(item, encoding, callback) {
    var self = this;
    self.input.write(item);
    self.numWritten++;
    callback(null);
};

S3OutputStream.prototype._writev = function(chunks, callback) {
    var self = this;
    for (var i in chunks){
        self.input.write(chunks[i].chunk);
    }
    self.numWritten += chunks.length;
    callback(null);
};


module.exports = S3OutputStream;
