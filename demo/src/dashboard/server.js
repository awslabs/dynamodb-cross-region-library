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
var http = require('http'),
    httpProxy = require('http-proxy'),
    finalhandler = require('finalhandler'),
    serveStatic = require('serve-static');
var config = require('./config');

var dynamodbLocalPort = 8000;
var httpServerPort = 10000;

var offset = process.env.PORT_OFFSET;

if (offset) {
   offset = parseInt(offset);
   dynamodbLocalPort += offset;
   httpServerPort += offset;
}

//
// Create a proxy server with custom application logic
//
var proxy = httpProxy.createProxyServer({});

var serve = serveStatic('./dist');

//
// Create your custom server and just call `proxy.web()` to proxy
// a web request to the target passed in the options
// also you can use `proxy.ws()` to proxy a websockets request
//
var server = http.createServer(function(req, res) {
  if (req.url == '/scripts/config.js') {
    console.log('received request' + req);
    res.setHeader('Content-Type', 'text/javascript');
    res.write(config.getConfigJS(req.headers.host));
    res.end();
  } else if (req.url.indexOf('/dynamodb') == 0){
     proxy.web(req, res, { target: 'http://localhost:' + dynamodbLocalPort });
  } else {
     var done = finalhandler(req, res);
     serve(req, res, done);
  }
});

console.log("listening on port " + httpServerPort );
server.listen(httpServerPort);

