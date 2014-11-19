## Overview
A sensor data dashboard used for demonstrating DynamoDB Cross Region Replication. It reads time series data from Amazon DynamoDB, calculates the number of events for a given period of time and plots it on the UI.

## Getting started
1. Install node.js and ruby for your platform if not installed
   - node.js: http://nodejs.org/download/
   - ruby: https://www.ruby-lang.org/en/installation/
2. Install necessary tools, i.e. grunt, bower, coffee-script and compass
```
  sudo npm -g install grunt-cli bower coffee-script
  sudo gem install compass
```
3. Install dependencies
```
  npm install
  bower install
```
4. Run the following command to start the demo app. 
```
  grunt serve
```
This will download DynamoDB Local, ingest a small subset of images from NASA ,launches a local HTTP server and opens the demo app.

## Building distributable package
1. Edit the configuration in `lib/mynconf.coffee` as appropriate or overwride the parameters with environment variables with the same name. The default configuration is as follows.
```
nconf.defaults
  DYNAMODB_ENDPOINT_DEV: 'http://localhost:9000/dynamodb/'
  DYNAMODB_REGION_DEV: 'us-east-1'
  DYNAMODB_ENDPOINT_TEST: 'http://localhost:8080/dynamodb/'
  DYNAMODB_REGION_TEST: 'us-east-1'
  DYNAMODB_ENDPOINT_PROD: 'http://dynamodb.us-east-1.amazonaws.com/'
  DYNAMODB_REGION_PROD: 'us-east-1' 
  USE_COGNITO_DEV: false
  USE_COGNITO_TEST: false
  USE_COGNITO_PROD: true
  AWS_ACCOUNT_ID: 'DummyAWSAccountID'
  COGNITO_IDENTITY_POOL_ID: 'DummyCognitoIdenityPoolID'
  COGNITO_UNAUTH_ROLE_ARN 'DummyCognitoUnauthRoleARN'
```

2. Run the following command
```
  grunt build
```

The above step will create two distribution directories. 
- dist -- contains the web app that can be deployed on an HTTP server, e.g. Amazon S3

## Automated Test
The following command performs unit tests with DynamoDB Local.
```
  grunt test
```
