nconf = require 'nconf' 

nconf.argv()
	 .env()

nconf.defaults
#	DYNAMODB_ENDPOINT_DEV: 'http://localhost:9000/dynamodb/'
	DYNAMODB_ENDPOINT_DEV: 'http://dynamodb.us-east-1.amazonaws.com/'
	DYNAMODB_REGION_DEV: 'us-east-1'
	USE_COGNITO_DEV: true
	DYNAMODB_ENDPOINT_TEST: 'http://localhost:8080/dynamodb/'
	DYNAMODB_REGION_TEST: 'us-east-1'
	USE_COGNITO_TEST: false	
	DYNAMODB_ENDPOINT_PROD: 'http://dynamodb.us-east-1.amazonaws.com/'
	DYNAMODB_REGION_PROD: 'us-east-1'	
	USE_COGNITO_PROD: true
	AWS_ACCOUNT_ID: '555818481905'
	COGNITO_IDENTITY_POOL_ID: 'us-east-1:b75360c9-38a9-4f52-ae0c-8ca67e30934f'
	COGNITO_UNAUTH_ROLE_ARN: 'arn:aws:iam::555818481905:role/Cognito_iotHackDay_kenta_testUnauth_DefaultRole'
	TABLE_DEVICES: 'hackday_common'
	READ_CAPACITY: 1
	WRITE_CAPACITY: 1

module.exports = nconf
