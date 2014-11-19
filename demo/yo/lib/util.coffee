AWS = require('aws-sdk')
DOC = require('dynamodb-doc')
nconf = require('./mynconf')

AWS.config.credentials = new AWS.Credentials (
	accessKeyId: nconf.get('AWS_ACCESS_KEY')
	secretAccessKey: nconf.get('AWS_SECRET_ACCESS_KEY')
)

dynamoDB = new DOC.DynamoDB(
	new AWS.DynamoDB(
		endpoint: nconf.get('DYNAMODB_ENDPOINT')
		region: nconf.get('DYNAMODB_REGION')
	)
)

isReady = (tableName, callback) ->
	dynamoDB.describeTable({ TableName: tableName }, (error, data) ->
		unless error 		
			callback(data.Table.TableStatus == 'ACTIVE') 
		else 
			console.error(error.message)
	)

notExists = (tableName, callback) ->
	dynamoDB.describeTable({ TableName: tableName }, (error, data) ->
		if error and error.code == 'ResourceNotFoundException' 
			callback(true)
		else 
			callback(false)
	)

waitUntil = (args, cond, ready) ->
	repeat = -> waitUntil(args, cond, ready)
	cond(args, (conditionMet) -> 
		if conditionMet 
			console.log("Done")
			ready(args) 
		else 
			console.log("Waiting for operation to complete...")
			setTimeout(repeat, 200)
	)

createTable = (tableParams, deleteIfExists, err, done) ->
	if deleteIfExists
		deleteTableAndWaitUntilRemoved(tableParams.TableName, 
			(error) ->
				console.error error
			() ->
				createTableAndWaitUntilReady(tableParams, err, done)
		)
	else 
		createTableAndWaitUntilReady(tableParams, err, done)

createTableAndWaitUntilReady = (tableParams, err, done) ->
	console.log "creating table #{tableParams.TableName} on #{nconf.get('DYNAMODB_ENDPOINT')}" 
	dynamoDB.createTable(tableParams, (error, data) ->
    	unless error
    		waitUntil(tableParams.TableName, isReady, done)
    	else 
    		if error.code == 'ResourceInUseException'
    			console.info "Table #{tableParams.TableName} already exists"
    			waitUntil(tableParams.TableName, isReady, done)
    		else
    			console.error error.message
    			err(error.message) if err
    	)

deleteTableAndWaitUntilRemoved = (tableName, err, done) ->
	console.log("Deleting table #{tableName}")
	dynamoDB.deleteTable({TableName: tableName}, (error, data) ->
		unless error
			waitUntil(tableName, notExists, done)
		else
			if error.code == 'ResourceNotFoundException'
				waitUntil(tableName, notExists, done)
			else 
				err(error) if err
	)

putItem = (tableName, item, err, done) ->
	params = 
      TableName: tableName
      Item: item

    dynamoDB.putItem(params, (error, data) ->
      unless error
        done(item) if done
      else
        err(error) if err
    )


module.exports.createTable = createTable
module.exports.putItem = putItem
module.exports.dynamoDB = dynamoDB
