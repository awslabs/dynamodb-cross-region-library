path = require('path')
fs   = require('fs')
resources  = path.join(path.dirname(fs.realpathSync(__filename)), '../resources')
zlib = require('zlib')
JSONStream = require('JSONStream')

tableName = 'testDynamoDBUtils'
testTableHashKey = 'sol'
testTableRangeKey = 'num_images'

TestCommons =
    testResourceFile: resources + '/image_manifest.json.gz'
    dynamodbEndpoint: 'http://localhost:8000'
    tableName: tableName
    tableDescription:
        TableName: tableName
        KeySchema: [
            {
                AttributeName: testTableHashKey
                KeyType: 'HASH'
            }
            {
                AttributeName: testTableRangeKey
                KeyType: 'RANGE'
            }
        ]
        AttributeDefinitions:[
            {
                AttributeName: testTableHashKey
                AttributeType: 'N'
            }
            {
                AttributeName: testTableRangeKey
                AttributeType: 'N'
            }
        ]
        ProvisionedThroughput: 
            ReadCapacityUnits: 1
            WriteCapacityUnits: 1
    redisHost: 'localhost'
    redisPort: 6379

TestCommons.getResourceAsStream = () ->
    fs.createReadStream(TestCommons.testResourceFile)
    .on('error', (err) -> logger.error(err))
    .pipe(zlib.createGunzip())
    .pipe(JSONStream.parse('*'))

TestCommons.getNumItemsInResource = () ->
    983

module.exports = TestCommons