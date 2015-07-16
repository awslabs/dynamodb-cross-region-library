path = require('path');
fs   = require('fs');
lib  = path.join(path.dirname(fs.realpathSync(__filename)), '../lib');

chai = require('chai')
chai.Should()
expect = chai.expect
util = require(lib + '/util')
logger = util.getLogger()
Constants = require(lib + '/constants')
testCommon = require('./lib/test_commons')
async = require('async')

DynamoDBOutputStream = require(lib + '/dynamodb_output_stream')
DynamoDBInputStream = require(lib + '/dynamodb_input_stream')


describe 'DynamoDB Input/Output stream interface', ->
    beforeEach((callback) -> 
        dynamodb = util.getDynamoDBClient(Constants.DYNAMODB_DEFAULT_REGION, testCommon.dynamodbEndpoint)

        async.waterfall([
            (done) ->
                dynamodb.deleteTable {TableName: testCommon.tableDescription.TableName},
                    (err) ->
                        if err && err.code == 'ResourceNotFoundException'
                            done(null)
                        else
                            done(err) 
            (done) ->
                dynamodb.waitFor 'tableNotExists', {TableName: testCommon.tableDescription.TableName}, (err) -> done(err)
            (done) ->
                dynamodb.createTable testCommon.tableDescription, (err) -> done(err)
            (done) ->
                dynamodb.waitFor 'tableExists', {TableName: testCommon.tableDescription.TableName}, (err) -> done(err)
            ]
            callback
        )
    )

    describe 'Output Stream', ->
        it 'should write all the piped data to DynamoDB table', (done) ->
            @timeout(10000)
            dynamodbOutputStream = new DynamoDBOutputStream
                region: Constants.DYNAMODB_DEFAULT_REGION
                tableName: testCommon.tableName
                endpoint: testCommon.dynamodbEndpoint


            dynamodbOutputStream.on 'error', (err) -> 
                logger.error(err)
                done(err)

            dynamodbOutputStream.on 'finish', () ->
                expect(dynamodbOutputStream.numWritten).to.equal(testCommon.getNumItemsInResource())
                done(null)

            testCommon.getResourceAsStream().pipe(dynamodbOutputStream)

    describe 'Input Stream', ->
        it 'should read all the data from DynamoDB table and pipe them as objects', (done) ->
            @timeout(20000)            
            dynamodbOutputStream = new DynamoDBOutputStream
                region: Constants.DYNAMODB_DEFAULT_REGION
                tableName: testCommon.tableName
                endpoint: testCommon.dynamodbEndpoint

            testCommon.getResourceAsStream()
            .pipe(dynamodbOutputStream)
            .on 'finish', () ->
                dynamodbInputStream = new DynamoDBInputStream
                    region: Constants.DYNAMODB_DEFAULT_REGION
                    tableName: testCommon.tableName
                    endpoint: testCommon.dynamodbEndpoint

                numRead = 0
                dynamodbInputStream.on 'data', ()-> numRead++
                dynamodbInputStream.on 'end', ()-> 
                    expect(numRead).to.equal(testCommon.getNumItemsInResource())
                    done()
            

