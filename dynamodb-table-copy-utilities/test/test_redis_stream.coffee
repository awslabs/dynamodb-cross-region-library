path = require('path');
fs   = require('fs');
lib  = path.join(path.dirname(fs.realpathSync(__filename)), '../lib');
chai = require('chai')
chai.Should()
expect = chai.expect
util = require(lib + '/util')
logger = util.getLogger()
async = require('async')
Redis = require('redis')
RedisOutputStream = require(lib + '/redis_output_stream')
RedisInputStream = require(lib + '/redis_input_stream')
testCommon = require('./lib/test_commons')

describe 'Redis Input/Output Stream interface', ->
	redis = null

	beforeEach((callback) -> 
		async.waterfall([
			(done) ->
				redis = Redis.createClient()
				redis.on('ready', () -> done(null))
			(done) ->
				redis.flushall(() -> done(null))
			]
			(err, session) ->
				callback err
		)
	)

	describe 'Output Stream', ->
		it 'should write all the piped data to Redis', (done) ->
            outputStream = new RedisOutputStream(testCommon.redisHost, testCommon.redisPort, 0, testCommon.tableDescription)

            outputStream.on 'error', (err) -> 
            	logger.error(err)
            	done(err)

            outputStream.on 'finish', () ->
            	expect(outputStream.numWritten).to.equal(testCommon.getNumItemsInResource())
            	done()

            testCommon.getResourceAsStream().pipe(outputStream)

	describe 'Input Stream', ->
		it 'should read all the data from Redis and pipe them as objects', (done) ->
            outputStream = new RedisOutputStream(testCommon.redisHost, testCommon.redisPort, 0, testCommon.tableDescription)

            testCommon.getResourceAsStream()
            .pipe(outputStream)
            .on 'finish', () ->
            	inputStream = new RedisInputStream(testCommon.redisHost, testCommon.redisPort, 0, testCommon.tableDescription)
            	numRead = 0
            	inputStream.on 'data', (item) -> 
            		expect(typeof(item.sol)).to.equal('number')
            		expect(typeof(item.num_images)).to.equal('number')
            		numRead++
            	inputStream.on 'end', ()-> 
            		expect(numRead).to.equal(testCommon.getNumItemsInResource())
            		done()
