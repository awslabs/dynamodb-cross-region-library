util = require('./util')
nconf = require('./mynconf')


class DummySensor  
	constructor: (@deviceID, @rate) ->

	start: ->
		@.emitData()

	emitData: ->
		item = 
			device_id: @deviceID,
			time: new Date().getTime().toString()

		util.putItem(
			nconf.get('TABLE_COMMON')
			item
			(err) -> console.error(err)
			(data) -> console.log(data)
			)

		sensor = @
		setTimeout(
			() ->
				sensor.emitData.call(sensor) 
			Math.floor(Math.random() * 1000 / @rate)
		)


for i in [1..10]
	sensor = new DummySensor("Sensor_#{i}", 0.2 * Math.random())
	sensor.start()

