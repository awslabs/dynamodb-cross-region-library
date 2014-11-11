(function() {
  var AWS, DOC, DummySensor, dynamoDB, i, sensor, uuid, _i;

  AWS = require('aws-sdk');

  DOC = require('dynamodb-doc');

  uuid = require('uuid');

  AWS.config.credentials = new AWS.Credentials ({
        accessKeyId: process.env.AWS_ACCESS_KEY,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  });

  dynamoDB = new DOC.DynamoDB(new AWS.DynamoDB({
    endpoint: process.env.DYNAMODB_ENDPOINT,
    region: process.env.DYNAMODB_REGION
  }));

  DummySensor = (function() {
    function DummySensor(deviceID, rate) {
      this.deviceID = deviceID;
      this.rate = rate;
    }

    DummySensor.prototype.start = function() {
      return this.emitData();
    };

    DummySensor.prototype.emitData = function() {
      var item, params, sensor, time;
      time = new Date();
      item = {
        device_id: this.deviceID,
        time: time.getTime().toString(),
        'USER___________UPDATE': null,
        'TIMESTAMP___________KEY': time.toISOString() + '_' + uuid()
      };
      params = {
        TableName: process.env.TABLE_COMMON,
        Item: item
      };
      dynamoDB.putItem(params, function(err, data) {
        if (!err) {
          return console.log(data);
        } else {
          return console.error(err);
        }
      });
      sensor = this;
      return setTimeout(function() {
        return sensor.emitData.call(sensor);
      }, Math.floor(Math.random() * 1000 / this.rate));
    };

    return DummySensor;

  })();

  for (i = _i = 1; _i <= 10; i = ++_i) {
    sensor = new DummySensor("Sensor_" + i, 0.2 * Math.random());
    sensor.start();
  }

}).call(this);
