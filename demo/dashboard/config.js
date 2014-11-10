module.exports.getConfigJS = function(host){
   return "angular.module('config', []).constant('ENV', " + getConfig(host) + ");";
}; 

var getConfig = function(host){
  var table = 'master';
  var region = 'us-east-1';
  if (process.env.PORT_OFFSET > 0){
    table = 'replica';
  }
  if (process.env.REGION){
    region = process.env.REGION;
  }
    
  var config = {
        name: 'production',
        useCognitoIdentity: false,
        userId: 'localUser',
        accessKeyId: 'DummyAccessKey',
        secretAccessKey: 'DummySecretKey',
        dynamoDBRegion: region,
        commonTable: table,
        dynamoDBEndpoint: getUrl(host)
   };
   return JSON.stringify(config);
};

var getUrl = function(host){
   return 'http://' + host + '/dynamodb';
};
