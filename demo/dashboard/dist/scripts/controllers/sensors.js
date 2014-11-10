'use strict';
/**
 * @ngdoc function
 * @name IoTHackDayDashboard.controller:SensorsCtrl
 * @description
 * # SensorsCtrl
 * Controller of the IoTHackDayDashboard that handles the error dialog view.
 */
angular.module('IoTHackDayDashboard').
	controller('SensorsCtrl', function ($log, $scope, $route, $routeParams, $location, $timeout, ENV, DynamoDBReader, Graph) {
		$scope.active = 'dashboard';
		$scope.region = ENV.dynamoDBRegion;
		var graph;
                $('#title').text('Sensor Dashboard for ' + ENV.dynamoDBRegion);

		var reader = new DynamoDBReader();

		reader.on('initialized', function(){
			$log.info('Instantiating a new graph');
			graph = new Graph();
			graph.init(reader.getDeviceIDList(), reader.getSeriesData());
		});

		reader.on('updated', function(){
			$log.debug('Updating the graph');
			graph.update();
		});

		var getDeviceIDList = function(callback){
			var deviceIDList = [];
			for (var i = 1; i <= 10; i++){
				deviceIDList.push('Sensor_' + i);
			}
			callback(deviceIDList);
		};

		getDeviceIDList(function(deviceIDList){
			reader.init(ENV.commonTable, deviceIDList, $routeParams.time, 3000, 3, 150);
			reader.start();
		});


        /*
        * Handler called when the view is destroyed. 
        */
        $scope.$on('$destroy', function(){
        	reader.stop();
        });  
    });

