/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

 'use strict';
/* global $ */
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
		$('#title').text('Reading Events from ' + ENV.commonTable + ' in ' + ENV.dynamoDBRegion);

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

