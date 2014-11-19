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
/**
 * @ngdoc function
 * @name DynamoDBCrossRegionReplicationDashboard.controller:ConfigureTableCtrl
 * @description
 * # ConfigureTableCtrl
 * Controller of the DynamoDBCrossRegionReplicationDashboard that handles the error dialog view.
 */
angular.module('DynamoDBCrossRegionReplicationDashboard').
	controller('ConfigureTableCtrl', function ($log, $location, $scope, $routeParams, ReplicationManager) {
		if ($routeParams.arnPrefix && $routeParams.tableName) {
			var arn = $routeParams.arnPrefix + '/' + $routeParams.tableName;
			$scope.table = ReplicationManager.getTable(arn);
			$scope.operation = 'Update';
			$scope.readonly = true;
			$scope.title = 'Edit Table Configuration';
		} else {
			$scope.table = ReplicationManager.newTable();
			$scope.operation = 'Add Table';
			$scope.title = 'Add Table';							
		}

		$scope.save = function(table) {
			$log.debug(table);
			ReplicationManager.putTable(table);
			$location.path('#/');  
		};

    });
