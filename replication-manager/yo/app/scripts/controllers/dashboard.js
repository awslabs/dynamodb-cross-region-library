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
 * @name DynamoDBCrossRegionReplicationDashboard.controller:DashboardCtrl
 * @description
 * # DashboardCtrl
 * Controller of the DynamoDBCrossRegionReplicationDashboard that handles the error dialog view.
 */
angular.module('DynamoDBCrossRegionReplicationDashboard').
	controller('DashboardCtrl', function ($log, $scope, ReplicationManager) {
		$scope.title = 'Replication Groups';

		$scope.active = 'dashboard';

		$scope.isAddTableButtonActive = false;

		$scope.hoverIn = function(){
			$scope.isAddTableButtonActive = true;
		};

		$scope.hoverOut = function(){
			$scope.isAddTableButtonActive = false;
		};

		ReplicationManager.getReplicationGroups(
			function(groups){
				$scope.groups = groups;
			}, 
			function(data){
				$log.error('Failed to load replication groups: ' + data);
			});

    });

