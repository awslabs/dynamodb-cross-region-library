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
 * @name DynamoDBCrossRegionReplicationDashboard.controller:ConfigureReplicationGroupCtrl
 * @description
 * # ConfigureReplicationGroupCtrl
 * Controller of the DynamoDBCrossRegionReplicationDashboard that handles the error dialog view.
 */
angular.module('DynamoDBCrossRegionReplicationDashboard').
	controller('ConfigureReplicationGroupCtrl', function ($log, $location, $scope, $routeParams, ReplicationManager) {
		$scope.active = 'dashboard';

		if ($routeParams.id){
			ReplicationManager.getReplicationGroup($routeParams.id, 
				function(group){
					$scope.group = group;
					$scope.operation = 'Update';
					$scope.title = 'Edit Replication Group Configuration';
				},
				function(error){
					$log.error(error);
				});
		} else {
			$scope.group = ReplicationManager.newReplicationGroup();
			$scope.group.coordinatorType = "LOCAL";
			$scope.operation = 'Add';							
			$scope.title = 'Add New Replication Group';							
		}

		$scope.save = function() {
			ReplicationManager.createReplicationGroup($scope.group,
				function(resp){
					$log.debug(resp);
					$location.path('#/replicationGroups/' + $scope.group.id);  
				},
				function(error){
					$log.error(error);
				});
		};

    });

