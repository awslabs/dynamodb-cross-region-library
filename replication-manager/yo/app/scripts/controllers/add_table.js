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
 * @name DynamoDBCrossRegionReplicationDashboard.controller:AddTableCtrl
 * @description
 * # AddTableCtrl
 * Controller of the DynamoDBCrossRegionReplicationDashboard that handles the error dialog view.
 */
angular.module('DynamoDBCrossRegionReplicationDashboard').
	controller('AddTableCtrl', function ($log, $location, $scope, $routeParams, ReplicationManager) {
		$scope.active = 'replicationGroup';

		$scope.table = ReplicationManager.newTable();
		
		if ($routeParams.type === 'master'){
			$scope.title = 'Add Master Table';
			$scope.table.isMaster = true;
		} else {
			$scope.title = 'Add Replica Table';
			$scope.table.isMaster = false;
		}

		ReplicationManager.getReplicationGroup($routeParams.id, 
            function(group){
                $scope.group = group;
                $scope.title += ' in ' + $scope.group.name;                
            },
            function(error){
                $log.error(error);
            });

		
		$scope.save = function(table) {
			ReplicationManager.addTableInReplicationGroup($routeParams.id, $scope.table,
				function(){
					$location.path('/replicationGroups/' + $routeParams.id);  
				},
				function(error){
					$log.error(error);
				});
		};

    });

