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

