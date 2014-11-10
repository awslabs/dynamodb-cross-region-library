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

