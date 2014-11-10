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
