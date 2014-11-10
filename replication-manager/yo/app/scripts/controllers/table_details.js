'use strict';
/**
 * @ngdoc function
 * @name DynamoDBCrossRegionReplicationDashboard.controller:TableDetailsCtrl
 * @description
 * # TableDetailsCtrl
 * Controller of the DynamoDBCrossRegionReplicationDashboard that handles the error dialog view.
 */
angular.module('DynamoDBCrossRegionReplicationDashboard').
	controller('TableDetailsCtrl', function ($log, $location, $scope, $routeParams, ReplicationManager) {
		var arn = $routeParams.arnPrefix + '/' + $routeParams.tableName;

		$scope.table = ReplicationManager.getTable(arn);
		$scope.srcTables = ReplicationManager.getTablesWritingTo(arn);
		$scope.dstTables = ReplicationManager.getTablesReadingFrom(arn);
		
    });

