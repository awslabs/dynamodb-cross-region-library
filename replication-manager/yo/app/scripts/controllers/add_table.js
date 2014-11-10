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

