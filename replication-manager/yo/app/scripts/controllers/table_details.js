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

