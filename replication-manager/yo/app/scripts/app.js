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
angular.module('DynamoDBCrossRegionReplicationDashboard', 
	[
    'ngRoute',
    'ngSanitize',
    'ngTouch',
    'config'
	]).
    config(['$routeProvider', function($routeProvider) {
        $routeProvider.
        when('/', {
            templateUrl: 'views/partials/dashboard.html',
            controller: 'DashboardCtrl'
        }).
        when('/replicationGroups/add', {
            templateUrl: 'views/partials/configure_replication_group.html',
            controller: 'ConfigureReplicationGroupCtrl'
        }).        
        when('/replicationGroups/:id', {
            templateUrl: 'views/partials/replication_group_details.html',
            controller: 'ReplicationGroupDetailsCtrl'
        }).
        when('/replicationGroups/:id/edit', {
            templateUrl: 'views/partials/configure_replication_group.html',
            controller: 'ConfigureReplicationGroupCtrl'
        }).                
        when('/replicationGroups/:id/tables/add/:type', {
            templateUrl: 'views/partials/add_table.html',
            controller: 'AddTableCtrl'
        }).                
        when('/tables/:arnPrefix/:tableName', {
            templateUrl: 'views/partials/table_details.html',
            controller: 'TableDetailsCtrl'
        }).                        
        when('/tables/:arnPrefix/:tableName/edit', {
            templateUrl: 'views/partials/configure_table.html',
            controller: 'ConfigureTableCtrl'
        }).                        
        otherwise({
            redirectTo: '/'
        });
    }]);


