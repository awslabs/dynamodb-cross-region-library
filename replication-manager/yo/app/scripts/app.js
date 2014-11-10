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


