'use strict';
angular.module('IoTHackDayDashboard', 
	[
    'ngRoute',
    'ngSanitize',
    'ngTouch',
    'config'
	]).
    config(['$routeProvider', function($routeProvider) {
        $routeProvider.
        when('/', {
            templateUrl: 'views/partials/sensors.html',
            controller: 'SensorsCtrl'
        }).
        otherwise({
            redirectTo: '/'
        });
    }]);


