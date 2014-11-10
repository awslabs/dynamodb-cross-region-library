angular.module('DynamoDBCrossRegionReplicationDashboard')
	.directive('navigation', function($window, $log, $rootScope) {
		return {
			restrict: 'EA',
			scope: {
				active: '=',
				groups: '=',
				activeGroup: '=',
				tables: '=',
				activeTable: '='
	     	},
			templateUrl: 'views/partials/navigation.html',
			link: function(scope, element, attrs) {
				$log.debug(scope);
				$('#' + scope.active).addClass('active');
			}
		}
	});