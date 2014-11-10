angular.module('DynamoDBCrossRegionReplicationDashboard')
	.directive('d3Line', function($window, $log, $rootScope) {
		return {
			restrict: 'EA',
			scope: {data: '='},
			link: function(scope, element, attrs) {
				var margin = parseInt(attrs.margin) || 20;
				var barHeight = parseInt(attrs.barHeight) || 320;
				var barPadding = parseInt(attrs.barPadding) || 10;
				var svg = d3.select(element[0])
				.append('svg')
				.style('width', '100%')
				.style('height', '250px');				

                $rootScope.$on('update', function(){
                	if (scope.data){
                		scope.render(scope.data, 'average');
                	}
                });

                scope.render = function(data, statistics) {
                	nv.addGraph(function() {
                		var chart = nv.models.lineChart()
                		.x(function(d) { return d.timestamp })
                		.y(function(d) { return d[statistics] })
                		.margin({top: 30, right: 50, bottom: 30, left: 50})
                        .tooltips(false)             //Show tooltips on hover.
                        .transitionDuration(350);

                        chart.yAxis
                        .tickFormat(d3.format(',.0f'));

                        chart.xAxis
                        .tickFormat(function(d){return d3.time.format('%d-%b %H:%M:%S')(new Date(d));});

                        svg.datum(data)
                        .call(chart);

                        nv.utils.windowResize(chart.update);

                        return chart;
                    });
                };

            }
        };
    });
