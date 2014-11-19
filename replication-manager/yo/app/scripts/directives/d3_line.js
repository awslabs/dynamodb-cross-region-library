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
