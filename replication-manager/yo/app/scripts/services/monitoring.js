'use strict';
/* global AWS */
/**
 * @ngdoc function
 * @name DynamoDBCrossRegionReplicationDashboard.service:Monitoring
 * @description
 * # Monitoring
 * Service of the DynamoDBCrossRegionReplicationDashboard.  
 */
angular.module('DynamoDBCrossRegionReplicationDashboard').
    service('Monitoring', function ($log, $http){
    	var Monitoring = {
    		getTableMetrics: function(table, from, to, period, success, error){
    			var metrics = ['NumberUserWrites', 'NumberReplicationWrites', 'NumberCheckpointedRecords', 'AccumulatedRecordLatency'];
    			for (var i in metrics){
    				var params = {
    					namespace: table.kinesisApplicationName,
    					statistics: ['Sum', 'Average'],
    					metricName: metrics[i],
    					startTime: from,
    					endTime: to,
    					period: period
    				};
    				$http.post('/api/statistics/' + table.region, params)
    				.success(function(data){
    					// Need to sort by timestamp
    					data.datapoints.sort(function(a, b){
    						return a.timestamp - b.timestamp;
    					});
    					success(table.arn, data);
    				})
    				.error(function(data){
    					$log.error(data);
    				});
    			}
    		},

    		getDummyMetrics: function(table, from, to, period, success, error){
    			var params = {
    				namespace: 'AWS/RDS',
    				statistics: ['Sum', 'Average'],
    				metricName: 'CPUUtilization',
    				startTime: from,
    				endTime: to,
    				period: period
    			};
    			$http.post('/api/statistics/ap-northeast-1', params)
    			.success(function(data){
    				// Need to sort by timestamp
    				data.datapoints.sort(function(a, b){
    					return a.timestamp - b.timestamp;
    				});
    				$log.debug(data);
    				success(table.arn, data);
    			})
    			.error(function(data){
    				$log.error(data);
    			});
    		},
    	}
    	return Monitoring;
    });
