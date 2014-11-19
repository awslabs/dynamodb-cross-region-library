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
 * @name DynamoDBCrossRegionReplicationDashboard.controller:ReplicationGroupDetailsCtrl
 * @description
 * # ReplicationGroupDetailsCtrl
 * Controller of the DynamoDBCrossRegionReplicationDashboard that handles the error dialog view.
 */
angular.module('DynamoDBCrossRegionReplicationDashboard').
	controller('ReplicationGroupDetailsCtrl', function ($log, $scope, $location, $route, $routeParams, ReplicationManager, Monitoring) {

        $scope.active = 'replicationGroup';

        ReplicationManager.getReplicationGroups(
            function(groups){
                $scope.groups = groups;
                $scope.group = groups[$routeParams.id];
                $scope.title = 'Replication Group ' + $scope.group.name; 
            },
            function(error){
                $log.error(error);
            });

        ReplicationManager.getTablesInReplicationGroup($routeParams.id,
            function(tables){
                $scope.masters = tables.masters;
                $scope.replicas = tables.replicas;

                $scope.updateMetrics(new Date().getTime() - 24 * 60 * 60 * 1000, new Date().getTime(), 300);
            },
            function(error){
                $log.error(error);
            });



        $scope.updateMetrics = function(from, to, period){
            for (var i in $scope.masters){
                $scope.masters[i].metrics = {
                    numbers: [],
                    latency: []
                };
                Monitoring.getTableMetrics($scope.masters[i], from, to, period,                        
                    function(arn, data){
                        if (data.label == 'AccumulatedRecordLatency'){
                            $scope.masters[arn].metrics.latency.push({
                                key: data.label,
                                values: data.datapoints
                            });                            
                        } else {
                            $scope.masters[arn].metrics.numbers.push({
                                key: data.label,
                                values: data.datapoints
                            });
                        }

                        $scope.$emit('update');
                        $log.debug($scope.masters[arn].metrics.numbers);
                    },
                    function(error){
                        $log.error(error);
                    });
            }
        };

        $scope.clickOn = function(){
            $log.debug('Start replication button clicked');
            if ($scope.group && $scope.group.status !== 'RUNNING') {
                ReplicationManager.startReplication($scope.group, function(status){
                    $log.debug('Group status changed to ' + status);
                    refreshView();
                }, 
                function(error){
                    $log.error(error);
                });
            } else {
                $log.debug('Replication already started');
            }
        };

        $scope.clickOff = function(){
            $log.debug('Stop replication button clicked');
            if ($scope.group && $scope.group.status !== 'STOPPED'){
                ReplicationManager.stopReplication($scope.group, function(status){
                    $log.debug('Group status changed to ' + status);
                    refreshView();                    
                }, 
                function(error){
                    $log.error(error);
                });
            } else {
                $log.debug('Replication already stopped');
            }
        };

        $scope.isReplicationGroupSelected = function(id) {
            return id === $routeParams.id;
        };

        $scope.isAddTableButtonActive = false;

        $scope.hoverIn = function(){
            $scope.isAddTableButtonActive = true;
        };

        $scope.hoverOut = function(){
            $scope.isAddTableButtonActive = false;
        };

        var refreshView = function(){
            $route.reload();
        }

    });

