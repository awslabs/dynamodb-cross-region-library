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
/* global AWS */
/**
 * @ngdoc function
 * @name DynamoDBCrossRegionReplicationDashboard.service:ReplicationManager
 * @description
 * # ReplicationManager
 * Service of the DynamoDBCrossRegionReplicationDashboard.  
 */
angular.module('DynamoDBCrossRegionReplicationDashboard').
    service('ReplicationManager', function ($log, $http){
    	var ReplicationManager = {
    		getReplicationGroups: function(success, error){
    			$http.get('/api/groups/')
    			.success(function(data){
    				var groups = {}
    				for (var i in data) {
    					var group = new ReplicationGroup(data[i]);
    					groups[group.id] = group;
    				}
    				success(groups);
    			})
    			.error(function(data){
    				error(data);
    			});
    		},

            getReplicationGroup: function(groupId, success, error){
            	$http.get('/api/groups/' + groupId)
            	.success(function(data){
            		success(new ReplicationGroup(data));
            	})
            	.error(function(data){
            		error(data);
            	});
            },

            getTablesInReplicationGroup: function(groupId, success, error){
            	$http.get('/api/groups/' + groupId + '/tables')
            	.success(function(data){
            		var tables = {
            			masters: {},
            			replicas: {}
            		};
            		for (var region in data){
            			for (var i in data[region]){
            				var table = new Table(data[region][i]);
            				if (table.isMaster) {
            					tables.masters[table.arn] = table;
            				} else {
            					tables.replicas[table.arn] = table;
            				}
            			}
            		}
            		success(tables);
            	})
            	.error(function(data){
            		error(data);
            	});
            },            

            createReplicationGroup: function(group, success, error){
            	$http.post('/api/groups/', group.serialize())
            	.success(function(data){
            		success(data);
            	})
            	.error(function(data){
            		error(data);
            	});
            },

            newReplicationGroup: function(){
                return new ReplicationGroup({});
            },

            deleteReplicationGroup: function(groupId, success, error){
            	$http.delete('/api/groups/' + groupId)
            	.success(function(data){
            		success(data);
            	})
            	.error(function(data){
            		error(data);
            	});
            },

            addTableInReplicationGroup: function(groupId, table, success, error){
            	$http.post('/api/groups/' + groupId + '/tables', table.serialize())
            	.success(function(data){
            		success(data);
            	})
            	.error(function(data){
            		error(data);
            	});
            },

            removeTableFromReplicationGroup: function(groupId, table, success, error){
            	$http.delete('/api/groups/' + groupId + '/tables/' + table.id)
            	.success(function(data){
            		success(data);
            	})
            	.error(function(data){
            		error(data);
            	});
            },

            newTable: function(){
                return new Table({
                    account: ReplicationManager.getDefaultAccountID(),
                    region: ReplicationManager.getDefaultRegion()
                });
            },

            startReplication: function(group, success, error) {
            	$http.put('/api/groups/' + group.id + '/status/start')
    			.success(function(data){
    				if (data){
    					group.status = data;
    				} else {
    					group.status = 'UNKNOWN';
    				}
    				success(group.status);
    			})
    			.error(function(data){
    				error(data);
    			});
            },

            stopReplication: function(group, success, error) {
            	$http.put('/api/groups/' + group.id + '/status/stop')
    			.success(function(data){
    				if (data){
    					group.status = data;
    				} else {
    					group.status = 'UNKNOWN';
    				}
    				success(group.status);
    			})
    			.error(function(data){
    				error(data);
    			});             
            },

            getDefaultAccountID: function() {
                return '12345678901';
            },

            getDefaultRegion: function() {
                return 'us-east-1';
            },

            parseArnPrefix: function(prefix) {
                var params = {};
                if (prefix) {
                    var tokens = prefix.split(':');
                    if (tokens.length > 5) {
                        params.account = tokens[4];
                        params.region = tokens[3];
                    }
                }
                return params;
            }
    	};

        var panelColorMap = {
            'RUNNING': 'panel-green',
            'REPLICATING': 'panel-green',
            'BOOTSTRAPPING': 'panel-yellow',
            'ERROR': 'panel-red',
            'DOES_NOT_EXIST': 'panel-red',            
            'STOPPED': 'panel-gray',
            'NOT_STARTED': 'panel-gray',
            'EMPTY': 'panel-gray',
            'COORDINATOR_UNREACHABLE': 'panel-red'
        };

        var ReplicationGroup = function(params) {
            this.id = params.ID;
            this.name = params.Name;
            this.tables = {};
            this.status = params.Status;
            this.coordinatorType = params.CoordinatorType;
            this.replicationStatuses = {};
        };
        ReplicationGroup.prototype.isStarted = function(){
        	return this.status === 'RUNNING';
        };
        ReplicationGroup.prototype.setMaster = function(table){
            $log.debug('Setting master table of the group to ' + table.arn);
            table.status = 'replicating';
            this.master = table;         
        };
        ReplicationGroup.prototype.putReplica = function(table){
            $log.debug('Adding table to the group: ' + table.arn);
            this.tables[table.arn] = table;    
            this.replicationStatuses[table.arn] = 'bootstrapping';        
        };

        ReplicationGroup.prototype.removeTable = function(arn){
            if (this.master.arn === arn) {
                this.master = null;
                this.status = 'stopped';
            } else {
                this.tables[arn].delete(arn);
                if (this.tables.length === 0){
                    this.status = 'stopped';
                }                
            }
        };
        ReplicationGroup.prototype.getPanelColor = function(arn){
            if (arn) {
                var color = panelColorMap[this.replicationStatuses[arn]];
                if (color) {
                    return color;
                }
            } else {
                return panelColorMap[this.status];
            }
            return panelColorMap['unknown'];
        };
        ReplicationGroup.prototype.serialize = function(){
        	return {
        		ID: this.id,
        		Name: this.name,
        		Status: this.status,
        		CoordinatorType: this.coordinatorType
        	};
        };

    	var Table = function(params) {
    		this.account = params.AccountId;
    		this.region = params.Region;
    		this.name = params.TableName;
    		this.status = params.TableStatus;
    		this.isMaster = params.Master;
    		this.endpoint = params.Endpoint;
    		this.kinesisApplicationName = params.KinesisApplicationName;
    		this.arn = 'arn:aws:dynamodb:' + this.region + ':' + this.account + ':table/' + this.name;
            this.groups = {};
            this.error = false;
    	};
        Table.prototype.getDisplayName = function(){
        	var name = this.region + '/' + this.name;
        	if (this.account) {
        		 name += '@' + this.account;
        	}
            return name;
        };
        Table.prototype.getPanelColor = function(){
            if (!this.error) {
                return panelColorMap[this.status];
            } else {
                return panelColorMap['error'];
            }
        };
        Table.prototype.getArn = function(){
            return 'arn:aws:dynamodb:' + this.region + ':' + this.account + ':table/' + this.name;
        };
        Table.prototype.serialize = function(){
        	return {
        		AccountId: this.account,
        		Region: this.region,
        		TableName: this.name,
        		Master: this.isMaster,
        		Endpoint: this.endpoint
        	};
        };


        return ReplicationManager;
    });
