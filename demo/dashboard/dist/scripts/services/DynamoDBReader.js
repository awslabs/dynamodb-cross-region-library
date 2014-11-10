'use strict';
/**
 * @ngdoc function
 * @name IoTHackDayDashboard.service:DynamoDBReader
 * @description
 * # DynamoDBReader
 */
angular.module('IoTHackDayDashboard').
    service('DynamoDBReader', function ($log, $interval, AWS){
        var DynamoDBReader = function(){
            var now;
            var updatesData;
            var tableName;
            var updateInterval;
            var nSlotsToUpdate;
            var maxPlots;
            var callbacks = {};
            var seriesData;
            var devices;
            var timerCancel;
            var deviceIDList;

            this.init = function(_tableName, _deviceIDList, _baseTime, _updateInterval, _nSlotsToUpdate, _maxPlots){
                tableName = _tableName;
                if (_baseTime){
                    console.log('Basetime is set to ' + _baseTime);
                    now = _baseTime;
                    updatesData = false;
                } else {
                    now = new Date().getTime();
                    updatesData = true;
                }
                updateInterval = _updateInterval;
                nSlotsToUpdate = _nSlotsToUpdate;
                maxPlots = _maxPlots;
                deviceIDList = _deviceIDList;

                seriesData = [];
                devices = {};
                for (var i in _deviceIDList) {
                    seriesData.push([]);
                    devices[_deviceIDList[i]] = {
                        updatedAt: 0,
                        data: seriesData[i]
                    };
                }
            };

            this.getDeviceIDList = function(){
                return deviceIDList;
            };

            this.getSeriesData = function(){
                return seriesData;
            };

            this.on = function(event, callback){
                callbacks[event] = callback;
            };

            this.start = function() {
                loadInitialData(function(){
                    var callback = callbacks.initialized;
                    if (callback) {
                        callback();
                    }
                    if (updatesData){ 
                        var reader = this;
                        timerCancel = $interval(function(){
                            updateData.call(reader, callbacks.updated);
                        }, updateInterval);
                    }
                });
            };

            this.stop = function() {
                if (timerCancel) {
                    $interval.cancel(timerCancel);
                    timerCancel = null;
                }
            };

            var loadInitialData = function(callback) {
                $log.debug('Loading initial data');
                for (var id in devices) { 
                    getData(id, now - updateInterval * maxPlots, function(id, events){
                        for (var i = maxPlots - 1; i >= 0; i--){
                            var slot = now - i * updateInterval;
                            devices[id].data.push({
                                x: slot,
                                y: countEvents(events, slot - updateInterval, slot)
                            });
                        }
                        devices[id].updatedAt = now;

                        if (allDevicesDataUpToDate()){
                            now += updateInterval;
                            callback();
                        }
                    });
                }
            };

            var updateData = function(callback){
                for (var id in devices) {
                    getData(id, now - updateInterval * nSlotsToUpdate, function(id, events){
                        for (var i = nSlotsToUpdate - 1; i >= 0; i--) {
                            var slot = now - i * updateInterval;
                            var plot = {
                                x: slot,
                                y: countEvents(events, slot - updateInterval, slot)
                            };
                            if (i > 0){
                                devices[id].data[maxPlots - i] = plot;
                            } else {
                                devices[id].data.push(plot);
                                devices[id].data.shift();
                            }
                        }

                        devices[id].updatedAt = now;
                        if (allDevicesDataUpToDate()){
                            $log.debug(devices);
                            now += updateInterval;
                            callback();
                        }
                    });
                }
            };

            var getData = function(deviceID, timeSince, callback) {
                var params = { 
                    TableName: tableName,
                    KeyConditions: [
                    AWS.dynamoDB.Condition('device_id', 'EQ', deviceID),
                    AWS.dynamoDB.Condition('time', 'GE', timeSince.toString())
                    ],
                    Limit: 500,
                    ScanIndexForward: true
                };
                AWS.dynamoDB.query(params, function(error, data){
                    if (!error) {
                        $log.debug(data.Count + ' data points received from device ' + deviceID);
                        callback(deviceID, data.Items);
                    } else {
                        $log.error(error);
                    }
                });
            };

            var countEvents = function(events, from, to) {
                var count = 0;
                var event = events.shift();
                while (event) {
                    var timestamp = parseTimestamp(event.time);
                    if (from <= timestamp && timestamp < to) {
                        count++;
                        event = events.shift();
                    } else {
                        events.unshift(event);
                        break;
                    }
                }
                return count;
            };

            var parseTimestamp = function(time){
                return parseInt(time);
            };

            var allDevicesDataUpToDate = function(){
                for (var id in devices) {
                    if (devices[id].updatedAt !== now) {
                        return false;
                    }
                }
                return true;
            };
        };

        return DynamoDBReader;	
    });
