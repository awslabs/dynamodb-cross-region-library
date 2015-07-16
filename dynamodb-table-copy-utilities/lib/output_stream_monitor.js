/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 * http://aws.amazon.com/asl/
 * 
 * or in the "LICENSE.txt" file accompanying this file.
 * 
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
var async = require('async');
var util = require('./util');
var logger = util.getLogger();

OutputStreamMonitor.MONITOR_INTERVAL = 5000;


function OutputStreamMonitor(taskId){
	if (taskId){
		this.taskId = taskId;
	} else {
		this.taskId = "PID " + process.pid;
	}
	this.outputStreams = [];
	this.lastTotalNumWritten = 0;
	this.lastTotalNumBackoff = 0;	
};

OutputStreamMonitor.prototype.add = function(outputStream){
	this.outputStreams.push(outputStream);
};

OutputStreamMonitor.prototype.print = function(){
	var self = this;
	var totalNumWritten = 0;
	var totalNumInFlightRequests = 0;
	var totalNumBackoff = 0;
	for (var i in self.outputStreams){
		totalNumWritten += self.outputStreams[i].numWritten;
		if (self.outputStreams[i].numInFlightRequests){
			totalNumInFlightRequests += self.outputStreams[i].numInFlightRequests;
			totalNumBackoff += self.outputStreams[i].numBackoff;
		}
	}	
	var elapsedTime = (new Date().getTime() - self.startTime) / 1000;
	var rate = (totalNumWritten - self.lastTotalNumWritten) / (OutputStreamMonitor.MONITOR_INTERVAL / 1000);
	var numBackoff = totalNumBackoff - self.lastTotalNumBackoff;
    logger.info('%s wrote %d items in %d seconds (Rate = %d items/s, # of backoff = %d)',
        self.taskId, totalNumWritten, elapsedTime, rate, numBackoff);
    if (totalNumInFlightRequests > 0){
	    logger.info('%d items are in flight', totalNumInFlightRequests);
	}
    self.lastTotalNumWritten = totalNumWritten;
    self.lastTotalNumBackoff = totalNumBackoff;
};

OutputStreamMonitor.prototype.start = function(){    
    var self = this;
    self.startTime = new Date().getTime();
    this.monitorInterval = setInterval(function(){
    	self.print();
    }, OutputStreamMonitor.MONITOR_INTERVAL);
};    

OutputStreamMonitor.prototype.stop = function(){ 
    if (this.monitorInterval){
        clearInterval(this.monitorInterval);
    }
    this.print();
};

module.exports = OutputStreamMonitor;