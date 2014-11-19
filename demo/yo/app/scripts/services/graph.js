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
/* global jQuery, Rickshaw */
/**
 * @ngdoc function
 * @name IoTHackDayDashboard.service:Graph
 * @description
 * # Graph
 */
angular.module('IoTHackDayDashboard').
    service('Graph', function ($log){
    	var Graph = function(){
    		var palette = new Rickshaw.Color.Palette( { scheme: 'classic9' } );

            this.init = function(names, seriesData){
                var series = [];
                for (var i = 0; i < names.length && i < seriesData.length; i++){
                    series.push({
                        color: palette.color(),
                        data: seriesData[i],
                        name: names[i]
                    });
                }
                $log.debug(series);                

                // instantiate our graph!
                this.graph = new Rickshaw.Graph( {
                	element: document.getElementById('chart'),
                	width: 900,
                	height: 500,
                	renderer: 'area',
                	stroke: true,
                	preserve: true,
                	series: series
                } );

                this.graph.render();

                new Rickshaw.Graph.RangeSlider( {
                	graph: this.graph,
                	element: document.getElementById('preview')    	
                });

                new Rickshaw.Graph.HoverDetail( {
                	graph: this.graph,
                	xFormatter: function(x) {
                		return new Date(x * 1000).toString();
                	}
                } );

                new Rickshaw.Graph.Annotate( {
                	graph: this.graph,
                	element: document.getElementById('timeline')
                } );

                var legend = new Rickshaw.Graph.Legend( {
                	graph: this.graph,
                	element: document.getElementById('legend')

                } );

                new Rickshaw.Graph.Behavior.Series.Toggle( {
                	graph: this.graph,
                	legend: legend
                } );

                new Rickshaw.Graph.Behavior.Series.Order( {
                	graph: this.graph,
                	legend: legend
                } );

                new Rickshaw.Graph.Behavior.Series.Highlight( {
                	graph: this.graph,
                	legend: legend
                } );

                new Rickshaw.Graph.Smoother( {
                	graph: this.graph,
                	element: document.querySelector('#smoother')
                } );

                var ticksTreatment = 'glow';

                var xAxis = new Rickshaw.Graph.Axis.Time( {
                	graph: this.graph,
                	ticksTreatment: ticksTreatment,
                	timeFixture: new Rickshaw.Fixtures.Time.Local()
                } );

                xAxis.render();

                var yAxis = new Rickshaw.Graph.Axis.Y( {
                	graph: this.graph,
                	tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
                	ticksTreatment: ticksTreatment
                } );

                yAxis.render();


                new RenderControls( {
                	element: document.querySelector('form'),
                	graph: this.graph
                } );
            };

            this.update = function(){
            	this.graph.update();
            };
        };

        Graph.RandomData = function(size){
        	return new Rickshaw.Fixtures.RandomData(size);
        };

        var RenderControls = function(args) {

        	var $ = jQuery;

        	this.initialize = function() {

        		this.element = args.element;
        		this.graph = args.graph;
        		this.settings = this.serialize();

        		this.inputs = {
        			renderer: this.element.elements.renderer,
        			interpolation: this.element.elements.interpolation,
        			offset: this.element.elements.offset
        		};

        		this.element.addEventListener('change', function(e) {

        			this.settings = this.serialize();

        			if (e.target.name === 'renderer') {
        				this.setDefaultOffset(e.target.value);
        			}

        			this.syncOptions();
        			this.settings = this.serialize();

        			var config = {
        				renderer: this.settings.renderer,
        				interpolation: this.settings.interpolation
        			};

        			if (this.settings.offset === 'value') {
        				config.unstack = true;
        				config.offset = 'zero';
        			} else if (this.settings.offset === 'expand') {
        				config.unstack = false;
        				config.offset = this.settings.offset;
        			} else {
        				config.unstack = false;
        				config.offset = this.settings.offset;
        			}

        			this.graph.configure(config);
        			this.graph.render();

        		}.bind(this), false);
        	};

        	this.serialize = function() {

        		var values = {};
        		var pairs = $(this.element).serializeArray();

        		pairs.forEach( function(pair) {
        			values[pair.name] = pair.value;
        		} );

        		return values;
        	};

        	this.syncOptions = function() {

        		var options = this.rendererOptions[this.settings.renderer];

        		Array.prototype.forEach.call(this.inputs.interpolation, function(input) {

        			if (options.interpolation) {
        				input.disabled = false;
        				input.parentNode.classList.remove('disabled');
        			} else {
        				input.disabled = true;
        				input.parentNode.classList.add('disabled');
        			}
        		});

        		Array.prototype.forEach.call(this.inputs.offset, function(input) {

        			if (options.offset.filter( function(o) { return o === input.value; } ).length) {
        				input.disabled = false;
        				input.parentNode.classList.remove('disabled');

        			} else {
        				input.disabled = true;
        				input.parentNode.classList.add('disabled');
        			}

        		}.bind(this));

        	};

        	this.setDefaultOffset = function(renderer) {

        		var options = this.rendererOptions[renderer];

        		if (options.defaults && options.defaults.offset) {

        			Array.prototype.forEach.call(this.inputs.offset, function(input) {
        				if (input.value === options.defaults.offset) {
        					input.checked = true;
        				} else {
        					input.checked = false;
        				}

        			}.bind(this));
        		}
        	};

        	this.rendererOptions = {

        		area: {
        			interpolation: true,
        			offset: ['zero', 'wiggle', 'expand', 'value'],
        			defaults: { offset: 'zero' }
        		},
        		line: {
        			interpolation: true,
        			offset: ['expand', 'value'],
        			defaults: { offset: 'value' }
        		},
        		bar: {
        			interpolation: false,
        			offset: ['zero', 'wiggle', 'expand', 'value'],
        			defaults: { offset: 'zero' }
        		},
        		scatterplot: {
        			interpolation: false,
        			offset: ['value'],
        			defaults: { offset: 'value' }
        		}
        	};

        	this.initialize();
        };

        return Graph;
    });

