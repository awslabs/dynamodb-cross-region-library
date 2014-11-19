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

// Karma configuration
// http://karma-runner.github.io/0.12/config/configuration-file.html
// Generated on 2014-08-19 using
// generator-karma 0.8.3

module.exports = function(config) {
  'use strict';

  config.set({
    // enable / disable watching file and executing tests whenever any file changes
    autoWatch: true,

    // base path, that will be used to resolve files and exclude
    basePath: '../',

    // testing framework to use (jasmine/mocha/qunit/...)
    frameworks: ['jasmine'],

    preprocessors: {
      '**/*.html': ['ng-html2js']
    },

    // list of files / patterns to load in the browser
    files: [
       // bower:js
       'bower_components/jquery/dist/jquery.js',
       'bower_components/es5-shim/es5-shim.js',
       'bower_components/angular/angular.js',
       'bower_components/json3/lib/json3.js',
       'bower_components/bootstrap-sass-official/assets/javascripts/bootstrap/affix.js',
       'bower_components/bootstrap-sass-official/assets/javascripts/bootstrap/alert.js',
       'bower_components/bootstrap-sass-official/assets/javascripts/bootstrap/button.js',
       'bower_components/bootstrap-sass-official/assets/javascripts/bootstrap/carousel.js',
       'bower_components/bootstrap-sass-official/assets/javascripts/bootstrap/collapse.js',
       'bower_components/bootstrap-sass-official/assets/javascripts/bootstrap/dropdown.js',
       'bower_components/bootstrap-sass-official/assets/javascripts/bootstrap/tab.js',
       'bower_components/bootstrap-sass-official/assets/javascripts/bootstrap/transition.js',
       'bower_components/bootstrap-sass-official/assets/javascripts/bootstrap/scrollspy.js',
       'bower_components/bootstrap-sass-official/assets/javascripts/bootstrap/modal.js',
       'bower_components/bootstrap-sass-official/assets/javascripts/bootstrap/tooltip.js',
       'bower_components/bootstrap-sass-official/assets/javascripts/bootstrap/popover.js',
       'bower_components/angular-resource/angular-resource.js',
       'bower_components/angular-sanitize/angular-sanitize.js',
       'bower_components/angular-touch/angular-touch.js',
       'bower_components/angular-route/angular-route.js',
       'bower_components/d3/d3.js',
       'bower_components/jquery-ui/jquery-ui.js',
       'bower_components/angular-bootstrap/ui-bootstrap-tpls.js',
       'bower_components/aws-sdk/dist/aws-sdk.js',
       'bower_components/angular-mocks/angular-mocks.js',
       // endbower
       'app/scripts/**/*.js',
       '.tmp/scripts/**/*.js',
       'test/spec/**/*.js',
       'dist/**/*.html'
    ],

    // list of files / patterns to exclude
    exclude: [
    'node_modules/**/*.html',
    'bower_components/**/*.html'
    ],

    // web server port
    port: 8080,

    // Start these browsers, currently available:
    // - Chrome
    // - ChromeCanary
    // - Firefox
    // - Opera
    // - Safari (only Mac)
    // - PhantomJS
    // - IE (only Windows)
    browsers: [
      'PhantomJS'
    ],

    // Which plugins to enable
    plugins: [
      'karma-phantomjs-launcher',
      'karma-ng-html2js-preprocessor',
      'karma-jasmine'
    ],

    // Continuous Integration mode
    // if true, it capture browsers, run tests and exit
    singleRun: false,

    colors: true,

    // level of logging
    // possible values: LOG_DISABLE || LOG_ERROR || LOG_WARN || LOG_INFO || LOG_DEBUG
    logLevel: config.LOG_INFO,

    proxies: {
       '/dynamodb/': 'http://localhost:8000/'
    },

    ngHtml2JsPreprocessor: {
      cacheIdFromPath: function(filepath) {
        filepath = filepath.replace(/^dist\//, '');
        filepath = filepath.replace(/^app\//, '');        
        return filepath;
      }
    }
  });
};
