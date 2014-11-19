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

 // Generated on 2014-08-19 using generator-angular 0.9.5
'use strict';

// # Globbing
// for performance reasons we're only matching one level down:
// 'test/spec/{,*/}*.js'
// use this if you want to recursively match all subfolders:
// 'test/spec/**/*.js'


module.exports = function (grunt) {

  // Load grunt tasks automatically
  require('load-grunt-tasks')(grunt);

  // Time how long tasks take. Can help when optimizing build times
  require('time-grunt')(grunt);

  // Configurable paths for the application
  var appConfig = {
    app: require('./bower.json').appPath || 'app',
    dist: 'dist'
  };

  // Load pre-configured nconf object
  var nconf = require('./lib/mynconf');

  grunt.loadNpmTasks('grunt-contrib-jade');
  grunt.loadNpmTasks('grunt-curl');  
  grunt.loadNpmTasks('grunt-tar.gz');      
  grunt.loadNpmTasks('grunt-if-missing');
  grunt.loadNpmTasks('grunt-bg-shell');  

  // Define the configuration for all the tasks
  grunt.initConfig({

    // Project settings
    yeoman: appConfig,

    // Downloads local dynamoDB for dev and test
    curl: {
      dynamoDBLocal: {
        dest: '.dependencies/DynamoDBLocal.tgz',
        src: 'http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest'
      }
    },

    // Unpack local DynamoDB
    targz: {
      dynamoDBLocal: {
        files: {
          '.dependencies/': '.dependencies/DynamoDBLocal.tgz'
        }
      },
    },

    // Start DynamoDB Local
    bgShell: {
      dynamoDBLocal: {
        cmd: 'java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb',
        bg: true,
        stderr: function(){
        },
        execOpts: {
          cwd: '.dependencies'
        }
      },
      prepareTables: {
        cmd: 'AWS_ACCESS_KEY=DummyAccessKey AWS_SECRET_ACCESS_KEY=DummySecretKey DYNAMODB_ENDPOINT=http://localhost:8000 DYNAMODB_REGION=us-east-1 delete_table_if_exists=1 coffee ./lib/prepare_tables.coffee'
      },
      generateData: {
        cmd: 'AWS_ACCESS_KEY=DummyAccessKey AWS_SECRET_ACCESS_KEY=DummySecretKey DYNAMODB_ENDPOINT=http://localhost:8000 DYNAMODB_REGION=us-east-1 coffee ./lib/generate_dummy_data.coffee'
      }      
    },

    // Environemnt settings
    ngconstant: {
      // Options for all targets
      options: {
        space: '  ',
        wrap: '"use strict";\n\n {%= __ngModule %}',
        name: 'config',
        serializerOptions: {indent: '\t'}
      },
      // Environment targets
      development: {
        options: {
          dest: '.tmp/scripts/config.js'
        },
        constants: {
          ENV: {
            name: 'development',
            awsAccountId: nconf.get('AWS_ACCOUNT_ID'),
            identityPoolId: nconf.get('COGNITO_IDENTITY_POOL_ID'),
            unauthRoleArn: nconf.get('COGNITO_UNAUTH_ROLE_ARN'),            
            useCognitoIdentity: nconf.get('USE_COGNITO_DEV'),                       
            userId: 'localUser',
            accessKeyId: 'DummyAccessKey',
            secretAccessKey: 'DummySecretKey',
            dynamoDBRegion: nconf.get('DYNAMODB_REGION_DEV'),
            dynamoDBEndpoint: nconf.get('DYNAMODB_ENDPOINT_DEV'),
            commonTable: nconf.get('TABLE_COMMON'),
            leaderboardTable: nconf.get('TABLE_LEADERBOARD')
          }
        }
      },
      test: {
        options: {
          dest: '.tmp/scripts/config.js'
        },
        constants: {
          ENV: {
            name: 'test',
            awsAccountId: nconf.get('AWS_ACCOUNT_ID'),
            identityPoolId: nconf.get('COGNITO_IDENTITY_POOL_ID'),
            unauthRoleArn: nconf.get('COGNITO_UNAUTH_ROLE_ARN'),            
            useCognitoIdentity: nconf.get('USE_COGNITO_TEST'),                                   
            userId: 'localUser',            
            accessKeyId: 'DummyAccessKey',
            secretAccessKey: 'DummySecretKey',
            dynamoDBRegion: nconf.get('DYNAMODB_REGION_TEST'),
            dynamoDBEndpoint: nconf.get('DYNAMODB_ENDPOINT_TEST'),
            commonTable: nconf.get('TABLE_COMMON'),
            leaderboardTable: nconf.get('TABLE_LEADERBOARD')
          }
        }
      },
      production: {
        options: {
          dest: '.tmp/scripts/config.js'
        },
        constants: {
          ENV: {
            name: 'production',
            awsAccountId: nconf.get('AWS_ACCOUNT_ID'),
            identityPoolId: nconf.get('COGNITO_IDENTITY_POOL_ID'),
            unauthRoleArn: nconf.get('COGNITO_UNAUTH_ROLE_ARN'),
            useCognitoIdentity: nconf.get('USE_COGNITO_PROD'),
            userId: 'localUser',            
            accessKeyId: 'DummyAccessKey',
            secretAccessKey: 'DummySecretKey',
            dynamoDBRegion: nconf.get('DYNAMODB_REGION_PROD'),
            dynamoDBEndpoint: nconf.get('DYNAMODB_ENDPOINT_PROD'),
            commonTable: nconf.get('TABLE_COMMON'),
            leaderboardTable: nconf.get('TABLE_LEADERBOARD')
          }
        }
      }
    },

    // Watches files for changes and runs tasks based on the changed files
    watch: {
      bower: {
        files: ['bower.json'],
        tasks: ['wiredep']
      },
      js: {
        files: ['<%= yeoman.app %>/scripts/{,*/}*.js'],
        tasks: ['newer:jshint:all'],
        options: {
          livereload: '<%= connect.options.livereload %>'
        }
      },
      jade: {
        files: ['<%= yeoman.app %>/**/*.jade'],
        tasks: ['jade', 'wiredep'],
        options: {
          livereload: '<%= connect.options.livereload %>'
        }
      },
      jsTest: {
        files: ['test/spec/{,*/}*.js'],
        tasks: ['newer:jshint:test', 'karma']
      },
      compass: {
        files: ['<%= yeoman.app %>/styles/{,*/}*.{scss,sass}'],
        tasks: ['compass:server', 'autoprefixer']
      },
      gruntfile: {
        files: ['Gruntfile.js']
      },
      livereload: {
        options: {
          livereload: '<%= connect.options.livereload %>'
        },
        files: [
          '<%= yeoman.app %>/{,*/}*.html',
          '.tmp/styles/{,*/}*.css',
          '<%= yeoman.app %>/images/{,*/}*.{png,jpg,jpeg,gif,webp,svg}'
        ]
      }
    },
    jade: {
      compile: {
        options: {
          client: false,
          pretty: true
        },
        files: [ {
          src: '**/*.jade',
          dest: '.tmp',
          ext: '.html',
          expand: true,
          cwd: 'app'
        } ]
      },
      dist: {
        options: {
          client: false,
          pretty: true
        },
        files: [ {
          src: '**/*.jade',
          dest: '<%= yeoman.dist %>',
          ext: '.html',
          expand: true,
          cwd: 'app'
        } ]
      }
    },

    // The actual grunt server settings
    connect: {
      proxies: [
        {
          context: '/dynamodb',
          host: 'localhost',
          port: '8000',
          https: false,
          changeOrigin: false,
          rewrite: {
            '^/dynamodb': ''
          }
        }
      ],
      options: {
        port: 9000,
        // Change this to '0.0.0.0' to access the server from outside.
        hostname: 'localhost',
        livereload: 35729
      },
      livereload: {
        options: {
          open: true,
          middleware: function (connect) {
            return [
              connect.static('.tmp'),
              connect().use(
                '/bower_components',
                connect.static('./bower_components')
              ),
              connect.static(appConfig.app),
              require('grunt-connect-proxy/lib/utils').proxyRequest         
            ];
          }
        }
      },
      test: {
        options: {
          port: 9001,
          middleware: function (connect) {
            return [
              connect.static('.tmp'),
              connect.static('test'),
              connect().use(
                '/bower_components',
                connect.static('./bower_components')
              ),
              connect.static(appConfig.app),
              require('grunt-connect-proxy/lib/utils').proxyRequest
            ];
          }
        }
      },
      dist: {
        options: {
          open: true,
          base: '<%= yeoman.dist %>'
        }
      }
    },

    // Make sure code styles are up to par and there are no obvious mistakes
    jshint: {
      options: {
        jshintrc: '.jshintrc',
        reporter: require('jshint-stylish')
      },
      all: {
        src: [
          'Gruntfile.js',
          '<%= yeoman.app %>/scripts/*.js',
          '<%= yeoman.app %>/scripts/controllers/*.js',
          '<%= yeoman.app %>/scripts/services/*.js',          
        ]
      },
      test: {
        options: {
          jshintrc: 'test/.jshintrc'
        },
        src: ['test/spec/{,*/}*.js']
      }
    },

    // Empties folders to start fresh
    clean: {
      dist: {
        files: [{
          dot: true,
          src: [
          '.tmp',
          '<%= yeoman.dist %>/{,*/}*',
          '!<%= yeoman.dist %>/.git*'
          ]
        }]
      },
      server: '.tmp',
      dependencies: '.dependencies'
    },

    // Add vendor prefixed styles
    autoprefixer: {
      options: {
        browsers: ['last 1 version']
      },
      dist: {
        files: [{
          expand: true,
          cwd: '.tmp/styles/',
          src: '{,*/}*.css',
          dest: '.tmp/styles/'
        }]
      }
    },

    // Automatically inject Bower components into the app
    wiredep: {
      options: {
        cwd: '.',
        exclude: [/rickshaw/]
      },
      app: {
        src: ['.tmp/index.html'],
        ignorePath:  /\.\.\//
      },
      dist: {
        src: ['<%= yeoman.dist %>/index.html'],
        ignorePath:  /\.\.\//
      },    
      test: {
        src: ['test/karma.conf.js'],
        fileTypes: {
          js: {
            block: /(([\s\t]*)\/\/\s*bower:*(\S*))(\n|\r|.)*?(\/\/\s*endbower)/gi,
            detect: {
              js: /'(.*\.js)'/gi
            },
            replace: {
              js: '\'{{filePath}}\','
            }
          }
        },
        devDependencies: true,
        exclude: [/bootstrap-sass-official/, /jasmine/],
        ignorePath:  /\.\.\//        
      },              
      sass: {
        src: ['<%= yeoman.app %>/styles/{,*/}*.{scss,sass}'],
        ignorePath: /(\.\.\/){1,2}bower_components\//
      }
    },

    // Compiles Sass to CSS and generates necessary files if requested
    compass: {
      options: {
        sassDir: '<%= yeoman.app %>/styles',
        cssDir: '.tmp/styles',
        generatedImagesDir: '.tmp/images/generated',
        imagesDir: '<%= yeoman.app %>/images',
        javascriptsDir: '<%= yeoman.app %>/scripts',
        fontsDir: '<%= yeoman.app %>/styles/fonts',
        importPath: './bower_components',
        httpImagesPath: '/images',
        httpGeneratedImagesPath: '/images/generated',
        httpFontsPath: '/styles/fonts',
        relativeAssets: false,
        assetCacheBuster: false,
        raw: 'Sass::Script::Number.precision = 10\n'
      },
      dist: {
        options: {
          generatedImagesDir: '<%= yeoman.dist %>/images/generated'
        }
      },
      server: {
        options: {
          debugInfo: true
        }
      }
    },

    // Renames files for browser caching purposes
    filerev: {
      dist: {
        src: [
          '<%= yeoman.dist %>/scripts/{,*/}*.js',
          '<%= yeoman.dist %>/styles/{,*/}*.css',
          '<%= yeoman.dist %>/images/{,*/}*.{png,jpg,jpeg,gif,webp,svg}',
          '<%= yeoman.dist %>/styles/fonts/*'
        ]
      }
    },

    // Reads HTML for usemin blocks to enable smart builds that automatically
    // concat, minify and revision files. Creates configurations in memory so
    // additional tasks can operate on them
    useminPrepare: {
      html: '<%= yeoman.dist %>/index.html',
      options: {
        dest: '<%= yeoman.dist %>',
        flow: {
          html: {
            steps: {
              js: ['concat', 'uglifyjs'],
              css: ['cssmin']
            },
            post: {}
          }
        }
      }
    },

    // Performs rewrites based on filerev and the useminPrepare configuration
    usemin: {
      html: ['<%= yeoman.dist %>/**/*.html'],
      css: ['<%= yeoman.dist %>/styles/{,*/}*.css'],
      options: {
        assetsDirs: ['<%= yeoman.dist %>','<%= yeoman.dist %>/images']
      }
    },

    // The following *-min tasks will produce minified files in the dist folder
    // By default, your `index.html`'s <!-- Usemin block --> will take care of
    // minification. These next options are pre-configured if you do not wish
    // to use the Usemin blocks.
    // cssmin: {
    //   dist: {
    //     files: {
    //       '<%= yeoman.dist %>/styles/main.css': [
    //         '.tmp/styles/{,*/}*.css'
    //       ]
    //     }
    //   }
    // },
    // uglify: {
    //   dist: {
    //     files: {
    //       '<%= yeoman.dist %>/scripts/scripts.js': [
    //         '<%= yeoman.dist %>/scripts/scripts.js'
    //       ]
    //     }
    //   }
    // },
    // concat: {
    //   dist: {}
    // },

    imagemin: {
      dist: {
        files: [{
          expand: true,
          cwd: '<%= yeoman.app %>/images',
          src: '{,*/}*.{png,jpg,jpeg,gif}',
          dest: '<%= yeoman.dist %>/images'
        }]
      }
    },

    svgmin: {
      dist: {
        files: [{
          expand: true,
          cwd: '<%= yeoman.app %>/images',
          src: '{,*/}*.svg',
          dest: '<%= yeoman.dist %>/images'
        }]
      }
    },

    htmlmin: {
      dist: {
        options: {
          collapseWhitespace: true,
          conservativeCollapse: true,
          collapseBooleanAttributes: true,
          removeCommentsFromCDATA: true,
          removeOptionalTags: true
        },
        files: [{
          expand: true,
          cwd: '<%= yeoman.dist %>',
          src: ['*.html', 'views/{,*/}*.html'],
          dest: '<%= yeoman.dist %>'
        }]
      }
    },

    // ngmin tries to make the code safe for minification automatically by
    // using the Angular long form for dependency injection. It doesn't work on
    // things like resolve or inject so those have to be done manually.
    ngmin: {
      dist: {
        files: [{
          expand: true,
          cwd: '.tmp/concat/scripts',
          src: '*.js',
          dest: '.tmp/concat/scripts'
        }]
      }
    },

    // Replace Google CDN references
    cdnify: {
      dist: {
        html: ['<%= yeoman.dist %>/*.html']
      }
    },

    // Copies remaining files to places other tasks can use
    copy: {
      dist: {
        files: [
        {
          expand: true,
          dot: true,
          cwd: '<%= yeoman.app %>',
          dest: '<%= yeoman.dist %>',
          src: [
            '*.{ico,png,txt}',
            '.htaccess',
            '*.html',
            'views/{,*/}*.html',
            'images/**/*',
            'fonts/*',
            'scripts/**/*.js'            
          ]
        }, {
          expand: true,
          cwd: '.tmp/images',
          dest: '<%= yeoman.dist %>/images',
          src: ['generated/*']
        }, {
          expand: true,
          cwd: '.tmp',
          dest: '<%= yeoman.dist %>/',
          src: ['scripts/**/*.js']
        }, {          
          expand: true,
          cwd: '.',
          src: 'bower_components/rickshaw/rickshaw.min.js',
          dest: '<%= yeoman.dist %>'
        }, {          
          expand: true,
          cwd: 'bower_components/rickshaw/examples/images/',
          src: '*.png',
          dest: '<%= yeoman.dist %>/images'
        }, {          
          expand: true,
          cwd: '.',
          src: 'bower_components/bootstrap-sass-official/assets/fonts/bootstrap/*',
          dest: '<%= yeoman.dist %>'
        }, {                            
          expand: true,
          cwd: 'bower_components/font-awesome',
          src: 'fonts/*',
          dest: '<%= yeoman.dist %>'
        }
        ]
      },
      tmp: {
        files: [
        {          
          expand: true,
          cwd: 'bower_components/rickshaw/examples/images/',
          src: '*.png',
          dest: '.tmp/images'
        }
        ]                           
      },
      styles: {
        expand: true,
        cwd: '<%= yeoman.app %>/styles',
        dest: '.tmp/styles/',
        src: '{,*/}*.css'
      }
    },

    // Run some tasks in parallel to speed up the build process
    concurrent: {
      server: [
        'compass:server'
      ],
      test: [
        'compass'
      ],
      dist: [
        'compass:dist',
        'svgmin'
      ]
    },

    // Test settings
    karma: {
      unit: {
        configFile: 'test/karma.conf.js',
        singleRun: true
      }
    }
  });

  grunt.registerTask('launch_dynamodb_local', 'Launches DynamoDB Local', function () {
    grunt.task.run([
      'if-missing:curl:dynamoDBLocal',
      'targz:dynamoDBLocal',     
      'bgShell:dynamoDBLocal'
    ]);
  });

  grunt.registerTask('prepare_tables', 'Creates tables in DynamoDB Local', function () {
    grunt.task.run([
      'bgShell:prepareTables'
    ]);
  });

  grunt.registerTask('generate_data', 'Generates dummy data and writes to the table', function () {
    grunt.task.run([
      'bgShell:generateData'
    ]);
  });


  grunt.registerTask('serve', 'Compile then start a connect web server', function (target) {
    if (target === 'dist') {
      return grunt.task.run(['build', 'connect:dist:keepalive']);
    }

    grunt.task.run([
      'clean:server',
      'ngconstant:development',
      'launch_dynamodb_local',
      'jade:compile',
      'copy:tmp',
      'wiredep',
      'concurrent:server',
      'autoprefixer',
      'configureProxies:server',
      'prepare_tables',
      'connect:livereload',
      'watch'
    ]);
  });

  grunt.registerTask('test', [
    'clean:server',
    'ngconstant:test', 
    'launch_dynamodb_local',
    'copy:tmp',
    'jade:dist',    
    'wiredep',   
    'concurrent:test',
    'configureProxies:server',
    'autoprefixer',
    'prepare_tables',
    'connect:test',
    'karma'
  ]);

  grunt.registerTask('build', [
    'clean:dist',
    'ngconstant:production',
    'jade:dist',    
    'wiredep',
    'useminPrepare',
    'concurrent:dist',
    'autoprefixer',
    'concat',
    'copy:dist',
    'cssmin',
    'uglify',
    'usemin'
  ]);

  grunt.registerTask('default', [
    'newer:jshint',
    'build'
  ]);
};
