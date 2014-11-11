## Overview
The Amazon DynamoDB Cross Region Replication (CRR) Manager an application that uses the Amazon DynamoDB CRR library to manage multiple replication groups with Web GUI. It launches an instance of Replication Group Coordinator that is implemented in the Amazon DynamoDB CRR library for each replication group. It interacts with each Replication Group Coordinator via the HTTP interface and manages its life cycle. It also fetches metrics statistics for each replication group via Amazon CloudWatch and display them on the Web interface.

## Components
- CRR Manager backend
  - Maintains configuration for each replication group 
  - Launches and terminates the Replication Group Coordinator for each replication group
  - Provisions and controls the Replication Group Coordinator via its HTTP API
  - Provides Web GUI frontend with API to view and manage replication groups that the user owns.
- Web GUI frontend
  - Communicates with CRR Manager backend with HTTP REST API
  - Displays the configurations and status of replication groups and tables in each group
  - Provides management interface to create/delete/modify replication groups
  - Displays CloudWatch metrics for replication statuses

## Project structure and artifact
- CRR Manager backend's source code is stored under `src` with the standard maven webapp project structure
- Web GUI frontend's source code is found under `yo` with AngularJS project built with grunt
- The final artifact is a standard web application archive (WAR) file that contains both the CRR Manager backend and the Web GUI frontend that can be deployed on a web servlet container such as jetty and glassfish.

## Getting started

1. Install maven, node.js and ruby for your platform if not installed
   - maven: http://maven.apache.org/
   - node.js: http://nodejs.org/download/
   - ruby: https://www.ruby-lang.org/en/installation/

2. Install tools necessary to build the Web GUI frontend, i.e. grunt, bower, coffee-script and compass
```
  sudo npm -g install grunt-cli bower coffee-script
  
  sudo gem install compass
```

3. Run the following command to build a WAR file 
```
  mvn install
```

4. Run the war file with e.g. Jetty or deploy on a web servlet container. One way to do this is to use jetty runner (https://wiki.eclipse.org/Jetty/Howto/Using_Jetty_Runner). 
```
  java -jar jettry-runner.jar target/dynamodb-cross-region-replication-manager.war
```

5. Open the web inteface with your favorite web browser (the command below launches the website on port 8080)
```
   open http://localhost:8080
```

## Release Notes
- The CRR Manager currently stores configuration on memory. Persistent storage can be implemented by overriding `com.amazonaws.services.dynamodbv2.replication.manager.data.DataStore` interface.
- Bootstrapping a Replication Group Coordinator tables and some of the API calls are done through local Java interface. A distributed deployment can be implemented once the CRR library implements necessary API calls. 
