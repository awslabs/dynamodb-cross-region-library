## Overview
Launcher for the sensor data dashboard used for demonstrating DynamoDB Cross Region Replication. It launches an HTTP server that serves static files for the dashboard and proxies request to DynamoDB Local. It also generates a configuration file for the client to connect to DynamoDB Local.

## Getting started
1. Install node.js and ruby for your platform if not installed
   - node.js: http://nodejs.org/download/
2. Install dependencies
```
  npm install
```
4. Run the following command to start the app. 
```
  npm start
```

## Configuration parameters
The following environment variables are used to alternate the configuration.
- `PORT_OFFSET` 
By default the HTTP server listens on port 10000 and proxies request to /dynamodb to port 8000 on the same host. 
- `REGION`
By default the client is configured to use us-east-1. This environment variable is used to alter the configuration. 


