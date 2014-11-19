## Features
This package demonstrates how the Cross Region Replication library preview works. It launches the following components:

- __Cross Region Replication application__: manages replication groups and adding/removing of replication regions and tables.
- __Two instances of DynamoDB Local__: emulate 2 separate DynamoDB regions locally 
- __Two instances of demo sensor dashboard__: provide graphical interface to view generated sensor data in DynamoDB tables
- __Dummy data generator__: an utility that helps generate dummy data that feeds into the DynamoDB tables

## Quick Launch using Cloud Formation Template
The included cloud formation template will set up the environment and launch the demo automatically on an EC2 instance. If you choose to use this template instead of running the demo on your own computer, please skip the installation scripts below and start directly from [enabling replication](#demoVideo). For more information on cloud formation templates, please visit its [page](http://aws.amazon.com/cloudformation/aws-cloudformation-templates/).

## Minimum Requirements 
- Java 1.7+
- wget
- NodeJS
  - npm
  - coffee-script

## Getting Started
Note: You must have built the demo package from the source from the [root cross region replication directory](https://github.com/awslabs/dynamodb-cross-region-library) before you can run the demo using commands given below (i.e. you should have a demo/target directory before proceeding).

To run the demo, run the following commands:
```
    > cd target/dynamodb-cross-region-replication-demo
    > ./setup.sh 
    > ./run.sh
```

This should launch two DynamoDB Local instances at http://localhost:8000 and http://localhost:8001, with region names "us-east-1" and "ap-northeast-1", respectively.

To enable replication, access the Cross Region Replication application interface at http://localhost:8080, and follow the steps below:

<a name="demoVideo"></a>These steps are also illustrated in the [demo video](https://s3.amazonaws.com/dynamodb-cross-region/crr-demo-10e72af8246.mov).

1. Click to add new replication group.
2. Name your replication group (e.g. "sensorData")
3. Once the new replication group appears, click to view details.
4. A master table is automatically created in the us-east-1 region, and a replica table is automatically created in the ap-northeast-1 region. Note replication is OFF by default. 
5. Click on "ON" at the top of the replication group page to begin replication. Table status for master and replica should both switch to "Replicating" at this point.
6. That's it! You may use the sensor data dashboards to monitor the real-time replication. The dummy data generator is launched to continuously feed data into the master table of region us-east-1. This can be seen clearly from the sensor data dashboard available at http://localhost:10000, which visualizes data in the master DynamoDB table. Data in the replica table can be viewed at http://localhost:10001, which should be empty before replication begins and
become populated once replication starts.

To stop the demo, kill the script with `Ctrl+c` and run:
```
   ./stop.sh
```
This will stop the processes running background. 

