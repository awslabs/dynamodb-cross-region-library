# DynamoDB Table Copy Nanny

DynamoDB Table Copy Nanny is a program utilized by the [DynamoDB Replication Coordinator](https://www.github.com) in order to monitor the bootstrapping task of Amazon DynamoDB Cross Region Replication application.

##Usage
  - Java 7
    - [Amazon DynamoDB](http://aws.amazon.com/dynamodb/)
      - Node v0.12.5

### Motivation

Abstract the logistics of a bootstrap task, such that a third party need not interface and handle the logic of starting up, cancelling, failing out or finish a bootstrap task.

##How it Works
Currently the behavior is determined by the Main Class Runner and geared specifically for use by the Replication Coordinator on top of [Amazon EC2 Container Service](http://aws.amazon.com/ecs/).  

* Utilizes [DynamoDB Table Copy Client](http://github.com)
* Emits progress to [CloudWatch Metrics](http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/CW_Support_For_AWS.html)
* Receives signals from a DynamoDB metadata table
* Specifies timeout for tasks
* Emits Standard *Output* and *Error* to [CloudWatch Logs](http://aws.amazon.com/about-aws/whats-new/2014/07/10/introducing-amazon-cloudwatch-logs/)
* Executes necessary callbacks

Currently, the Table Copy Client is defined to spawn a subprocess in order to perform the bootstrap.  Under DynamoDBTableCopyUtilities, there are a few scripts written in Node that actually perform the task.

##Extensibility

* **DynamoDBTableCopyClient.java**
> How to define, run, and monitor the task (full reference [here](http://github.com))
* **CommandLineArgs.java**
> Define new parameters that will be supplied to the task
* **NannyDaemon.java**
> Define custom daemons to control and monitor the task (i.e. Cancellation, Timeout, etc.)
* **CancellationDaemon.java**
> Daemon polling metadata for a cancellation specific signal
* **TimeoutCalculator.java**
> Define the amount of time given before giving up
* **TableCopyUtils.java**
> Generic toolset for utility functions, currently implements responding to metadata


##License
----
Amazon Software License
