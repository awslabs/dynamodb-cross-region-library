# DynamoDB Cross-Region Replication Library Release Notes

This document summarizes the changes in each release of the DynamoDB Cross-Region Replication library.

## 1.2.1
Updates dependency on DynamoDB Streams Kinesis Adapter to 1.2.1.

## 1.2.0
Updates the KCL, kinesis-connectors, dynamodb-streams-kinesis-adapter and AWS SDK dependencies to recent versions.
To be consistent with the AWS SDK's new focus on regions and to make it even easier to hit the ground running,
you no longer have to specify full HTTPS endpoints when starting the cross-region replication library.
You will need to replace `--sourceEndpoint https://dynamodb.us-east-1.amazonaws.com` with `--sourceRegion us-east-1`.
The same applies for `--destinationRegion` being required instead of `--destinationEndpoint` when launching the CRR
command line. These changes among other optional parameters are documented in the README and they facilitate
testing cross-region replication with DynamoDB Local.

## 1.1.0
Major refactor of the Cross-Region Replication Library that eliminates the AWS console integration of the
stack. Now, you can achieve cross-region replication on DynamoDB with a few commands on the command
line on an EC2 instance.

## 1.0.1
Updated README.md with getting started guide link

## 1.0.0
The initial version of the Cross-Region Replication Library was a semi-hosted EC2 application that
extended the AWS console.
