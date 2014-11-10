#!/bin/sh

DASHBOARD_DIR=dashboard
DYNAMODB_LOCAL_DIR=DynamoDBLocal
PORT1=8000
PORT2=8001

kill `cat $DYNAMODB_LOCAL_DIR/dynamodb_local_$PORT1.pid`
kill `cat $DYNAMODB_LOCAL_DIR/dynamodb_local_$PORT2.pid`
kill `cat $DASHBOARD_DIR/dashboard_port_offset0.pid`
kill `cat $DASHBOARD_DIR/dashboard_port_offset1.pid`
kill `cat $DASHBOARD_DIR/data_generator.pid`
