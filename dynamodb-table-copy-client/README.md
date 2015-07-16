# DynamoDBTableCopyClient
DynamoDBTableCopyClient provides a fault tolerant self contained method for copying data between DynamoDB tables. It uses a table copy workflow to keep track of progress. At any point, if the calling process driving the workflow fails, it can restart and resume the workflow from its last checkpoint. The workflow can also be configured to run a callback method after completion, and can be canceled at any point.

### Using the client
##### Create the client
The DynamoDBTableCopyClient is responsible for launching table copy tasks. You must create the client by passing in a TableCopyMetadataAccess, TableCopyTaskHandlerFactory, and ExecutorService. The TableCopyMetadataAccess will allow the table copy workflow to checkpoint its step progress. The TableCopyTaskHandlerFactory will create task handlers for each table copy request, which specify how to perform the copy. The ExecutorService will be used to drive table copy tasks in a separate thread. 
##### Launch the table copy task
To execute a table copy, you call launchTableCopy() with a TableCopyRequest. The TableCopyRequest consists of the source region, source table name, source table read throughput percentage, destination region, destination table name, and destination write throughput percentage. You can also specify a callback method which will be run at the end of the table copy task.
##### Monitor or cancel the table copy task
The launchTableCopy() method returns a tracker, which can be used to monitor the table copy task status or cancel the task altogether. 

### Package layout
* client - table copy client
* client.ecs - table copy implementation using ECS
* client.exceptions - client side exceptions
* client.metadataaccess - workflow step metadata storage access implementations
* client.tablecopy - table copy task related code, including request details, workflow steps, and workflow trackers