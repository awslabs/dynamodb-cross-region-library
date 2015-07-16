# DynamoDB Utilities
## Get started
1. npm install
2. Run commands in ./bin

Common options are as follows:
```
  -d, --debug                       Set log level to debug
  --help                            Print usage instructions
```

### Copy table
It copies a table to another. The destination table must exist and have the same schema as the source table. 
```
 	$ copy_table [region:]<src table name> [region:]<dst table name>
```

It spawns a number of processes that is the same as the number of CPU cores on the host. By default, each process takes 1 segment. If the total number of segments is specified, the segments are distributed to each process equally. 

Source and destination endpoints, and total number of segments in parallel scan can be specified as follows:
```
 	$ copy_table --sourceEndpoint http://localhost:8000 --destinationEndpoint http://localhost:8000 --totalSegments 16 [region:]<src table name> [region:]<dst table name>
```

### Export table to Amazon S3 as compressed JSON files
It exports items in the source table as JSON array and export to S3. It applies gzip compression on the fly. 
```
	$ export_to_s3 [region:]<src table name> s3://<bucket name>/<object prefix>
```

It spawns a number of processes that is the same as the number of CPU cores on the host. By default, each process takes 1 segment. If the total number of segments is specified, the segments are distributed to each process equally. 

Source endpoint and total number of segments in parallel scan can be specified as follows:
```
 	$ export_to_s3 --sourceEndpoint http://localhost:8000 --totalSegments 16 [region:]<src table name> s3://<bucket name>/<object prefix>
```

### Import table from JSON files on Amazon S3 
It imports JSON files stored in S3 bucket and restore to the destination table. Gzip compressed files that end with `.gz` suffix and plain text JSON files are supported. 
```
	$ import_from_s3 s3://<bucket name>/<object prefix> [region:]<dst table name> 
```

It spawns a number of processes that is the same as the number of CPU cores on the host. The keys found under the specified prefix are distributed to each process equally.

Destination endpoint can be specified as follows:
```
	$ import_from_s3 --destinationEndpoint http://localhost:8000 s3://<bucket name>/<object prefix> [region:]<dst table name> 
```

### Count items in DynamoDB table
It counts number of items by performing parallel scan with `Select: 'Count'`. 
```
	$ count_items [region:]<table name> [# of total segments]
```
By default, the number of total segments is set to 4. 


### Export table to Redis
```
	$ export_to_redis [region:]<src table name> redis://<hostname>:<port>[/<database id>]
```

### Import table from Redis data store
```
	$ import_from_redis redis://<hostname>:<port>[/<database id>] [region:]<dst table name> 
```

## Test
1. Install and run DynamoDB local and Redis on localhost.
2. npm test

## Execute with debug output
LOG_LEVEL=debug <command>
