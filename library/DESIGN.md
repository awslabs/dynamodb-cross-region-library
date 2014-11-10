## Overview

This document contains detailed information on the design of the Cross Region Replication (CRR) library preview.

## Data Model

The eventual consistent guarantees of the CRR Library are maintained by a few rules:
1. All user writes are made to designated master tables only.
2. The only operations allowed by the user are PutItem and UpdateItem. The DeleteItem and BatchWriteItem operations are not allowed. Deletes are possible by replacing items with a tombstone item. This is discussed in detail below.
3. All writes by the user contain an attribute `<USER UPDATE FLAG>` to distinguish user writes from writes made by the CRR Library.
4. All writes by the user contain an attribute with a monotonically increasing value (padded to a fixed length) followed by a nonce. In this document this attribute will be called `<TIMESTAMP>`. A timestamp is a simple implementation of this requirement and the starter code provided by the CRR library uses a timestamp for this purpose.
5. All writes made are conditional based on the `<TIMESTAMP>` of the item being greater than an existing item's `<TIMESTAMP>` or an item not existing.

Because all items contain a `<TIMESTAMP>` attribute and every write is conditional on increasing the attribute, the latest version of an item will eventually replace all previous versions. Any writes where a newer item is present in the table will fail because of such conditions. In the case that conflicting writes are made to multiple master regions, one will have a greater value for `<TIMESTAMP>` and will propagate to all regions. The purpose of appending a nonce to the monotonically increasing field is to break the tie when the increasing value is equal. 

The CRR Library makes all writes using PutItem. This prevents inconsistency across tables in the replication group arising from UpdateItem where an untracked attribute is present in the item.

To get started, a Java SDK wrapper, `AmazonDynamoDBTimestampReplicationClient` is provided. All writes made through this API (including DeleteItem) adhere to the above rules.

### <a name="deletes"></a>Deletes

The DeleteItem operation would remove the assumption that the `<TIMESTAMP>` attribute is available to compare with in an existing item. If an item does not exist in the table, a write must succeed to allow for the creation of items in the table. Here is an example of how a delete would result in an inconsistent state: 
1. Begin with Table1 and Table2 with an item I0 in both tables.
2. I1 replaces I0 in Table1 at Time1.
3. Before I1 is replicated to Table 2, I0 is deleted in Table2 at Time2.
4. The replication for deleting I0 arrives at Table1, since Time2 > Time1, the delete goes through and I1 is deleted from Table1.
4. Finally, the replication for I1 arrives at Table2. Because there is no longer an item in Table2, I1 is written to Table2. 
5. This leaves no item in Table1 and I1 in Table2, an inconsistent state.

Instead of actually deleting items from the table, the CRR Library replaces an item with a tombstone version. The tombstone version contains only the item key, the timestamp, and a special tombstone flag. Items with the tombstone flag can be filtered out of Query, Scan, GetItem, and BatchGetItem results.

## Architecture
### Replication Policy
A ReplicationPolicy defines the following behaviors of the replication group:
- DeleteBehavior: In the preview release, the only DeleteBehavior available is TOMBSTONE, which implements the item tombstoning described above in [Deletes](#deletes).
- PolicyViolationBehavior: PolicyViolationBehavior can be either IGNORE or ROLLBACK. Each user write that is processed by the CRR Library will be checked to see if it complies with the monotonically increasing attribute (complying with the CRR Library write rules). If a violation is detected, the IGNORE behavior will log a warning that the tables are now in an inconsistent state, but will continue replicating. The ROLLBACK behavior tells the CRR Library to attempt to revert the item back to the replaced version. This may fail and leave the tables in an inconsistent state. 
- Additionally the following methods must be implemented by the ReplicationPolicy: isValidUpdate, getConditionExpression, and getExpressionAttributes. These methods define how the CRR Library sets the condition expression and expression attributes for conditional writes. isValidUpdate is used to detect invalid user writes.

BasicTimestampReplicationPolicy is an implementation of ReplicationPolicy that uses an ISO-8601 UTC timestamp as the monotonically increasing value.

### Replication Group and Replication Coordinator
A replication group is managed by a RegionReplicationCoordinator. Each master DynamoDB table requires its own KCL application to process its DynamoDB Stream. The RegionReplicationCoordinator provides the following actions: 
- AddRegion
- RemoveTable 
- RemoveRegion 
- StartReplicationWorkers
- StopReplicationWorkers

The RegionReplicationCoordinator uses a ReplicationConfiguration to keep track of replication group state. Additionally, a single ReplicationPolicy defines how the KCL workers process records.

A LocalRegionReplicationCoordinator runs the CRR Library in a local JVM. It uses the LocalShardSubscriberProxy and LocalTableApplierProxy to allow the KCL components to communicate within the local JVM. Alternative implementations of the proxies could allow these processes to be distribtued across multiple processes or machines.

### KCL Application

The preview release of the CRR Library runs all of the KCL workers and replication coordinator in the same JVM. The architecture is designed to be scalable, so alternative implementations could include each KCL application running in an EC2 autoscaling group.

The KCL implementation consists of two main components: 
1. ShardSubscriber: implements the IRecordProcessor interface of KCL. It processes DynamoDB Stream records using the following logic:
    - If a write does not contain the `<USER UPDATE FLAG>` attribute, ignore it because it was made by the CRR Library. Otherwise, remove the `<USER UPDATE FLAG>` attribute and propagate the write to the Applier for each table in the replication group (except the source table).
    - Keep track of acknowledgements from Appliers for each sequence number
    - Once Appliers for all tables in the replication group acknowledge a write for a sequence number, checkpoint on that sequence number.
2. Applier: uses the ReplicationPolicy to make the conditional write to a table in the replication group. Once the write is successful, it sends an acknowledgement back to the ShardSubscriber that processed the user write.

## Release Notes
1. The CRR Library currently contains some experimental features:
    - Bootstrapping tables and adding them to an active replication group
    - Rolling back invalid user writes (ReplicationPolicy.ROLLBACK)
2. The CRR library uses the new expressions API for updates and conditional writes. Using the legacy expected API will result in a ValidationException
