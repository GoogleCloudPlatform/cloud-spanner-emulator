# Changes

## v1.0.0

### New Features

* Added support for Cloud Spanner C# client library
* Added SQL Functions - FORMAT, FROM_BASE32, TO_BASE32
* Additional Information Schema features:
  * Reflected INFORMATION_SCHEMA tables within INFORMATION_SCHEMA.TABLES
  * Added SPANNER_IS_MANAGED column to INFORMATION_SCHEMA.INDEXES
  * Added SPANNER_STATE column to INFORMATION_SCHEMA.TABLES
  * Added INDEX_TYPE column to INFORMATION_SCHEMA.INDEX_COLUMNS
* Partitioned APIs now checks for partitionability (with limitations)

### Bugs

* Fixed bug to correctly return the status code for ExecuteBatchDml rpc.

## v0.8.0

### New Features

* PartitionRead, PartitionQuery and PartitionDML API support
* Cloud Spanner Client Libraries - PHP, Ruby, Python and Node.js support
* SQL Functions - JSON_VALUE, JSON_QUERY, CEILING, POWER, CHARACTER_LENGTH
* Support for Large Reads

### Bugs

* Fixed bug where InsertOrUpdate mutation was not checking `NOT NULL` columns.
