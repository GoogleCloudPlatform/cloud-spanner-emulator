# Changes

## v1.4.5

### New Feature

* Add creation time to instance API
* Add update time to instance API
* Change type of COLUMN\_DEFAULT in INFORMATION\_SCHEMA from BYTES to STRING
* The Spanner emulator will enforce a maximum number of elements
in an IN list, to match that enforced by Spanner\.
* upgrade bazel and almost all of the external dependencies
* Add support for Default Primary Key Columns\.
* update to ubuntu 1804 and reuse existing ubuntu docker file for release



## v1.4.3

### New Features

*   Add support for INFORMATION\_SCHEMA\.TABLES ROW\_DELETION\_POLICY\_EXPRESSION column

*   Support ANALYZE parsing in emulator and ignore the schema change

*   Add INFORMATION\_SCHEMA\.DATABASE\_OPTIONS to spanner emulator\.

## v1.4.2

### New Features

*   Add support for INFORMATION_SCHEMA.TABLES ROW_DELETION_POLICY_EXPRESSION column.

### Bugs

*   Fixed compatibility issue due to reliance on libstdc++.so.6 version `GLIBCXX_3.4.26'.

## v1.4.1

### New Features

* Addded INFORMATION_SCHEMA.COLUMN_COLUMN_USAGE to spanner emulator.
* Addded syntax-only support for LOCK_SCANNED_RANGES hint to avoid syntax errors.
* Updated zetasql version to 2021.09.1.
* Added support for row deletion policy syntax. The syntax will be treated as no op.

### Bugs

* Fixed grpc metadata error: gRPC error (INTERNAL_ERROR) which happens when metadata is set.

## v1.4.0

### New Features

* Added support for named arguments.
* The emulator now supports JSON columns and functions (#37).
* Added support for INFORMATION_SCHEMA.TABLES TABLE_TYPE column (#43).
* Query size limits updated to match production (#52).

## v1.3.0

### New Features

* ALTER TABLE ADD [COLUMN] statement no longer requires COLUMN keyword (#16).
* Added support for check constraints (#11).
* Added support for `SELECT * EXCEPT` (#26).
* Added support for NUMERIC as a key/index in the emulator.
* Added numeric math functions.
* Added NET.* functions.

### Bugs

* Fixed crashes when evaluating DMLs on tables with generated columns (#23).
* Do not error when a query has a MERGE_JOIN hint.
* Return an error when a transaction updates a row already deleted in itself.
* Return an error for returning structs as columns.
* Return an error for `SELECT ARRAY<STRUCT<i INT64>>[]` expressions.

## v1.2.0

### New Features

* Added support for ARRAY_IS_DISTINCT and TABLESAMPLE functions.
* Added NUMERIC types.
* Added generated columns.
* Allow @{parameter_sensitive=always|auto|never} hint for ParameterSensitive plans.

### Bugs

* Fixed crash due to interaction between foreign keys and information schema (#10).
* Reject instance names with an underscore (#13).

## v1.1.1

### Bugs

* Fixed bug where direct index reads of commit timestamp values returned the max timestamp value.

## v1.1.0

### New Features

* Added support for 'EXTRACT_DATE|TIME' functions.
* Added DML sequence number support.
* Added random transaction abort flag (enable_fault_injection) for testing abort/retry logic.
* Increased CreateDatabase parallelism.
* Support for foreign keys.

### Bugs

* Fixed bug where set options disabled the NOT NULL constraint.
* Fixed bug where existing columns in a table could not be correctly altered.
* Fixed issue where BUILD file didn't work correctly on non-case sensitive OSes (macOS).

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
