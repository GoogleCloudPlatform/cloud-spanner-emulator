//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "common/errors.h"

#include <string>

#include "zetasql/base/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "backend/common/ids.h"
#include "common/limits.h"
#include "zetasql/base/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace error {

// Generic errors.
zetasql_base::Status Internal(absl::string_view msg) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInternal, msg);
}

// Project errors.
zetasql_base::Status InvalidProjectURI(absl::string_view uri) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid project uri: ", uri));
}

// Instance config errors.
zetasql_base::Status InvalidInstanceConfigURI(absl::string_view uri) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid instance config uri: ", uri));
}

zetasql_base::Status InstanceConfigNotFound(absl::string_view config_id) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kNotFound,
      absl::StrCat("Instance config \"", config_id, "\" not found"));
}

// Instance errors.
zetasql_base::Status InvalidInstanceURI(absl::string_view uri) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid instance uri: ", uri));
}

zetasql_base::Status InstanceNotFound(absl::string_view uri) {
  return zetasql_base::Status(zetasql_base::StatusCode::kNotFound,
                      absl::StrCat("Instance not found: ", uri));
}

zetasql_base::Status InstanceAlreadyExists(absl::string_view uri) {
  return zetasql_base::Status(zetasql_base::StatusCode::kAlreadyExists,
                      absl::StrCat("Instance already exists: ", uri));
}

zetasql_base::Status InstanceNameMismatch(absl::string_view uri) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      absl::StrCat("Mismatching instance: ", uri));
}

zetasql_base::Status InstanceUpdatesNotSupported() {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kUnimplemented,
      "Cloud Spanner Emulator does not support updating instances.");
}

zetasql_base::Status InvalidInstanceName(absl::string_view instance_id) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::StrCat("Instance ID must start with a lowercase letter, "
                   "be 2-64 characters long, contain only lowercase "
                   "letters, numbers, or hyphens, and not end with a "
                   "hyphen. Got: ",
                   instance_id));
}

// Database errors.
zetasql_base::Status InvalidDatabaseURI(absl::string_view uri) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid database uri: ", uri));
}

zetasql_base::Status DatabaseNotFound(absl::string_view uri) {
  return zetasql_base::Status(zetasql_base::StatusCode::kNotFound,
                      absl::StrCat("Database not found: ", uri));
}

zetasql_base::Status DatabaseAlreadyExists(absl::string_view uri) {
  return zetasql_base::Status(zetasql_base::StatusCode::kAlreadyExists,
                      absl::StrCat("Database already exists: ", uri));
}

zetasql_base::Status CreateDatabaseMissingCreateStatement() {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      "Missing create_statement in the request.");
}

zetasql_base::Status InvalidCreateDatabaseStatement(absl::string_view statement) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::StrCat("Invalid \"CREATE DATABASE\" statement: ", statement));
}

zetasql_base::Status UpdateDatabaseMissingStatements() {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      "Missing statements in the request.");
}

zetasql_base::Status TooManyDatabasesPerInstance(absl::string_view instance_uri) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kResourceExhausted,
      absl::StrCat(
          "Too many databases for instance: ", instance_uri,
          ". See https://cloud.google.com/spanner/quotas#database_limits for "
          "more information."));
}

zetasql_base::Status InvalidDatabaseName(absl::string_view database_id) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::StrCat(
          "Invalid database name. Database name must start with a lowercase "
          "letter, be 2-30 characters long, contain only lowercase letters, "
          "numbers, underscores or hyphens, and not end with an underscore "
          "or hyphen. Got: ",
          database_id));
}

// Operation errors.
zetasql_base::Status InvalidOperationId(absl::string_view id) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      absl::Substitute("Invalid operation id: $0.", id));
}

zetasql_base::Status InvalidOperationURI(absl::string_view uri) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid operation uri: ", uri));
}

zetasql_base::Status OperationAlreadyExists(absl::string_view uri) {
  return zetasql_base::Status(zetasql_base::StatusCode::kAlreadyExists,
                      absl::StrCat("Operation already exists: ", uri));
}

zetasql_base::Status OperationNotFound(absl::string_view uri) {
  return zetasql_base::Status(zetasql_base::StatusCode::kNotFound,
                      absl::StrCat("Operation not found: ", uri));
}

// IAM errors.
zetasql_base::Status IAMPoliciesNotSupported() {
  return zetasql_base::Status(zetasql_base::StatusCode::kUnimplemented,
                      "Cloud Spanner Emulator does not support IAM policies.");
}

// Label errors.
zetasql_base::Status TooManyLabels(int num) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::StrCat("Too many labels: ", num, ". There can be a maximum of ",
                   limits::kMaxNumCloudLabels, " labels per resource."));
}

zetasql_base::Status BadLabelKey(absl::string_view key) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::StrCat("Label keys must be between 1-63 characters long, consist "
                   "only of lowercase letters, digits, underscores, and "
                   "dashes, and must start with a letter. Got: ",
                   key));
}

zetasql_base::Status BadLabelValue(absl::string_view key, absl::string_view value) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::StrCat("Non-empty label values must be between 1-63 characters "
                   "long, and consist only of lowercase letters, digits, "
                   "underscores, and dashes. Got: ",
                   key));
}

// Session errors.
zetasql_base::Status InvalidSessionURI(absl::string_view uri) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid session uri: ", uri));
}

zetasql_base::Status SessionNotFound(absl::string_view uri) {
  return zetasql_base::Status(zetasql_base::StatusCode::kNotFound,
                      absl::StrCat("Session not found: ", uri));
}

zetasql_base::Status TooFewSessions(int session_count) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Invalid number of sessions $0. "
                       "Requested sessions should be greater than 0.",
                       session_count));
}

// General proto formatting errors.
zetasql_base::Status InvalidProtoFormat(absl::string_view msg) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid proto formatting: ", msg));
}

zetasql_base::Status MissingRequiredFieldError(absl::string_view field) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::StrCat("No value provided for required field: ", field));
}

// Type proto errors.
zetasql_base::Status UnspecifiedType(absl::string_view proto) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::StrCat("Type code must be specified, found ", proto));
}

zetasql_base::Status ArrayTypeMustSpecifyElementType(absl::string_view proto) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::StrCat("Array types must specify element type, found ", proto));
}

// TODO: We should differentiate parameter type mismatch errors
// since they should return a kInvalidArgument error instead. Value proto
// errors.
zetasql_base::Status ValueProtoTypeMismatch(absl::string_view proto,
                                    absl::string_view expected_type) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::StrCat("Could not parse ", proto, " as ", expected_type));
}

zetasql_base::Status CouldNotParseStringAsInteger(absl::string_view str) {
  return zetasql_base::Status(zetasql_base::StatusCode::kFailedPrecondition,
                      absl::StrCat("Could not parse ", str, " as an integer"));
}

zetasql_base::Status CouldNotParseStringAsDouble(absl::string_view str) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::StrCat(
          "Could not parse ", str,
          " as a FLOAT64. Only the following string values are supported for "
          "FLOAT64: 'Infinity', 'NaN', and '-Infinity'. See "
          "https://cloud.google.com/spanner/docs/"
          "data-types#floating-point-type for more details"));
}

zetasql_base::Status CouldNotParseStringAsTimestamp(absl::string_view str,
                                            absl::string_view error) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::StrCat(
          "Could not parse ", str, " as a TIMESTAMP: ", error,
          ". For details on the TIMESTAMP type encoding, see "
          "https://cloud.google.com/spanner/docs/data-types#timestamp-type"));
}

zetasql_base::Status TimestampMustBeInUTCTimeZone(absl::string_view str) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::StrCat(
          "Could not parse ", str,
          " as a TIMESTAMP. The timestamp value must end with an uppercase "
          "literal 'Z' to specify Zulu time (UTC-0). For details on the "
          "TIMESTAMP type encoding, see "
          "https://cloud.google.com/spanner/docs/data-types#timestamp-type"));
}

zetasql_base::Status CouldNotParseStringAsDate(absl::string_view str) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::StrCat(
          "Could not parse ", str,
          " as a DATE. Dates must be in the format YYYY-[M]M-[D]D and the year "
          "must be in the range [1, 9999]. For more details on the DATE type "
          "encoding, see "
          "https://cloud.google.com/spanner/docs/data-types#date-type"));
}

zetasql_base::Status InvalidDate(absl::string_view str) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::StrCat(
          "Invalid date value: ", str,
          ". Dates must be in the format YYYY-[M]M-[D]D and the year must be "
          "in the range [1, 9999]. For more details on the DATE type encoding, "
          "see https://cloud.google.com/spanner/docs/data-types#date-type"));
}

zetasql_base::Status CouldNotParseStringAsBytes(absl::string_view str) {
  return zetasql_base::Status(zetasql_base::StatusCode::kFailedPrecondition,
                      absl::StrCat("Could not parse ", str,
                                   " as BYTES. Byte strings must be "
                                   "base64-encoded. For more details, see "
                                   "https://cloud.google.com/spanner/docs/"
                                   "reference/rpc/google.spanner.v1#typecode"));
}

zetasql_base::Status TimestampOutOfRange(absl::string_view time) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Timestamp out of range: $0. For details on the TIMESTAMP type "
          "encoding, see "
          "https://cloud.google.com/spanner/docs/data-types#timestamp-type",
          time));
}

zetasql_base::Status MultipleValuesForColumn(absl::string_view column) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Multiple values for column $0.", column));
}

// Key proto errors.
zetasql_base::Status WrongNumberOfKeyParts(absl::string_view table_or_index_name,
                                   int expected_key_parts, int found_key_parts,
                                   absl::string_view supplied_key) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::StrCat("Wrong number of key parts for ", table_or_index_name,
                   ". Expected ", expected_key_parts, " key parts, found ",
                   found_key_parts,
                   " key parts. Supplied key: ", supplied_key));
}

zetasql_base::Status KeyRangeMissingStart() {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      "Key range must contain either start_closed or start_open.");
}

zetasql_base::Status KeyRangeMissingEnd() {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      "Key range must contain either end_closed or end_open.");
}

// Mutation proto errors.
zetasql_base::Status BadDeleteRange(absl::string_view start_key,
                            absl::string_view limit_key) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kUnimplemented,
      absl::StrCat("For delete ranges, start and limit keys may only differ in "
                   "the final key part: start=",
                   start_key, ", limit=", limit_key));
}

zetasql_base::Status MutationTableRequired() {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      "Mutation does not specify table.");
}

// Transaction errors.
zetasql_base::Status AbortConcurrentTransaction(int64_t requestor_id, int64_t holder_id) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kAborted,
      absl::StrCat("Transaction ", requestor_id,
                   " aborted due to active transaction ", holder_id,
                   ". The emulator only supports one transaction at a time."));
}

zetasql_base::Status TransactionNotFound(backend::TransactionID id) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kNotFound,
      absl::StrCat("Transaction not found: ", std::to_string(id)));
}

zetasql_base::Status TransactionClosed(backend::TransactionID id) {
  return zetasql_base::Status(zetasql_base::StatusCode::kFailedPrecondition,
                      absl::StrCat("Transaction has already been closed: ",
                                   std::to_string(id)));
}

zetasql_base::Status InvalidTransactionID(backend::TransactionID id) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::StrCat("Invalid transaction ID. Transaction ID must be >= ",
                   std::to_string(id)));
}

zetasql_base::Status InvalidTransactionUsage(absl::string_view msg,
                                     backend::TransactionID id) {
  return zetasql_base::Status(zetasql_base::StatusCode::kFailedPrecondition,
                      absl::StrCat(msg, std::to_string(id)));
}

zetasql_base::Status InvalidTransactionType(absl::string_view msg) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      absl::StrCat("Bad Usage: ", msg));
}

zetasql_base::Status CannotReturnReadTimestampForReadWriteTransaction() {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInternal,
      "Cannot return read timestamp for a read-write transaction.");
}

zetasql_base::Status InvalidReadOptionForMultiUseTransaction(
    absl::string_view timestamp_bound) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::StrCat(
          "Timestamp bound: ", timestamp_bound,
          " for read can be only be used for a single use transaction."));
}

zetasql_base::Status InvalidModeForReadOnlySingleUseTransaction() {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      "Transaction mode for read only single use transaction "
                      "should be read_only.");
}

zetasql_base::Status DmlDoesNotSupportSingleUseTransaction() {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      "DML statements may not be performed in single-use "
                      "transactions, to avoid replay.");
}

zetasql_base::Status PartitionReadDoesNotSupportSingleUseTransaction() {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      "Partition reads may not be performed in single-use "
                      "transactions, since the response needs to be reused.");
}

zetasql_base::Status PartitionReadOnlySupportsReadOnlyTransaction() {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      "Partition reads can only be used to perform reads.");
}

zetasql_base::Status PartitionReadNeedsReadOnlyTxn() {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      "Partitioned reads can only be performed in a read-only transaction.");
}

zetasql_base::Status CannotCommitRollbackReadOnlyTransaction() {
  return zetasql_base::Status(zetasql_base::StatusCode::kFailedPrecondition,
                      "Cannot commit or rollback a read-only transaction.");
}

zetasql_base::Status CannotCommitAfterRollback() {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      "Cannot commit a transaction after it has been rolled back.");
}

zetasql_base::Status CannotRollbackAfterCommit() {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      "Cannot rollback a transaction after it has been committed.");
}

zetasql_base::Status CannotReadOrQueryAfterCommitOrRollback() {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      "Cannot read or query using a transaction after it has been "
      "committed or rolledback.");
}

zetasql_base::Status AbortDueToConcurrentSchemaChange(backend::TransactionID id) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kAborted,
      absl::StrCat("Transaction: ", id,
                   " aborted due to concurrent schema change."));
}

zetasql_base::Status ReadTimestampPastVersionGCLimit(absl::Time timestamp) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::StrCat("Read-only transaction timestamp ",
                   absl::FormatTime(timestamp),
                   " has exceeded the maximum timestamp staleness"));
}

// DDL errors.
zetasql_base::Status EmptyDDLStatement() {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      "DDL statement is empty.");
}

zetasql_base::Status DDLStatementWithErrors(absl::string_view ddl_string,
                                    const std::vector<std::string>& errors) {
  if (errors.empty()) return zetasql_base::OkStatus();
  if (errors.size() == 1) {
    return zetasql_base::Status(
        zetasql_base::StatusCode::kInvalidArgument,
        absl::StrCat("DDL statement ", ddl_string, " has error ", errors[0]));
  }
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::StrCat("DDL statement ", ddl_string, " has errors:\n-",
                   absl::StrJoin(errors, "\n-")));
}

// Schema errors.
zetasql_base::Status InvalidSchemaName(absl::string_view object_kind,
                               absl::string_view identifier,
                               absl::string_view reason) {
  std::string error =
      absl::Substitute("$0 name not valid: $1.", object_kind, identifier);
  if (!reason.empty()) {
    absl::StrAppend(&error, "Reason: ", reason);
  }
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument, error);
}

zetasql_base::Status CannotNameIndexPrimaryKey() {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      "Cannot use reserved name PRIMARY_KEY for a secondary index");
}

zetasql_base::Status CannotCreateIndexOnArrayColumns(absl::string_view index_name,
                                             absl::string_view column_name) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot reference ARRAY $1 in the creation of index $0.",
                       index_name, column_name));
}

zetasql_base::Status InvalidPrimaryKeyColumnType(absl::string_view column_name,
                                         absl::string_view type) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Column $0 has type $1, but is part of the primary key.",
                       column_name, type));
}

zetasql_base::Status InvalidColumnLength(absl::string_view column_name,
                                 int64_t specified_length, int64_t min_length,
                                 int64_t max_length) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Bad length for column $0: $1 : "
                       "Allowed length range: [$2, $3].",
                       column_name, specified_length, min_length, max_length));
}

zetasql_base::Status InvalidColumnSizeReduction(absl::string_view column_name,
                                        int64_t specified_length,
                                        int64_t existing_length,
                                        absl::string_view key) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("Reducing the length of column $0 to $1 is not allowed "
                       "because it has a value of length $2 at key: $3",
                       column_name, specified_length, existing_length, key));
}

zetasql_base::Status ColumnNotNull(absl::string_view column_name,
                           absl::string_view key) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("Adding a NOT NULL constraint on a column $0 "
                       "is not allowed because it has a NULL value at key: $1",
                       column_name, key));
}

zetasql_base::Status UnallowedCommitTimestampOption(absl::string_view column_name) {
  return zetasql_base::Status(zetasql_base::StatusCode::kFailedPrecondition,
                      absl::Substitute("Column $0 has invalid "
                                       "allow_commit_timestamp option.  Option "
                                       "only allowed on TIMESTAMP columns.",
                                       column_name));
}

zetasql_base::Status InvalidDropColumnWithDependency(absl::string_view column_name,
                                             absl::string_view table_name,
                                             absl::string_view index_name) {
  return zetasql_base::Status(zetasql_base::StatusCode::kFailedPrecondition,
                      absl::Substitute("Cannot drop column $0 from table $1 "
                                       "because it is used by index $2.",
                                       column_name, table_name, index_name));
}

zetasql_base::Status CannotChangeKeyColumn(absl::string_view column_name,
                                   absl::string_view reason) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot change key column $0.", column_name));
}

zetasql_base::Status InvalidDropKeyColumn(absl::string_view colum_name,
                                  absl::string_view table_name) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot drop key column $0 from table $1.", colum_name,
                       table_name));
}

zetasql_base::Status TooManyColumns(absl::string_view object_type,
                            absl::string_view object_name, int64_t limit) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 has too many columns; the limit is $2.",
                       object_type, object_name, limit));
}

zetasql_base::Status TooManyKeys(absl::string_view object_type,
                         absl::string_view object_name, int64_t key_count,
                         int64_t limit) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("$0 $1 has too many keys ($2); the limit is $3.",
                       object_type, object_name, key_count, limit));
}

zetasql_base::Status NoColumnsTable(absl::string_view object_type,
                            absl::string_view object_name) {
  return zetasql_base::Status(zetasql_base::StatusCode::kFailedPrecondition,
                      absl::Substitute("$0 $1 adds no columns and is not a top "
                                       "level table.",
                                       object_type, object_name));
}

zetasql_base::Status TooManyIndicesPerTable(absl::string_view index_name,
                                    absl::string_view table_name, int64_t limit) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot add index $0 to table $1: too many indices "
                       "(limit $2 per table).",
                       index_name, table_name, limit));
}

zetasql_base::Status DeepNesting(absl::string_view object_type,
                         absl::string_view object_name, int limit) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 is too deeply nested; the limit is $2.",
                       object_type, object_name, limit));
}

zetasql_base::Status DropTableWithInterleavedTables(absl::string_view table_name,
                                            absl::string_view child_tables) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot drop table $0 with interleaved tables: $1.",
                       table_name, child_tables));
}

zetasql_base::Status DropTableWithDependentIndices(absl::string_view table_name,
                                           absl::string_view indexes) {
  return zetasql_base::Status(zetasql_base::StatusCode::kFailedPrecondition,
                      absl::Substitute("Cannot drop table $0 with indices: $1.",
                                       table_name, indexes));
}

zetasql_base::Status SetOnDeleteWithoutInterleaving(absl::string_view table_name) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot SET ON DELETE on table $0 that does not have "
                       "an INTERLEAVE clause.",
                       table_name));
}

zetasql_base::Status NonExistentKeyColumn(absl::string_view object_type,
                                  absl::string_view object_name,
                                  absl::string_view key_column) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 references nonexistent key column $2.",
                       object_type, object_name, key_column));
}

zetasql_base::Status DuplicateColumnName(absl::string_view column_name) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("Duplicate column name $0.", column_name));
}

zetasql_base::Status MultipleRefsToKeyColumn(absl::string_view object_type,
                                     absl::string_view object_name,
                                     absl::string_view key_column) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("$0 $1 references key column $2 more than once.",
                       object_type, object_name, key_column));
}

zetasql_base::Status IncorrectParentKeyPosition(absl::string_view child_object_type,
                                        absl::string_view child_object_name,
                                        absl::string_view parent_key_column,
                                        int position) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 references parent key column $2 at incorrect "
                       "position $3.",
                       child_object_type, child_object_name, parent_key_column,
                       position));
}

zetasql_base::Status IncorrectParentKeyOrder(absl::string_view child_object_type,
                                     absl::string_view child_object_name,
                                     absl::string_view parent_key_column,
                                     absl::string_view child_key_order) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 references parent key column $2 with incorrect "
                       "order $3.",
                       child_object_type, child_object_name, parent_key_column,
                       child_key_order));
}

zetasql_base::Status MustReferenceParentKeyColumn(absl::string_view child_object_type,
                                          absl::string_view child_object_name,
                                          absl::string_view parent_key_column) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 does not reference $3parent key column $2.",
                       child_object_type, child_object_name, parent_key_column,
                       child_object_type == "Index" ? "index " : ""));
}

zetasql_base::Status IncorrectParentKeyType(absl::string_view child_object_type,
                                    absl::string_view child_object_name,
                                    absl::string_view parent_key_column,
                                    absl::string_view child_key_type,
                                    absl::string_view parent_key_type) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 references parent key column $2 with incorrect "
                       "type $3 (should be $4).",
                       child_object_type, child_object_name, parent_key_column,
                       child_key_type, parent_key_type));
}

zetasql_base::Status IncorrectParentKeyLength(absl::string_view child_object_type,
                                      absl::string_view child_object_name,
                                      absl::string_view parent_key_column,
                                      absl::string_view child_key_length,
                                      absl::string_view parent_key_length) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 references parent key column $2 with incorrect "
                       "length $3 (should be $4).",
                       child_object_type, child_object_name, parent_key_column,
                       child_key_length, parent_key_length));
}

zetasql_base::Status IncorrectParentKeyNullability(
    absl::string_view child_object_type, absl::string_view child_object_name,
    absl::string_view parent_key_column, absl::string_view parent_nullability,
    absl::string_view child_nullability) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 references parent key column "
                       "$2 that is $3, but child key is $4.",
                       child_object_type, child_object_name, parent_key_column,
                       parent_nullability, child_nullability));
}

zetasql_base::Status IndexWithNoKeys(absl::string_view index_name) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Index $0 does not specify any key columns.",
                       index_name));
}

zetasql_base::Status IndexRefsKeyAsStoredColumn(absl::string_view index_name,
                                        absl::string_view column_name) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Index $0 specifies stored column $1 already "
                       "specified as primary key.",
                       index_name, column_name));
}

zetasql_base::Status IndexRefsColumnTwice(absl::string_view index_name,
                                  absl::string_view key_column) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Index $0 specifies key column $1 twice.", index_name,
                       key_column));
}

zetasql_base::Status IndexRefsNonExistentColumn(absl::string_view index_name,
                                        absl::string_view column_name) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Index $0 specifies key column $1 which does not exist "
                       "in the index's base table.",
                       index_name, column_name));
}

zetasql_base::Status ConcurrentSchemaChangeOrReadWriteTxnInProgress() {
  return zetasql_base::Status(zetasql_base::StatusCode::kFailedPrecondition,
                      "Schema change operation rejected because a concurrent "
                      "schema change operation or read-write transaction is "
                      "already in progress.");
}

zetasql_base::Status IndexInterleaveTableNotFound(absl::string_view index_name,
                                          absl::string_view table_name) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot interleave index $0 within nonexistent "
                       "table $1.",
                       index_name, table_name));
}

zetasql_base::Status IndexRefsUnsupportedColumn(absl::string_view index_name,
                                        absl::string_view type) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Index $0 is defined on a column of unsupported type $1.", index_name,
          type));
}

zetasql_base::Status IndexInterleaveTableUnacceptable(absl::string_view index_name,
                                              absl::string_view indexed_table,
                                              absl::string_view parent_table) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot interleave index $0 of table $1 "
                       "within table $2 because $2 is not an ancestor of $1.",
                       index_name, indexed_table, parent_table));
}

zetasql_base::Status IndexRefsTableKeyAsStoredColumn(absl::string_view index_name,
                                             absl::string_view stored_column,
                                             absl::string_view base_table) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("Index $0 specifies stored column $1 which is a "
                       "key of table $2.",
                       index_name, stored_column, base_table));
}

zetasql_base::Status ChangingNullConstraintOnIndexedColumn(
    absl::string_view column_name, absl::string_view index_name) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("Changing NOT NULL constraints on column $0 is not "
                       "allowed because it affects index $1.",
                       column_name, index_name));
}

zetasql_base::Status TableNotFound(absl::string_view table_name) {
  return zetasql_base::Status(zetasql_base::StatusCode::kNotFound,
                      absl::StrCat("Table not found: ", table_name));
}

zetasql_base::Status TableNotFoundAtTimestamp(absl::string_view table_name,
                                      absl::Time timestamp) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kNotFound,
      absl::StrCat("Table '", table_name, "' is not found at timestamp: ",
                   absl::FormatTime(timestamp), "."));
}

zetasql_base::Status IndexNotFound(absl::string_view index_name) {
  return zetasql_base::Status(zetasql_base::StatusCode::kNotFound,
                      absl::Substitute("Index not found: $0", index_name));
}

zetasql_base::Status ColumnNotFound(absl::string_view table_name,
                            absl::string_view column_name) {
  return zetasql_base::Status(zetasql_base::StatusCode::kNotFound,
                      absl::StrCat("Column not found in table ", table_name,
                                   ": ", column_name));
}

zetasql_base::Status ColumnNotFoundAtTimestamp(absl::string_view table_name,
                                       absl::string_view column_name,
                                       absl::Time timestamp) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kNotFound,
      absl::StrCat("Table '", table_name, "' does not have Column ",
                   column_name, " at timestamp ", absl::FormatTime(timestamp),
                   "."));
}

zetasql_base::Status MutationColumnAndValueSizeMismatch(int columns_size,
                                                int values_size) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::StrCat("Mutation column and value size mismatch: Found: ",
                   columns_size, " columns and ", values_size, " values"));
}

zetasql_base::Status ColumnValueTypeMismatch(absl::string_view table_name,
                                     absl::string_view column_type,
                                     absl::string_view value_type) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::StrCat(
          "Column and value type mismatch: Found column_type: ", column_type,
          " and value_type ", value_type, " in table ", table_name));
}

zetasql_base::Status CannotParseKeyValue(absl::string_view table_name,
                                 absl::string_view column,
                                 absl::string_view column_type) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::StrCat("Could not parse the provided key value for key column ",
                   column, " which is of type ", column_type, " in table ",
                   table_name, "."));
}

zetasql_base::Status SchemaObjectAlreadyExists(absl::string_view schema_object,
                                       absl::string_view name) {
  return zetasql_base::Status(zetasql_base::StatusCode::kFailedPrecondition,
                      absl::Substitute("Duplicate name in schema: $0.", name));
}

zetasql_base::Status CannotChangeColumnType(absl::string_view column_name,
                                    absl::string_view old_type,
                                    absl::string_view new_type) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot change type of column `$0` from `$1` to `$2`",
                       column_name, old_type, new_type));
}

zetasql_base::Status AddingNotNullColumn(absl::string_view table_name,
                                 absl::string_view column_name) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kUnimplemented,
      absl::Substitute("Cannot add NOT NULL column $0.$1 to existing table $0.",
                       table_name, column_name));
}

// Commit timestamp errors.
zetasql_base::Status CommitTimestampInFuture(absl::Time timestamp) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::StrCat("Cannot write timestamps in the future, found: ",
                   absl::FormatTime(timestamp)));
}

zetasql_base::Status CommitTimestampNotInFuture(absl::string_view column,
                                        absl::string_view key,
                                        absl::Time timestamp) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Setting allow_commit_timestamp option on column $0 is not allowed "
          "because it has a timestamp in the future at key: $1 $2",
          column, key, absl::FormatTime(timestamp)));
}

zetasql_base::Status CannotReadPendingCommitTimestamp(absl::string_view table_name) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("$0 cannot be accessed because it, or its "
                       "associated index, has a pending CommitTimestamp",
                       table_name));
}

zetasql_base::Status PendingCommitTimestampAllOrNone(int64_t index_num) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Invalid use of PENDING_COMMIT_TIMESTAMP in column $0. All values "
          "for the column must be PENDING_COMMIT_TIMESTAMP() or none of them.",
          index_num));
}

// Time errors.
zetasql_base::Status InvalidTime(absl::string_view msg) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument, msg);
}

// Read argument errors.
zetasql_base::Status StalenessMustBeNonNegative() {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      "Staleness must be non-negative.");
}

zetasql_base::Status InvalidMinReadTimestamp(absl::Time min_read_timestamp) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid min read timestamp: ",
                                   absl::FormatTime(min_read_timestamp)));
}

zetasql_base::Status InvalidExactReadTimestamp(absl::Time exact_read_timestamp) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid exact read timestamp: ",
                                   absl::FormatTime(exact_read_timestamp)));
}

zetasql_base::Status StrongReadOptionShouldBeTrue() {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      "Strong read option must be true for a strong read.");
}

// Constraint errors.
zetasql_base::Status RowAlreadyExists(absl::string_view table_name,
                              absl::string_view key) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kAlreadyExists,
      absl::StrCat("Table ", table_name, ": Row ", key, " already exists."));
}

zetasql_base::Status RowNotFound(absl::string_view table_name, absl::string_view key) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kNotFound,
      absl::StrCat("Table ", table_name, ": Row ", key, " not found."));
}

zetasql_base::Status ParentKeyNotFound(absl::string_view parent_table_name,
                               absl::string_view child_table_name,
                               absl::string_view key) {
  return zetasql_base::Status(zetasql_base::StatusCode::kNotFound,
                      absl::StrCat("Insert failed because key was not found in "
                                   "parent table:  Parent Table: ",
                                   parent_table_name, "  Child Table: ",
                                   child_table_name, "  Key: ", key));
}

zetasql_base::Status ChildKeyExists(absl::string_view parent_table_name,
                            absl::string_view child_table_name,
                            absl::string_view key) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::StrCat("Delete failed because one or more keys exist within the "
                   "child table:  Parent Table: ",
                   parent_table_name, "  Child Table: ", child_table_name,
                   "  Key: ", key));
}

zetasql_base::Status NullValueForNotNullColumn(absl::string_view table_name,
                                       absl::string_view column_name) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot specify a null value for column: $0 in table: $1",
          column_name, table_name));
}

zetasql_base::Status NullValueForNotNullColumn(absl::string_view table_name,
                                       absl::string_view column_name,
                                       absl::string_view key) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot specify a null value for column: $0 in table: "
                       "$1 referenced by key: $2",
                       column_name, table_name, key));
}

zetasql_base::Status ValueExceedsLimit(absl::string_view column_name, int value_size,
                               int max_column_size) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("New value exceeds the maximum size limit for this "
                       "column: $0, size: $1, limit: $2.",
                       column_name, value_size, max_column_size));
}

zetasql_base::Status NonNullValueNotSpecifiedForInsert(absl::string_view table_name,
                                               absl::string_view column_name) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "A new row in table $0 does not specify a non-null value "
          "for NOT NULL column: $1",
          table_name, column_name));
}

zetasql_base::Status KeyTooLarge(absl::string_view table_name, int64_t key_size,
                         int64_t max_key_size) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Number of bytes for key of table $0 is $1 which "
                       "exceeds the maximum of $2.",
                       table_name, key_size, max_key_size));
}

zetasql_base::Status IndexKeyTooLarge(absl::string_view index_name, int64_t key_size,
                              int64_t max_key_size) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      absl::Substitute("Number of bytes for key of index $0 is "
                                       "$1 which exceeds the maximum of $2.",
                                       index_name, key_size, max_key_size));
}

zetasql_base::Status InvalidStringEncoding(absl::string_view table_name,
                                   absl::string_view column_name) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::StrCat("Invalid UTF-8 encoding for string value from column ",
                   column_name, " in table ", table_name, "."));
}

zetasql_base::Status UTF8StringColumn(absl::string_view column_name,
                              absl::string_view key) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::Substitute("Alteration of column $0 is not allowed because it has "
                       "an invalid UTF-8 character at key: $1",
                       column_name, key));
}

// Index errors.
zetasql_base::Status UniqueIndexConstraintViolation(absl::string_view index_name,
                                            absl::string_view key) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kAlreadyExists,
      absl::StrCat("UNIQUE violation on index ", index_name,
                   ",  duplicate key: ", key, " in this transaction."));
}

zetasql_base::Status UniqueIndexViolationOnIndexCreation(absl::string_view index_name,
                                                 absl::string_view key) {
  return zetasql_base::Status(zetasql_base::StatusCode::kFailedPrecondition,
                      absl::StrCat("Found uniqueness violation on index ",
                                   index_name, ",  duplicate key: ", key));
}

zetasql_base::Status IndexTableDoesNotMatchBaseTable(absl::string_view base_table,
                                             absl::string_view indexed_table,
                                             absl::string_view index) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kFailedPrecondition,
      absl::StrCat("Index '", index, "' indexes '", indexed_table,
                   "' which does not match the given base table '", base_table,
                   "'."));
}

zetasql_base::Status IndexNotFound(absl::string_view index, absl::string_view table) {
  return zetasql_base::Status(zetasql_base::StatusCode::kNotFound,
                      absl::StrCat("No index '", index,
                                   "' was found for table '", table, "'."));
}

zetasql_base::Status ColumnNotFoundInIndex(absl::string_view index,
                                   absl::string_view indexed_table,
                                   absl::string_view column) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kNotFound,
      absl::StrCat("Index '", index, "' which indexes '", indexed_table,
                   "' does not have a column named ", column,
                   ". Reads with an index can only query columns present in "
                   "the index (see "
                   "https://cloud.google.com/spanner/docs/"
                   "secondary-indexes#read-with-index)."));
}

// Query errors.
zetasql_base::Status UnimplementedUntypedParameterBinding(
    absl::string_view param_name) {
  return zetasql_base::Status(zetasql_base::StatusCode::kUnimplemented,
                      absl::Substitute("Untyped parameter binding: $0. Support "
                                       "for untyped parameter bindings is "
                                       "not yet implemented.",
                                       param_name));
}

zetasql_base::Status InvalidHint(absl::string_view hint_string) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      absl::Substitute("Unsupported hint: $0.", hint_string));
}

zetasql_base::Status InvalidHintValue(absl::string_view hint_string,
                              absl::string_view value_string) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      absl::Substitute("Invalid hint value for: $0 hint: $1.",
                                       hint_string, value_string));
}

zetasql_base::Status InvalidBatchDmlRequest() {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      "Request must contain at least one DML statement");
}

zetasql_base::Status ExecuteBatchDmlOnlySupportsDmlStatements(int index,
                                                      absl::string_view query) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Statement $0: '$1' is not valid DML.", index, query));
}

zetasql_base::Status DmlRequiresReadWriteTransaction(
    absl::string_view transaction_type) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("DML statements can only be performed in a read-write "
                       "transaction. Current transaction type is $0.",
                       transaction_type));
}

zetasql_base::Status EmulatorDoesNotSupportQueryPlans() {
  return zetasql_base::Status(zetasql_base::StatusCode::kUnimplemented,
                      "The emulator does not support the PLAN query mode.");
}

zetasql_base::Status InvalidStatementHintValue(absl::string_view hint_string,
                                       absl::string_view hint_value) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Invalid statement-level hint value: $0=$1.",
                       hint_string, hint_value));
}
zetasql_base::Status MultipleValuesForSameHint(absl::string_view hint_string) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Multiple values for hint: $0", hint_string));
}
zetasql_base::Status InvalidHintForNode(absl::string_view hint_string,
                                absl::string_view supported_node) {
  return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                      absl::Substitute("$0 hint is only supported for $1.",
                                       hint_string, supported_node));
}

// Partition Read errors.
zetasql_base::Status InvalidBytesPerBatch(absl::string_view message_name) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute("Invalid $0: bytes per batch must be greater than zero.",
                       message_name));
}

zetasql_base::Status InvalidMaxPartitionCount(absl::string_view message_name) {
  return zetasql_base::Status(
      zetasql_base::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Invalid $0: max partition count must be greater than zero.",
          message_name));
}

}  // namespace error
}  // namespace emulator
}  // namespace spanner
}  // namespace google
