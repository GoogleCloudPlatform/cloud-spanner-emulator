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

#include "google/rpc/error_details.pb.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "backend/common/ids.h"
#include "common/constants.h"
#include "common/limits.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace error {

// Generic errors.
absl::Status Internal(absl::string_view msg) {
  return absl::Status(absl::StatusCode::kInternal, msg);
}

absl::Status CycleDetected(absl::string_view object_type,
                           absl::string_view cycle) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Cycle detected while analysing $0, "
                                       "which include objects ($1)",
                                       object_type, cycle));
}

// Project errors.
absl::Status InvalidProjectURI(absl::string_view uri) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid project uri: ", uri));
}

// Instance config errors.
absl::Status InvalidInstanceConfigURI(absl::string_view uri) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid instance config uri: ", uri));
}

absl::Status InstanceConfigNotFound(absl::string_view config_id) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::StrCat("Instance config \"", config_id, "\" not found"));
}

// Instance errors.
absl::Status InvalidInstanceURI(absl::string_view uri) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid instance uri: ", uri));
}

absl::Status InstanceNotFound(absl::string_view uri) {
  absl::Status error(absl::StatusCode::kNotFound,
                     absl::StrCat("Instance not found: ", uri));

  google::rpc::ResourceInfo info;
  info.set_resource_type(kInstanceResourceType);
  std::string resource_name(uri);
  info.set_resource_name(resource_name);
  info.set_description("Instance does not exist.");
  absl::Cord serialized(info.SerializeAsString());
  error.SetPayload(kResourceInfoType, serialized);
  return error;
}

absl::Status InstanceAlreadyExists(absl::string_view uri) {
  return absl::Status(absl::StatusCode::kAlreadyExists,
                      absl::StrCat("Instance already exists: ", uri));
}

absl::Status InstanceNameMismatch(absl::string_view uri) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Mismatching instance: ", uri));
}

absl::Status InstanceUpdatesNotSupported() {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      "Cloud Spanner Emulator does not support updating instances.");
}

absl::Status InvalidInstanceName(absl::string_view instance_id) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat("Instance ID must start with a lowercase letter, "
                   "be 2-64 characters long, contain only lowercase "
                   "letters, numbers, or hyphens, and not end with a "
                   "hyphen. Got: ",
                   instance_id));
}

// Database errors.
absl::Status InvalidDatabaseURI(absl::string_view uri) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid database uri: ", uri));
}

absl::Status DatabaseNotFound(absl::string_view uri) {
  absl::Status error(absl::StatusCode::kNotFound,
                     absl::StrCat("Database not found: ", uri));

  google::rpc::ResourceInfo info;
  info.set_resource_type(kDatabaseResourceType);
  std::string resource_name(uri);
  info.set_resource_name(resource_name);
  info.set_description("Database does not exist.");
  absl::Cord serialized(info.SerializeAsString());
  error.SetPayload(kResourceInfoType, serialized);
  return error;
}

absl::Status DatabaseAlreadyExists(absl::string_view uri) {
  return absl::Status(absl::StatusCode::kAlreadyExists,
                      absl::StrCat("Database already exists: ", uri));
}

absl::Status CreateDatabaseMissingCreateStatement() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "Missing create_statement in the request.");
}

absl::Status InvalidCreateDatabaseStatement(absl::string_view statement) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat("Invalid \"CREATE DATABASE\" statement: ", statement));
}

absl::Status UpdateDatabaseMissingStatements() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "Missing statements in the request.");
}

absl::Status TooManyDatabasesPerInstance(absl::string_view instance_uri) {
  return absl::Status(
      absl::StatusCode::kResourceExhausted,
      absl::StrCat(
          "Too many databases for instance: ", instance_uri,
          ". See https://cloud.google.com/spanner/quotas#database_limits for "
          "more information."));
}

absl::Status InvalidDatabaseName(absl::string_view database_id) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat(
          "Invalid database name. Database name must start with a lowercase "
          "letter, be 2-30 characters long, contain only lowercase letters, "
          "numbers, underscores or hyphens, and not end with an underscore "
          "or hyphen. Got: ",
          database_id));
}

// Operation errors.
absl::Status InvalidOperationId(absl::string_view id) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("Invalid operation id: $0.", id));
}

absl::Status InvalidOperationURI(absl::string_view uri) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid operation uri: ", uri));
}

absl::Status OperationAlreadyExists(absl::string_view uri) {
  return absl::Status(absl::StatusCode::kAlreadyExists,
                      absl::StrCat("Operation already exists: ", uri));
}

absl::Status OperationNotFound(absl::string_view uri) {
  return absl::Status(absl::StatusCode::kNotFound,
                      absl::StrCat("Operation not found: ", uri));
}

// IAM errors.
absl::Status IAMPoliciesNotSupported() {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "Cloud Spanner Emulator does not support IAM policies.");
}

// Label errors.
absl::Status TooManyLabels(int num) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat("Too many labels: ", num, ". There can be a maximum of ",
                   limits::kMaxNumCloudLabels, " labels per resource."));
}

absl::Status BadLabelKey(absl::string_view key) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat("Label keys must be between 1-63 characters long, consist "
                   "only of lowercase letters, digits, underscores, and "
                   "dashes, and must start with a letter. Got: ",
                   key));
}

absl::Status BadLabelValue(absl::string_view key, absl::string_view value) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat("Non-empty label values must be between 1-63 characters "
                   "long, and consist only of lowercase letters, digits, "
                   "underscores, and dashes. Got: ",
                   key));
}

// Session errors.
absl::Status InvalidSessionURI(absl::string_view uri) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid session uri: ", uri));
}

absl::Status SessionNotFound(absl::string_view uri) {
  absl::Status error(absl::StatusCode::kNotFound,
                     absl::StrCat("Session not found: ", uri));

  google::rpc::ResourceInfo info;
  info.set_resource_type(kSessionResourceType);
  std::string resource_name(uri);
  info.set_resource_name(resource_name);
  info.set_description("Session does not exist.");
  absl::Cord serialized(info.SerializeAsString());
  error.SetPayload(kResourceInfoType, serialized);
  return error;
}

absl::Status TooFewSessions(int session_count) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Invalid number of sessions $0. "
                       "Requested sessions should be greater than 0.",
                       session_count));
}

// General proto formatting errors.
absl::Status InvalidProtoFormat(absl::string_view msg) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid proto formatting: ", msg));
}

absl::Status MissingRequiredFieldError(absl::string_view field) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat("No value provided for required field: ", field));
}

// Type proto errors.
absl::Status UnspecifiedType(absl::string_view proto) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat("Type code must be specified, found ", proto));
}

absl::Status ArrayTypeMustSpecifyElementType(absl::string_view proto) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat("Array types must specify element type, found ", proto));
}

// TODO: We should differentiate parameter type mismatch errors
// since they should return a kInvalidArgument error instead. Value proto
// errors.
absl::Status ValueProtoTypeMismatch(absl::string_view proto,
                                    absl::string_view expected_type) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat("Could not parse ", proto, " as ", expected_type));
}

absl::Status CouldNotParseStringAsInteger(absl::string_view str) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::StrCat("Could not parse ", str, " as an integer"));
}

absl::Status CouldNotParseStringAsDouble(absl::string_view str) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat(
          "Could not parse ", str,
          " as a FLOAT64. Only the following string values are supported for "
          "FLOAT64: 'Infinity', 'NaN', and '-Infinity'. See "
          "https://cloud.google.com/spanner/docs/"
          "data-types#floating-point-type for more details"));
}

absl::Status CouldNotParseStringAsNumeric(absl::string_view str) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat(
          "Could not parse ", str,
          " as a NUMERIC. The NUMERIC type supports 38 digits of precision and "
          "9 digits of scale. See "
          "https://cloud.google.com/spanner/docs/data-types for more details"));
}

absl::Status CouldNotParseStringAsJson(absl::string_view str) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat(
          "Could not parse ", str,
          " as a JSON. See https://cloud.google.com/spanner/docs/data-types "
          "for more details"));
}

absl::Status CouldNotParseStringAsTimestamp(absl::string_view str,
                                            absl::string_view error) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat(
          "Could not parse ", str, " as a TIMESTAMP: ", error,
          ". For details on the TIMESTAMP type encoding, see "
          "https://cloud.google.com/spanner/docs/data-types#timestamp-type"));
}

absl::Status TimestampMustBeInUTCTimeZone(absl::string_view str) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat(
          "Could not parse ", str,
          " as a TIMESTAMP. The timestamp value must end with an uppercase "
          "literal 'Z' to specify Zulu time (UTC-0). For details on the "
          "TIMESTAMP type encoding, see "
          "https://cloud.google.com/spanner/docs/data-types#timestamp-type"));
}

absl::Status CouldNotParseStringAsDate(absl::string_view str) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat(
          "Could not parse ", str,
          " as a DATE. Dates must be in the format YYYY-[M]M-[D]D and the year "
          "must be in the range [1, 9999]. For more details on the DATE type "
          "encoding, see "
          "https://cloud.google.com/spanner/docs/data-types#date-type"));
}

absl::Status InvalidDate(absl::string_view str) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat(
          "Invalid date value: ", str,
          ". Dates must be in the format YYYY-[M]M-[D]D and the year must be "
          "in the range [1, 9999]. For more details on the DATE type encoding, "
          "see https://cloud.google.com/spanner/docs/data-types#date-type"));
}

absl::Status CouldNotParseStringAsBytes(absl::string_view str) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::StrCat("Could not parse ", str,
                                   " as BYTES. Byte strings must be "
                                   "base64-encoded. For more details, see "
                                   "https://cloud.google.com/spanner/docs/"
                                   "reference/rpc/google.spanner.v1#typecode"));
}

absl::Status TimestampOutOfRange(absl::string_view time) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Timestamp out of range: $0. For details on the TIMESTAMP type "
          "encoding, see "
          "https://cloud.google.com/spanner/docs/data-types#timestamp-type",
          time));
}

absl::Status MultipleValuesForColumn(absl::string_view column) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Multiple values for column $0.", column));
}

// Key proto errors.
absl::Status WrongNumberOfKeyParts(absl::string_view table_or_index_name,
                                   int expected_key_parts, int found_key_parts,
                                   absl::string_view supplied_key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat("Wrong number of key parts for ", table_or_index_name,
                   ". Expected ", expected_key_parts, " key parts, found ",
                   found_key_parts,
                   " key parts. Supplied key: ", supplied_key));
}

absl::Status KeyRangeMissingStart() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "Key range must contain either start_closed or start_open.");
}

absl::Status KeyRangeMissingEnd() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "Key range must contain either end_closed or end_open.");
}

// Mutation proto errors.
absl::Status BadDeleteRange(absl::string_view start_key,
                            absl::string_view limit_key) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::StrCat("For delete ranges, start and limit keys may only differ in "
                   "the final key part: start=",
                   start_key, ", limit=", limit_key));
}

absl::Status MutationTableRequired() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "Mutation does not specify table.");
}

// Transaction errors.
absl::Status AbortConcurrentTransaction(int64_t requestor_id, int64_t holder_id) {
  return absl::Status(
      absl::StatusCode::kAborted,
      absl::StrCat("Transaction ", requestor_id,
                   " aborted due to active transaction ", holder_id,
                   ". The emulator only supports one transaction at a time."));
}

absl::Status TransactionNotFound(backend::TransactionID id) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::StrCat("Transaction not found: ", std::to_string(id)));
}

absl::Status TransactionClosed(backend::TransactionID id) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::StrCat("Transaction has already been closed: ",
                                   std::to_string(id)));
}

absl::Status InvalidTransactionID(backend::TransactionID id) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat("Invalid transaction ID. Transaction ID must be >= ",
                   std::to_string(id)));
}

absl::Status InvalidTransactionUsage(absl::string_view msg,
                                     backend::TransactionID id) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::StrCat(msg, std::to_string(id)));
}

absl::Status InvalidTransactionType(absl::string_view msg) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Bad Usage: ", msg));
}

absl::Status CannotReturnReadTimestampForReadWriteTransaction() {
  return absl::Status(
      absl::StatusCode::kInternal,
      "Cannot return read timestamp for a read-write transaction.");
}

absl::Status InvalidReadOptionForMultiUseTransaction(
    absl::string_view timestamp_bound) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat(
          "Timestamp bound: ", timestamp_bound,
          " for read can be only be used for a single use transaction."));
}

absl::Status InvalidModeForReadOnlySingleUseTransaction() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "Transaction mode for read only single use transaction "
                      "should be read_only.");
}

absl::Status DmlDoesNotSupportSingleUseTransaction() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "DML statements may not be performed in single-use "
                      "transactions, to avoid replay.");
}

absl::Status DmlSequenceOutOfOrder(int64_t request_seqno, int64_t last_seqno,
                                   absl::string_view sql_statement) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Request has an out-of-order seqno. Request seqno=$0. Last seen "
          "seqno=$1. The sequence number must be monotonically increasing "
          "within the transaction. If a request arrives for the first time "
          "with an out-of-order sequence number, the transaction will be "
          "invalidated.\nRequested SQL: $2",
          request_seqno, last_seqno, sql_statement));
}

absl::Status ReplayRequestMismatch(int64_t request_seqno,
                                   absl::string_view sql_statement) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Previously received a different "
                       "request with this seqno. seqno=$0\nRequested SQL: $1",
                       request_seqno, sql_statement));
}

absl::Status PartitionReadDoesNotSupportSingleUseTransaction() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "Partition reads may not be performed in single-use "
                      "transactions, since the response needs to be reused.");
}

absl::Status PartitionReadOnlySupportsReadOnlyTransaction() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "Partition reads can only be used to perform reads.");
}

absl::Status PartitionReadNeedsReadOnlyTxn() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "Partitioned reads can only be performed in a read-only transaction.");
}

absl::Status CannotCommitRollbackReadOnlyOrPartitionedDmlTransaction() {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      "Cannot commit or rollback a read-only or partitioned-dml transaction.");
}

absl::Status CannotReusePartitionedDmlTransaction() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "PartitionedDml transaction may only be used to execute one statement");
}

absl::Status PartitionedDMLOnlySupportsSimpleQuery() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "Partitioned DML only supports fully partitionable statements.");
}

absl::Status NoInsertForPartitionedDML() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "INSERT is not supported for Partitioned DML");
}

absl::Status InvalidOperationUsingPartitionedDmlTransaction() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "PartitionedDml Transactions may only be used to execute "
                      "DML statements");
}

absl::Status CannotCommitAfterRollback() {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      "Cannot commit a transaction after it has been rolled back.");
}

absl::Status CannotRollbackAfterCommit() {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      "Cannot rollback a transaction after it has been committed.");
}

absl::Status CannotReadOrQueryAfterCommitOrRollback() {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      "Cannot read or query using a transaction after it has been "
      "committed or rolledback.");
}

absl::Status CannotUseTransactionAfterConstraintError() {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      "Cannot use a transaction after it has been invalidated "
                      "due to a constraint error.");
}

absl::Status AbortDueToConcurrentSchemaChange(backend::TransactionID id) {
  return absl::Status(
      absl::StatusCode::kAborted,
      absl::StrCat("Transaction: ", id,
                   " aborted due to concurrent schema change."));
}

absl::Status AbortReadWriteTransactionOnFirstCommit(backend::TransactionID id) {
  return absl::Status(
      absl::StatusCode::kAborted,
      absl::StrCat(
          "Transaction: ", id,
          " was aborted in the emulator to mimic production behavior that can "
          "ABORT transactions due to a variety of reasons. All transactions "
          "should be running inside of retry loops, and should "
          "tolerate aborts (which will happen in production occasionally)."));
}

absl::Status UpdateDeletedRowInTransaction(absl::string_view table,
                                           absl::string_view key) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Row ", key, " in ", table,
                                   " was deleted and then updated in the same "
                                   "transaction."));
}

absl::Status ReadTimestampPastVersionGCLimit(absl::Time timestamp) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat("Read-only transaction timestamp ",
                   absl::FormatTime(timestamp),
                   " has exceeded the maximum timestamp staleness"));
}

absl::Status ReadTimestampTooFarInFuture(absl::Time timestamp) {
  return absl::Status(
      absl::StatusCode::kDeadlineExceeded,
      absl::StrCat(
          "Read-only transaction timestamp ", absl::FormatTime(timestamp),
          " is more than 1 hour in future. This request will execute for longer"
          " than the configured Cloud Spanner server deadline of 1 hour and"
          " will return with the DEADLINE_EXCEEDED error."));
}

// DDL errors.
absl::Status EmptyDDLStatement() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "DDL statement is empty.");
}

absl::Status DDLStatementWithErrors(absl::string_view ddl_string,
                                    const std::vector<std::string>& errors) {
  if (errors.empty()) return absl::OkStatus();
  // Avoid submits errors due to trailing spaces.
  std::string separator = ddl_string.at(0) == '\n' ? "" : " ";
  if (errors.size() == 1) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        absl::StrCat("Error parsing Spanner DDL statement:",
                                     separator, ddl_string, " : ", errors[0]));
  }
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Errors parsing Spanner DDL statement:",
                                   separator, ddl_string, " has errors:\n-",
                                   absl::StrJoin(errors, "\n-")));
}

absl::Status InvalidSchemaName(absl::string_view object_kind,
                               absl::string_view identifier) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("$0 name not valid: $1.", object_kind, identifier));
}

absl::Status InvalidConstraintName(absl::string_view constraint_type,
                                   absl::string_view constraint_name,
                                   absl::string_view reserved_prefix) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Invalid $0 name: $1. Prefix $2 cannot be used.",
                       constraint_type, constraint_name, reserved_prefix));
}

absl::Status CannotNameIndexPrimaryKey() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "Cannot use reserved name PRIMARY_KEY for a secondary index");
}

absl::Status CannotCreateIndexOnColumn(absl::string_view index_name,
                                       absl::string_view column_name,
                                       absl::string_view column_type) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot reference $2 $1 in the creation of index $0.",
                       index_name, column_name, column_type));
}

absl::Status InvalidPrimaryKeyColumnType(absl::string_view column_name,
                                         absl::string_view type) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Column $0 has type $1, but is part of the primary key.",
                       column_name, type));
}

absl::Status InvalidColumnLength(absl::string_view column_name,
                                 int64_t specified_length, int64_t min_length,
                                 int64_t max_length) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Bad length for column $0: $1 : "
                       "Allowed length range: [$2, $3].",
                       column_name, specified_length, min_length, max_length));
}

absl::Status InvalidColumnSizeReduction(absl::string_view column_name,
                                        int64_t specified_length,
                                        int64_t existing_length,
                                        absl::string_view key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Reducing the length of column $0 to $1 is not allowed "
                       "because it has a value of length $2 at key: $3",
                       column_name, specified_length, existing_length, key));
}

absl::Status ColumnNotNull(absl::string_view column_name,
                           absl::string_view key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Adding a NOT NULL constraint on a column $0 "
                       "is not allowed because it has a NULL value at key: $1",
                       column_name, key));
}

absl::Status UnallowedCommitTimestampOption(absl::string_view column_name) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Column $0 has invalid "
                                       "allow_commit_timestamp option.  Option "
                                       "only allowed on TIMESTAMP columns.",
                                       column_name));
}

absl::Status InvalidDropColumnWithDependency(absl::string_view column_name,
                                             absl::string_view table_name,
                                             absl::string_view index_name) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Cannot drop column $0 from table $1 "
                                       "because it is used by index $2.",
                                       column_name, table_name, index_name));
}

absl::Status CannotChangeKeyColumn(absl::string_view column_name,
                                   absl::string_view reason) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot change key column $0.", column_name));
}

absl::Status CannotChangeKeyColumnWithChildTables(
    absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Requested change to key column $0 could not be made.",
                       column_name));
}

absl::Status InvalidDropKeyColumn(absl::string_view colum_name,
                                  absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot drop key column $0 from table $1.", colum_name,
                       table_name));
}

absl::Status TooManyColumns(absl::string_view object_type,
                            absl::string_view object_name, int64_t limit) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 has too many columns; the limit is $2.",
                       object_type, object_name, limit));
}

absl::Status TooManyKeys(absl::string_view object_type,
                         absl::string_view object_name, int64_t key_count,
                         int64_t limit) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("$0 $1 has too many keys ($2); the limit is $3.",
                       object_type, object_name, key_count, limit));
}

absl::Status NoColumnsTable(absl::string_view object_type,
                            absl::string_view object_name) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("$0 $1 adds no columns and is not a top "
                                       "level table.",
                                       object_type, object_name));
}

absl::Status TooManyIndicesPerTable(absl::string_view index_name,
                                    absl::string_view table_name, int64_t limit) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot add index $0 to table $1: too many indices "
                       "(limit $2 per table).",
                       index_name, table_name, limit));
}

absl::Status TooManyTablesPerDatabase(absl::string_view table_name,
                                      int64_t limit) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Cannot add Table $0 : too many tables "
                                       "(limit $1 per database).",
                                       table_name, limit));
}

absl::Status TooManyIndicesPerDatabase(absl::string_view index_name,
                                       int64_t limit) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Cannot add Index $0 : too many indexes "
                                       "(limit $1 per database).",
                                       index_name, limit));
}

absl::Status DeepNesting(absl::string_view object_type,
                         absl::string_view object_name, int limit) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 is too deeply nested; the limit is $2.",
                       object_type, object_name, limit));
}

absl::Status DropTableWithInterleavedTables(absl::string_view table_name,
                                            absl::string_view child_tables) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot drop table $0 with interleaved tables: $1.",
                       table_name, child_tables));
}

absl::Status DropTableWithDependentIndices(absl::string_view table_name,
                                           absl::string_view indexes) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Cannot drop table $0 with indices: $1.",
                                       table_name, indexes));
}

absl::Status SetOnDeleteWithoutInterleaving(absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot SET ON DELETE on table $0 that does not have "
                       "an INTERLEAVE clause.",
                       table_name));
}

absl::Status NonExistentKeyColumn(absl::string_view object_type,
                                  absl::string_view object_name,
                                  absl::string_view key_column) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 references nonexistent key column $2.",
                       object_type, object_name, key_column));
}

absl::Status DuplicateColumnName(absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Duplicate column name $0.", column_name));
}

absl::Status MultipleRefsToKeyColumn(absl::string_view object_type,
                                     absl::string_view object_name,
                                     absl::string_view key_column) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("$0 $1 references key column $2 more than once.",
                       object_type, object_name, key_column));
}

absl::Status IncorrectParentKeyPosition(absl::string_view child_object_type,
                                        absl::string_view child_object_name,
                                        absl::string_view parent_key_column,
                                        int position) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 references parent key column $2 at incorrect "
                       "position $3.",
                       child_object_type, child_object_name, parent_key_column,
                       position));
}

absl::Status IncorrectParentKeyOrder(absl::string_view child_object_type,
                                     absl::string_view child_object_name,
                                     absl::string_view parent_key_column,
                                     absl::string_view child_key_order) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 references parent key column $2 with incorrect "
                       "order $3.",
                       child_object_type, child_object_name, parent_key_column,
                       child_key_order));
}

absl::Status MustReferenceParentKeyColumn(absl::string_view child_object_type,
                                          absl::string_view child_object_name,
                                          absl::string_view parent_key_column) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 does not reference $3parent key column $2.",
                       child_object_type, child_object_name, parent_key_column,
                       child_object_type == "Index" ? "index " : ""));
}

absl::Status IncorrectParentKeyType(absl::string_view child_object_type,
                                    absl::string_view child_object_name,
                                    absl::string_view parent_key_column,
                                    absl::string_view child_key_type,
                                    absl::string_view parent_key_type) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 references parent key column $2 with incorrect "
                       "type $3 (should be $4).",
                       child_object_type, child_object_name, parent_key_column,
                       child_key_type, parent_key_type));
}

absl::Status IncorrectParentKeyLength(absl::string_view child_object_type,
                                      absl::string_view child_object_name,
                                      absl::string_view parent_key_column,
                                      absl::string_view child_key_length,
                                      absl::string_view parent_key_length) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 references parent key column $2 with incorrect "
                       "length $3 (should be $4).",
                       child_object_type, child_object_name, parent_key_column,
                       child_key_length, parent_key_length));
}

absl::Status IncorrectParentKeyNullability(
    absl::string_view child_object_type, absl::string_view child_object_name,
    absl::string_view parent_key_column, absl::string_view parent_nullability,
    absl::string_view child_nullability) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("$0 $1 references parent key column "
                       "$2 that is $3, but child key is $4.",
                       child_object_type, child_object_name, parent_key_column,
                       parent_nullability, child_nullability));
}

absl::Status IndexWithNoKeys(absl::string_view index_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Index $0 does not specify any key columns.",
                       index_name));
}

absl::Status IndexRefsKeyAsStoredColumn(absl::string_view index_name,
                                        absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Index $0 specifies stored column $1 already "
                       "specified as primary key.",
                       index_name, column_name));
}

absl::Status IndexRefsColumnTwice(absl::string_view index_name,
                                  absl::string_view key_column) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Index $0 specifies key column $1 twice.", index_name,
                       key_column));
}

absl::Status IndexRefsNonExistentColumn(absl::string_view index_name,
                                        absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Index $0 specifies key column $1 which does not exist "
                       "in the index's base table.",
                       index_name, column_name));
}

absl::Status AlteringParentColumn(absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot alter parent key column $0.", column_name));
}

absl::Status ConcurrentSchemaChangeOrReadWriteTxnInProgress() {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      "Schema change operation rejected because a concurrent "
                      "schema change operation or read-write transaction is "
                      "already in progress.");
}

absl::Status IndexInterleaveTableNotFound(absl::string_view index_name,
                                          absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot interleave index $0 within nonexistent "
                       "table $1.",
                       index_name, table_name));
}

absl::Status IndexRefsUnsupportedColumn(absl::string_view index_name,
                                        absl::string_view type) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Index $0 is defined on a column of unsupported type $1.", index_name,
          type));
}

absl::Status IndexInterleaveTableUnacceptable(absl::string_view index_name,
                                              absl::string_view indexed_table,
                                              absl::string_view parent_table) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot interleave index $0 of table $1 "
                       "within table $2 because $2 is not an ancestor of $1.",
                       index_name, indexed_table, parent_table));
}

absl::Status IndexRefsTableKeyAsStoredColumn(absl::string_view index_name,
                                             absl::string_view stored_column,
                                             absl::string_view base_table) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Index $0 specifies stored column $1 which is a "
                       "key of table $2.",
                       index_name, stored_column, base_table));
}

absl::Status ChangingNullConstraintOnIndexedColumn(
    absl::string_view column_name, absl::string_view index_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Changing NOT NULL constraints on column $0 is not "
                       "allowed because it affects index $1.",
                       column_name, index_name));
}

absl::Status TableNotFound(absl::string_view table_name) {
  return absl::Status(absl::StatusCode::kNotFound,
                      absl::StrCat("Table not found: ", table_name));
}

absl::Status TableNotFoundAtTimestamp(absl::string_view table_name,
                                      absl::Time timestamp) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::StrCat("Table '", table_name, "' is not found at timestamp: ",
                   absl::FormatTime(timestamp), "."));
}

absl::Status IndexNotFound(absl::string_view index_name) {
  return absl::Status(absl::StatusCode::kNotFound,
                      absl::Substitute("Index not found: $0", index_name));
}

absl::Status DropForeignKeyManagedIndex(absl::string_view index_name,
                                        absl::string_view foreign_key_names) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot drop index `$0`. It is in use by foreign keys: "
                       "$1.",
                       index_name, foreign_key_names));
}

absl::Status ColumnNotFound(absl::string_view table_name,
                            absl::string_view column_name) {
  return absl::Status(absl::StatusCode::kNotFound,
                      absl::StrCat("Column not found in table ", table_name,
                                   ": ", column_name));
}

absl::Status ColumnNotFoundAtTimestamp(absl::string_view table_name,
                                       absl::string_view column_name,
                                       absl::Time timestamp) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::StrCat("Table '", table_name, "' does not have Column ",
                   column_name, " at timestamp ", absl::FormatTime(timestamp),
                   "."));
}

absl::Status MutationColumnAndValueSizeMismatch(int columns_size,
                                                int values_size) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat("Mutation column and value size mismatch: Found: ",
                   columns_size, " columns and ", values_size, " values"));
}

absl::Status ColumnValueTypeMismatch(absl::string_view table_name,
                                     absl::string_view column_type,
                                     absl::string_view value_type) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat(
          "Column and value type mismatch: Found column_type: ", column_type,
          " and value_type ", value_type, " in table ", table_name));
}

absl::Status CannotParseKeyValue(absl::string_view table_name,
                                 absl::string_view column,
                                 absl::string_view column_type) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat("Could not parse the provided key value for key column ",
                   column, " which is of type ", column_type, " in table ",
                   table_name, "."));
}

absl::Status SchemaObjectAlreadyExists(absl::string_view schema_object,
                                       absl::string_view name) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Duplicate name in schema: $0.", name));
}

absl::Status ConstraintNotFound(absl::string_view constraint_name,
                                absl::string_view table_name) {
  return absl::Status(absl::StatusCode::kNotFound,
                      absl::Substitute("$0 is not a constraint in $1",
                                       constraint_name, table_name));
}

absl::Status CannotChangeColumnType(absl::string_view column_name,
                                    absl::string_view old_type,
                                    absl::string_view new_type) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot change type of column `$0` from `$1` to `$2`",
                       column_name, old_type, new_type));
}

absl::Status AddingNotNullColumn(absl::string_view table_name,
                                 absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("Cannot add NOT NULL column $0.$1 to existing table $0.",
                       table_name, column_name));
}

// Commit timestamp errors.
absl::Status CommitTimestampInFuture(absl::Time timestamp) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat("Cannot write timestamps in the future, found: ",
                   absl::FormatTime(timestamp)));
}

absl::Status CommitTimestampNotInFuture(absl::string_view column,
                                        absl::string_view key,
                                        absl::Time timestamp) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Setting allow_commit_timestamp option on column $0 is not allowed "
          "because it has a timestamp in the future at key: $1 $2",
          column, key, absl::FormatTime(timestamp)));
}

absl::Status CannotReadPendingCommitTimestamp(absl::string_view entity_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("$0 cannot be accessed because it, or its "
                       "associated index, has a pending CommitTimestamp",
                       entity_string));
}

absl::Status PendingCommitTimestampAllOrNone(int64_t index_num) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Invalid use of PENDING_COMMIT_TIMESTAMP in column $0. All values "
          "for the column must be PENDING_COMMIT_TIMESTAMP() or none of them.",
          index_num));
}

absl::Status CommitTimestampOptionNotEnabled(absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot write commit timestamp because the allow_commit_timestamp "
          "column option is not set to true for column $0, or for all "
          "corresponding shared key columns in this table's interleaved table "
          "hierarchy.",
          column_name));
}

// Time errors.
absl::Status InvalidTime(absl::string_view msg) {
  return absl::Status(absl::StatusCode::kInvalidArgument, msg);
}

// Read argument errors.
absl::Status StalenessMustBeNonNegative() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "Staleness must be non-negative.");
}

absl::Status InvalidMinReadTimestamp(absl::Time min_read_timestamp) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid min read timestamp: ",
                                   absl::FormatTime(min_read_timestamp)));
}

absl::Status InvalidExactReadTimestamp(absl::Time exact_read_timestamp) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Invalid exact read timestamp: ",
                                   absl::FormatTime(exact_read_timestamp)));
}

absl::Status StrongReadOptionShouldBeTrue() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "Strong read option must be true for a strong read.");
}

absl::Status InvalidReadLimit() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "Limit must be non-negative.");
}

absl::Status InvalidReadLimitWithPartitionToken() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "A limit cannot be used when a partition_token is specified.");
}

// Constraint errors.
absl::Status RowAlreadyExists(absl::string_view table_name,
                              absl::string_view key) {
  return absl::Status(
      absl::StatusCode::kAlreadyExists,
      absl::StrCat("Table ", table_name, ": Row ", key, " already exists."));
}

absl::Status RowNotFound(absl::string_view table_name, absl::string_view key) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::StrCat("Table ", table_name, ": Row ", key, " not found."));
}

absl::Status ParentKeyNotFound(absl::string_view parent_table_name,
                               absl::string_view child_table_name,
                               absl::string_view key) {
  return absl::Status(absl::StatusCode::kNotFound,
                      absl::StrCat("Insert failed because key was not found in "
                                   "parent table:  Parent Table: ",
                                   parent_table_name, "  Child Table: ",
                                   child_table_name, "  Key: ", key));
}

absl::Status ChildKeyExists(absl::string_view parent_table_name,
                            absl::string_view child_table_name,
                            absl::string_view key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat("Delete failed because one or more keys exist within the "
                   "child table:  Parent Table: ",
                   parent_table_name, "  Child Table: ", child_table_name,
                   "  Key: ", key));
}

absl::Status NullValueForNotNullColumn(absl::string_view table_name,
                                       absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot specify a null value for column: $0 in table: $1",
          column_name, table_name));
}

absl::Status NullValueForNotNullColumn(absl::string_view table_name,
                                       absl::string_view column_name,
                                       absl::string_view key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot specify a null value for column: $0 in table: "
                       "$1 referenced by key: $2",
                       column_name, table_name, key));
}

absl::Status ValueExceedsLimit(absl::string_view column_name, int value_size,
                               int max_column_size) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("New value exceeds the maximum size limit for this "
                       "column: $0, size: $1, limit: $2.",
                       column_name, value_size, max_column_size));
}

absl::Status NonNullValueNotSpecifiedForInsert(absl::string_view table_name,
                                               absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "A new row in table $0 does not specify a non-null value "
          "for NOT NULL column: $1",
          table_name, column_name));
}

absl::Status KeyTooLarge(absl::string_view table_name, int64_t key_size,
                         int64_t max_key_size) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Number of bytes for key of table $0 is $1 which "
                       "exceeds the maximum of $2.",
                       table_name, key_size, max_key_size));
}

absl::Status IndexKeyTooLarge(absl::string_view index_name, int64_t key_size,
                              int64_t max_key_size) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("Number of bytes for key of index $0 is "
                                       "$1 which exceeds the maximum of $2.",
                                       index_name, key_size, max_key_size));
}

absl::Status InvalidStringEncoding(absl::string_view table_name,
                                   absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat("Invalid UTF-8 encoding for string value from column ",
                   column_name, " in table ", table_name, "."));
}

absl::Status UTF8StringColumn(absl::string_view column_name,
                              absl::string_view key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Alteration of column $0 is not allowed because it has "
                       "an invalid UTF-8 character at key: $1",
                       column_name, key));
}

// Index errors.
absl::Status UniqueIndexConstraintViolation(absl::string_view index_name,
                                            absl::string_view key) {
  return absl::Status(
      absl::StatusCode::kAlreadyExists,
      absl::StrCat("UNIQUE violation on index ", index_name,
                   ",  duplicate key: ", key, " in this transaction."));
}

absl::Status UniqueIndexViolationOnIndexCreation(absl::string_view index_name,
                                                 absl::string_view key) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::StrCat("Found uniqueness violation on index ",
                                   index_name, ",  duplicate key: ", key));
}

absl::Status IndexTableDoesNotMatchBaseTable(absl::string_view base_table,
                                             absl::string_view indexed_table,
                                             absl::string_view index) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat("Index '", index, "' indexes '", indexed_table,
                   "' which does not match the given base table '", base_table,
                   "'."));
}

absl::Status IndexNotFound(absl::string_view index, absl::string_view table) {
  return absl::Status(absl::StatusCode::kNotFound,
                      absl::StrCat("No index '", index,
                                   "' was found for table '", table, "'."));
}

absl::Status ColumnNotFoundInIndex(absl::string_view index,
                                   absl::string_view indexed_table,
                                   absl::string_view column) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::StrCat("Index '", index, "' which indexes '", indexed_table,
                   "' does not have a column named ", column,
                   ". Reads with an index can only query columns present in "
                   "the index (see "
                   "https://cloud.google.com/spanner/docs/"
                   "secondary-indexes#read-with-index)."));
}

// Foreign key errors.
absl::Status ForeignKeyColumnsRequired(absl::string_view table,
                                       absl::string_view foreign_key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "At least one column is required for table `$0` in foreign key `$1`.",
          foreign_key, table));
}

absl::Status ForeignKeyColumnCountMismatch(absl::string_view referencing_table,
                                           absl::string_view referenced_table,
                                           absl::string_view foreign_key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("The number of columns are different for table `$0` and "
                       "table `$1` in foreign key `$2`.",
                       referencing_table, referenced_table, foreign_key));
}

absl::Status ForeignKeyDuplicateColumn(absl::string_view column,
                                       absl::string_view table,
                                       absl::string_view foreign_key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Column `$0` used more than once for table `$1` in foreign key `$2`.",
          column, table, foreign_key));
}

absl::Status ForeignKeyColumnNotFound(absl::string_view column,
                                      absl::string_view table,
                                      absl::string_view foreign_key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Column `$0` not found for table `$1` in foreign key `$2`.", column,
          table, foreign_key));
}

absl::Status ForeignKeyColumnTypeUnsupported(absl::string_view column,
                                             absl::string_view table,
                                             absl::string_view foreign_key) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Column `$0` for foreign key `$1` on "
                                       "table `$2` has an unsupported type.",
                                       column, foreign_key, table));
}

absl::Status ForeignKeyCommitTimestampColumnUnsupported(
    absl::string_view column, absl::string_view table,
    absl::string_view foreign_key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Commit timestamp column is not supported for column "
                       "`$0` of table `$1` in foreign key `$2`.",
                       column, table, foreign_key));
}

absl::Status ForeignKeyColumnTypeMismatch(absl::string_view referencing_column,
                                          absl::string_view referencing_table,
                                          absl::string_view referenced_column,
                                          absl::string_view referenced_table,
                                          absl::string_view foreign_key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "The column types are different for column `$0` of table `$1` and "
          "column `$2` of table `$3` in foreign key `$4`.",
          referencing_column, referencing_table, referenced_column,
          referenced_table, foreign_key));
}

absl::Status ForeignKeyReferencedTableDropNotAllowed(
    absl::string_view table, absl::string_view foreign_keys) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot drop table `$0`. It is referenced by one or more foreign "
          "keys: $1. You must drop the foreign keys before dropping the table.",
          table, foreign_keys));
}

absl::Status ForeignKeyColumnDropNotAllowed(absl::string_view column,
                                            absl::string_view table,
                                            absl::string_view foreign_keys) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot drop column `$0` from table `$1`. It is used by "
                       "one or more foreign keys: $2. You must drop the "
                       "foreign keys before dropping the column.",
                       column, table, foreign_keys));
}

absl::Status ForeignKeyColumnNullabilityChangeNotAllowed(
    absl::string_view column, absl::string_view table,
    absl::string_view foreign_keys) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot change the nullability for column `$0` of table "
                       "`$1`. It is used by one or more foreign keys: $2.",
                       column, table, foreign_keys));
}

absl::Status ForeignKeyColumnTypeChangeNotAllowed(
    absl::string_view column, absl::string_view table,
    absl::string_view foreign_keys) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot change the type for column `$0` of table `$1`. "
                       "It is used by one or more foreign keys: $2.",
                       column, table, foreign_keys));
}

absl::Status ForeignKeyColumnSetCommitTimestampOptionNotAllowed(
    absl::string_view column, absl::string_view table,
    absl::string_view foreign_key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot set the commit_timestamp option for column `$0` of table "
          "`$1`. It is used by one or more foreign keys: $2.",
          column, table, foreign_key));
}

absl::Status ForeignKeyReferencedKeyNotFound(
    absl::string_view foreign_key, absl::string_view referencing_table,
    absl::string_view referenced_table, absl::string_view referenced_key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Foreign key `$0` constraint violation on table `$1`. "
                       "Cannot find referenced key `$3` in table `$2`.",
                       foreign_key, referencing_table, referenced_table,
                       referenced_key));
}

absl::Status ForeignKeyReferencingKeyFound(absl::string_view foreign_key,
                                           absl::string_view referencing_table,
                                           absl::string_view referenced_table,
                                           absl::string_view referencing_key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Foreign key `$0` constraint violation when deleting or updating "
          "referenced key `$3` in table `$2`. A row with that key exists in "
          "the referencing table `$1`.",
          foreign_key, referencing_table, referenced_table, referencing_key));
}

absl::Status CheckConstraintNotEnabled() {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "Check Constraint is not implemented.");
}

absl::Status CheckConstraintViolated(absl::string_view check_constraint_name,
                                     absl::string_view table_name,
                                     absl::string_view key_debug_string) {
  return absl::Status(
      absl::StatusCode::kOutOfRange,
      absl::Substitute("Check constraint `$0`.`$1` is violated for key $2",
                       table_name, check_constraint_name, key_debug_string));
}

absl::Status CheckConstraintNotUsingAnyNonGeneratedColumn(
    absl::string_view table_name, absl::string_view check_constraint_name,
    absl::string_view expression) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Check constraint `$0`.`$1` does not use any "
          "non generated column. Expression: '$2'. A check "
          "constraint expression requires at least one non-generated column "
          "as dependency.",
          table_name, check_constraint_name, expression));
}

absl::Status NumericTypeNotEnabled() {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "NUMERIC type is not implemented.");
}

absl::Status JsonTypeNotEnabled() {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "JSON type is not implemented.");
}

// Check constraint errors.
absl::Status CheckConstraintExpressionParseError(
    absl::string_view table_name, absl::string_view check_constraint_expression,
    absl::string_view check_constraint_name, absl::string_view message) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Error parsing expression '$0' from check constraint "
                       "'$1' in table '$2': $3",
                       check_constraint_expression, check_constraint_name,
                       table_name, message));
}

absl::Status CannotUseCommitTimestampColumnOnCheckConstraint(
    absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Column `$0` has option commit_timestamp, which is not "
                       "supported in check constraint.",
                       column_name));
}

absl::Status InvalidDropColumnReferencedByCheckConstraint(
    absl::string_view table_name, absl::string_view check_constraint_name,
    absl::string_view referencing_column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot drop column `$0` from table `$1` because it is "
                       "referenced by check constraint `$2`.",
                       referencing_column_name, table_name,
                       check_constraint_name));
}

absl::Status CannotAlterColumnDataTypeWithDependentCheckConstraint(
    absl::string_view column_name, absl::string_view check_constraint_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot change the data type of column `$0`, which is "
                       "used by check constraint `$1`.",
                       column_name, check_constraint_name));
}

// Generated column errors.
absl::Status GeneratedColumnsNotEnabled() {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "Generated columns are not enabled.");
}

absl::Status NonStoredGeneratedColumnUnsupported(
    absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("Generated column `$0` without the STORED "
                       "attribute is not supported.",
                       column_name));
}

absl::Status GeneratedColumnDefinitionParseError(absl::string_view table_name,
                                                 absl::string_view column_name,
                                                 absl::string_view message) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Error parsing the definition of generated column `$0`.`$1`: $2",
          table_name, column_name, message));
}

absl::Status NonScalarExpressionInColumnExpression(absl::string_view type) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot use non-scalar expressions inside $0.", type));
}

absl::Status ColumnExpressionMaxDepthExceeded(int depth, int max_depth) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Expression depth of $0 exceeds the maximum allowed depth of $1.",
          depth, max_depth));
}

absl::Status InvalidDropColumnReferencedByGeneratedColumn(
    absl::string_view column_name, absl::string_view table_name,
    absl::string_view referencing_column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot drop column `$0` from table `$1` "
                       "because it is referenced by generated column `$2`.",
                       column_name, table_name, referencing_column_name));
}

absl::Status CannotConvertGeneratedColumnToRegularColumn(
    absl::string_view table_name, absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Cannot convert generated column `$0.$1` to a regular column.",
          table_name, column_name));
}

absl::Status CannotConvertRegularColumnToGeneratedColumn(
    absl::string_view table_name, absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot convert column `$0.$1` to a generated column.",
                       table_name, column_name));
}

absl::Status CannotAlterStoredGeneratedColumnDataType(
    absl::string_view table_name, absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Cannot change the data type of a stored generated column `$0.$1`.",
          table_name, column_name));
}

absl::Status CannotAlterGeneratedColumnExpression(
    absl::string_view table_name, absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Cannot change the expression of a generated column `$0.$1` because "
          "it is stored or has other dependencies.",
          table_name, column_name));
}

absl::Status CannotAlterColumnDataTypeWithDependentStoredGeneratedColumn(
    absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot change the data type of column `$0`, which "
                       "has a dependent stored generated column.",
                       column_name));
}

absl::Status CannotUseCommitTimestampOnGeneratedColumnDependency(
    absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot use commit timestamp column `$0` as a "
                       "dependency of a materialized generated column.",
                       column_name));
}

absl::Status CannotUseGeneratedColumnInPrimaryKey(
    absl::string_view table_name, absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Generated column `$0.$1` cannot be part of the "
                       "primary key.",
                       table_name, column_name));
}

absl::Status CannotWriteToGeneratedColumn(absl::string_view table_name,
                                          absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot write into generated column `$0.$1`.",
                       table_name, column_name));
}

absl::Status NonDeterministicFunctionInColumnExpression(
    absl::string_view function_name, absl::string_view expression_use) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Expression is non-deterministic due to the use of non-determinstic "
          "function `$0`. Expression of $1 must yield "
          "the same value for the same dependent column values. "
          "Non-deterministic functions inside the expressions are not allowed.",
          function_name, expression_use));
}

// Query errors.
absl::Status UnableToInferUndeclaredParameter(
    absl::string_view parameter_name) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("Unable to infer type for parameter $0.",
                                       parameter_name));
}
absl::Status InvalidHint(absl::string_view hint_string) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("Unsupported hint: $0.", hint_string));
}

absl::Status InvalidEmulatorHint(absl::string_view hint_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Invalid emulator-only hint: $0.", hint_string));
}

absl::Status InvalidHintValue(absl::string_view hint_string,
                              absl::string_view value_string) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("Invalid hint value for: $0 hint: $1.",
                                       hint_string, value_string));
}

absl::Status InvalidEmulatorHintValue(absl::string_view hint_string,
                                      absl::string_view value_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Invalid hint value for emulator-only hint $0: $1.",
                       hint_string, value_string));
}

absl::Status QueryHintIndexNotFound(absl::string_view table_name,
                                    absl::string_view index_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("The table $0 does not have a secondary index "
                       "called $1",
                       table_name, index_name));
}

absl::Status QueryHintManagedIndexNotSupported(absl::string_view index_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Cannot use managed index $0 in query hints. The name of this index "
          "was generated in the emulator. Only names generated in production "
          "are permitted.",
          index_name));
}

absl::Status NullFilteredIndexUnusable(absl::string_view index_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "The emulator is not able to determine whether the "
          "null filtered index $0 can be used to answer this query as it "
          "may filter out nulls that may be required to answer the query. "
          "Please test this query against Cloud Spanner. If you confirm "
          "against Cloud Spanner that the null filtered index can be used to "
          "answer the query, set the hint @{spanner_emulator."
          "disable_query_null_filtered_index_check=true} on the table "
          "to bypass this check in the emulator. This hint will be ignored by "
          "the production Cloud Spanner service and the emulator will accept "
          "the query and return a valid result when it is run with the check "
          "disabled.",
          index_name));
}

absl::Status NonPartitionableQuery(absl::string_view reason) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat(
          "The emulator is not able to determine whether this query is "
          "partitionable. Please test this query against Cloud Spanner to "
          "confirm it is partitionable. If you confirm it is partitionable, "
          "set the hint @{spanner_emulator."
          "disable_query_partitionability_check=false} on the query statement "
          "to bypass this check in the emulator. This hint will be ignored by "
          "the production Cloud Spanner service and the emulator will accept "
          "the query and return a valid result when it is run with the check "
          "disabled. ",
          "Possible reason why the query may not be partitionable: ", reason));
}

absl::Status InvalidBatchDmlRequest() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "Request must contain at least one DML statement");
}

absl::Status BatchDmlOnlySupportsReadWriteTransaction() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "ExecuteBatchDml is only supported within a read-write transaction.");
}

absl::Status ExecuteBatchDmlOnlySupportsDmlStatements(int index,
                                                      absl::string_view query) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Statement $0: '$1' is not valid DML.", index, query));
}

absl::Status ReadOnlyTransactionDoesNotSupportDml(
    absl::string_view transaction_type) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "DML statements can only be performed in a read-write or "
          "partitioned-dml transaction. Current transaction type is $0.",
          transaction_type));
}

// Unsupported query shape errors.
absl::Status UnsupportedReturnStructAsColumn() {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      "Unsupported query shape: A struct value cannot be returned as a column "
      "value. Rewrite the query to flatten the struct fields in the result.");
}

absl::Status UnsupportedArrayConstructorSyntaxForEmptyStructArray() {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      "Unsupported query shape: Spanner does not support array constructor "
      "syntax for an empty array where array elements are Structs.");
}

absl::Status UnsupportedFeatureSafe(absl::string_view feature_type,
                                    absl::string_view info_message) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("Unsupported $0: $1", feature_type, info_message));
}

absl::Status UnsupportedFunction(absl::string_view function_name) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("Unsupported built-in function: $0", function_name));
}

absl::Status UnsupportedHavingModifierWithDistinct() {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      "Aggregates with DISTINCT are not supported with a HAVING modifier.");
}

absl::Status UnsupportedIgnoreNullsInAggregateFunctions() {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      "IGNORE NULLS and RESPECT NULLS in functions other than ARRAY_AGG are "
      "not supported");
}

absl::Status NullifStructNotSupported() {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "NULLIF is not supported for arguments of STRUCT type.");
}

absl::Status ComparisonNotSupported(int arg_num,
                                    absl::string_view function_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Argument $0 does not support comparison in function "
                       "$1",
                       arg_num, function_name));
}

absl::Status StructComparisonNotSupported(absl::string_view function_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Comparison between structs are not supported in "
                       "function $0",
                       function_name));
}

absl::Status PendingCommitTimestampDmlValueOnly() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "The PENDING_COMMIT_TIMESTAMP() function may only be used as a "
      "value for INSERT or UPDATE of an appropriately typed column. It "
      "cannot be used in SELECT, or as the input to any other scalar "
      "expression.");
}

absl::Status NoFeatureSupportDifferentTypeArrayCasts(
    absl::string_view from_type, absl::string_view to_type) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Casting between arrays with incompatible element types "
                       "is not supported: Invalid cast from $0 to $1.",
                       from_type, to_type));
}

absl::Status UnsupportedTablesampleRepeatable() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "REPEATABLE is not supported");
}

absl::Status UnsupportedTablesampleSystem() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "SYSTEM sampling is not supported");
}

absl::Status TooManyFunctions(int max_function_nodes) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Number of functions exceeds the maximum allowed limit of $0.",
          max_function_nodes));
}

absl::Status TooManyNestedBooleanPredicates(int max_nested_function_nodes) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Number of nested boolean predicates exceeds the "
                       "maximum allowed limit of $0.",
                       max_nested_function_nodes));
}

absl::Status TooManyJoins(int max_joins) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Number of joins exceeds the maximum allowed limit of $0.",
          max_joins));
}

absl::Status TooManyNestedSubqueries(int max_nested_subquery_expressions) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("Number of nested subqueries exceeds "
                                       "the maximum allowed limit of $0.",
                                       max_nested_subquery_expressions));
}

absl::Status TooManyNestedSubselects(int max_nested_subselects) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("Number of nested subselects exceeds "
                                       "the maximum allowed limit of $0.",
                                       max_nested_subselects));
}

absl::Status TooManyNestedAggregates(int max_nested_group_by) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("Number of nested aggregations exceeds "
                                       "the maximum allowed limit of $0.",
                                       max_nested_group_by));
}

absl::Status TooManyParameters(int max_parameters) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("Number of parameters in query exceeds "
                                       "the maximum allowed limit of $0.",
                                       max_parameters));
}

absl::Status TooManyAggregates(int max_columns_in_group_by) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Number of grouping columns exceeds the maximum allowed limit of $0.",
          max_columns_in_group_by));
}

absl::Status TooManyUnions(int max_unions_in_query) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Number of UNION operations exceeds the maximum allowed limit of $0.",
          max_unions_in_query));
}

absl::Status TooManySubqueryChildren(int max_subquery_expression_children) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Number of child subqueries exceeds the maximum allowed limit of $0.",
          max_subquery_expression_children));
}

absl::Status TooManyStructFields(int max_struct_fields) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Number of struct fields exceeds the maximum allowed limit of $0.",
          max_struct_fields));
}

absl::Status TooManyNestedStructs(int max_nested_struct_depth) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("Number of nested struct types exceeds "
                                       "the maximum allowed limit of $0.",
                                       max_nested_struct_depth));
}

absl::Status QueryStringTooLong(int query_length, int max_length) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("Query string length of $0 exceeds "
                                       "maximum allowed length of $1",
                                       query_length, max_length));
}

absl::Status EmulatorDoesNotSupportQueryPlans() {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "The emulator does not support the PLAN query mode.");
}

absl::Status InvalidStatementHintValue(absl::string_view hint_string,
                                       absl::string_view hint_value) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Invalid statement-level hint value: $0=$1.",
                       hint_string, hint_value));
}
absl::Status MultipleValuesForSameHint(absl::string_view hint_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Multiple values for hint: $0", hint_string));
}
absl::Status InvalidHintForNode(absl::string_view hint_string,
                                absl::string_view supported_node) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("$0 hint is only supported for $1.",
                                       hint_string, supported_node));
}

// Partition Read errors.
absl::Status InvalidBytesPerBatch(absl::string_view message_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Invalid $0: bytes per batch must be greater than zero.",
                       message_name));
}

absl::Status InvalidMaxPartitionCount(absl::string_view message_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Invalid $0: max partition count must be greater than zero.",
          message_name));
}

absl::Status InvalidPartitionToken() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "Invalid partition token.");
}

absl::Status ReadFromDifferentSession() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "Partitioned request was created for a different session.");
}

absl::Status ReadFromDifferentTransaction() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "Partitioned request was created for a different transaction.");
}

absl::Status ReadFromDifferentParameters() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "Partitioned request was created for different read or sql parameters.");
}

absl::Status InvalidPartitionedQueryMode() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "Query modes returning query plan and profile information cannot be used "
      "with partitioned queries.");
}

}  // namespace error
}  // namespace emulator
}  // namespace spanner
}  // namespace google
