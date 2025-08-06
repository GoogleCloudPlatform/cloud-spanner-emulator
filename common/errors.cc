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

#include <cstdint>
#include <string>
#include <vector>

#include "google/rpc/error_details.pb.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "common/constants.h"
#include "common/limits.h"

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

absl::Status InvalidCreateInstanceRequestUnitsNotBoth() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "Invalid CreateInstance request. Only one of nodes or "
                      "processing units should be specified.");
}

absl::Status InvalidCreateInstanceRequestUnitsMultiple() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "Invalid CreateInstance request. Processing units should be "
      "multiple of 100 for values below 1000 and multiples of "
      "1000 for values above 1000.");
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

absl::Status CannotCreatePostgreSQLDialectDatabase() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "Cannot create a PostgreSQL database.");
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

absl::Status InvalidOperationSessionDelete() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "Multiplexed sessions may not be deleted.");
}

absl::Status InvalidOperationBatchCreateSessions() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "Multiplexed sessions may not be created in batch.");
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

absl::Status CouldNotParseStringAsPgOid(absl::string_view str) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::StrCat("Could not parse ", str, " as PG.OID "));
}

absl::Status CouldNotParseStringAsFloat(absl::string_view str) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat(
          "Could not parse ", str,
          " as a FLOAT32. Only the following string values are supported for "
          "FLOAT32: 'Infinity', 'NaN', and '-Infinity'. See "
          "https://cloud.google.com/spanner/docs/"
          "data-types#floating-point-type for more details"));
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

absl::Status CouldNotParseStringAsPgNumeric(absl::string_view str) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat("Could not parse ", str, " as a PG.NUMERIC. See ",
                   "https://cloud.google.com/spanner/docs/reference/postgresql/"
                   "data-types for more details"));
}

absl::Status CouldNotParseStringAsJson(absl::string_view str) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat(
          "Could not parse ", str,
          " as a JSON. See https://cloud.google.com/spanner/docs/data-types "
          "for more details"));
}

absl::Status CouldNotParseStringAsPgJsonb(absl::string_view str) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::StrCat("Could not parse ", str, " as a JSONB. See ",
                                   "https://cloud.google.com/spanner/docs/"
                                   "reference/postgresql/data-types "
                                   "for more details"));
}

absl::Status CouldNotParseStringAsTimestamp(absl::string_view str,
                                            absl::string_view error) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat(
          "Could not parse ", str, " as a TIMESTAMP: ", error,
          ". For details on the TIMESTAMP type encoding, see "
          "https://cloud.google.com/spanner/docs/data-types#timestamp_type"));
}

absl::Status CouldNotParseStringAsInterval(absl::string_view str,
                                           absl::string_view error) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat(
          "Could not parse ", str, " as a INTERVAL: ", error,
          ". For details on the INTERVAL type , see "
          "https://cloud.google.com/spanner/docs/data-types#interval_type"));
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
absl::Status AbortConcurrentTransaction(int64_t requestor_id,
                                        int64_t holder_id) {
  return absl::Status(
      absl::StatusCode::kAborted,
      absl::StrCat("Transaction ", requestor_id,
                   " aborted due to active transaction ", holder_id,
                   ". The emulator only supports one transaction at a time."));
}

absl::Status AbortCurrentTransaction(backend::TransactionID holder_id,
                                     backend::TransactionID requestor_id) {
  return absl::Status(
      absl::StatusCode::kAborted,
      absl::StrCat("Transaction: ", holder_id, " aborted due to transaction ",
                   requestor_id,
                   " getting priority. "
                   "The emulator only supports one transaction at a time."));
}

absl::Status WoundedTransaction(backend::TransactionID id) {
  return absl::Status(
      absl::StatusCode::kAborted,
      absl::StrCat("Transaction: ", id,
                   " aborted due to another transaction getting priority. "
                   "The emulator only supports one transaction at a time."));
}

absl::Status CouldNotObtainLockHandleMutex(backend::TransactionID id) {
  return absl::Status(
      absl::StatusCode::kInternal,
      absl::StrCat("Could not obtain lock handle mutex for transaction: ", id));
}

absl::Status CouldNotObtainTransactionMutex(backend::TransactionID id) {
  return absl::Status(
      absl::StatusCode::kInternal,
      absl::StrCat("Could not obtain transaction mutex for transaction: ", id));
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

absl::Status DirectedReadNeedsReadOnlyTxn() {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      "Directed reads can only be performed in a read-only transaction.");
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

absl::Status ForeignKeyReferencedRestrictionInTransaction(
    absl::string_view table, absl::string_view key) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::StrCat("Cannot write and delete the row with key ",
                                   key, " in the foreign key referenced table ",
                                   table, " within the same transaction."));
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

absl::Status SchemaObjectTypeUnsupportedInNamedSchema(
    absl::string_view object_kind, absl::string_view identifier) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("$0 not yet supported in named schema: $1.", object_kind,
                       identifier));
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

absl::Status VectorLengthExceedsLimit(absl::string_view column_name,
                                      int element_num, int limit) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("Array column $0 has $1 elements and "
                                       "exceeds the `vector_length` limit: $2.",
                                       column_name, element_num, limit));
}

absl::Status VectorLengthLessThanLimit(absl::string_view column_name,
                                       int element_num, int limit) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Array column $0 has $1 elements and "
                       "is less than the `vector_length` limit: $2.",
                       column_name, element_num, limit));
}

absl::Status DisallowNullsInSearchArray(absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("The array column $0 has `vector_length`, and Null is "
                       "not allowed..",
                       column_name));
}

absl::Status InvalidTypeForVectorLength(absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("`vector_length` can only be applied on ARRAY<FLOAT32>"
                       "or ARRAY<FLOAT64>, but it is applied on $0.",
                       column_name));
}

absl::Status VectorLengthOnGeneratedOrDefaultColumn(
    absl::string_view column_name) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("`vector_length` cannot be applied on "
                                       "generated column or default column,"
                                       "but it is applied on $0.",
                                       column_name));
}

absl::Status CannotAlterColumnToAddVectorLength(absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot change column $0 to add `vector_length`",
                       column_name));
}

absl::Status CannotAlterColumnToRemoveVectorLength(
    absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot change column $0 to remove `vector_length`",
                       column_name));
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
                                    absl::string_view table_name,
                                    int64_t limit) {
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

absl::Status CreateChangeStreamForClauseInvalidOneof(
    absl::string_view change_stream_name) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Invalid CreateChangeStreamForClause: "
                                       "Change Stream $0 is missing"
                                       " `all` or `tracked_tables` in oneof "
                                       "for_clause_type.",
                                       change_stream_name));
}

absl::Status CreateChangeStreamForClauseZeroEntriesInTrackedTables(
    absl::string_view change_stream_name) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Invalid CreateChangeStreamForClause "
                                       "tracked_tables: Change Stream $0 has"
                                       " zero entries in `tracked_tables`.",
                                       change_stream_name));
}

absl::Status CreateChangeStreamForClauseTrackedTablesEntryMissingTableName(
    absl::string_view change_stream_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Invalid CreateChangeStreamForClause::TrackedTables::Entry: "
          "Change Stream $0 is missing `table_name`"
          " in CreateChangeStreamForClause::TrackedTables::Entry.",
          change_stream_name));
}

absl::Status ChangeStreamDuplicateTable(absl::string_view change_stream_name,
                                        absl::string_view table_name) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Change Stream $0 should not list Table "
                                       "$1 more than once in the FOR clause.",
                                       change_stream_name, table_name));
}

absl::Status InvalidTrackedObjectInChangeStream(
    absl::string_view change_stream_name, absl::string_view object_type,
    absl::string_view object_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Change Stream $0 does "
                       "not support tracking $1 $2. Change "
                       "Streams support tracking Tables and "
                       "Columns in the default schema (not in "
                       "INFORMATION_SCHEMA or SPANNER_SYS)."
                       " Change Streams, Indexes, Queues, "
                       "Views/Functions and non-key Generated "
                       "Columns that are not STORED VOLATILE are"
                       " not supported for tracking.",
                       change_stream_name, object_type, object_name));
}

absl::Status UnsupportedTrackedObjectOrNonExistentTableInChangeStream(
    absl::string_view change_stream_name, absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Change Stream $0 cannot track $1 because either Change "
                       "Streams do not track database objects of this type, or "
                       "this Table does not exist.",
                       change_stream_name, table_name));
}

absl::Status UnsupportedProcedure(absl::string_view procedure_string) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::Substitute("$0 is not supported.", procedure_string));
}

absl::Status CreateChangeStreamForClauseTrackedTablesEntryInvalidOneof(
    absl::string_view change_stream_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Invalid CreateChangeStreamForClause::TrackedTables::Entry: Change "
          "Stream $0 is missing `all` or `tracked_columns` in "
          "CreateChangeStreamForClause::TrackedTables::Entry oneof track_type.",
          change_stream_name));
}

absl::Status ChangeStreamDuplicateColumn(absl::string_view change_stream_name,
                                         absl::string_view column_name,
                                         absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Change Stream $0 should not list Column $1 "
                       "of Table $2 more than once in the FOR clause.",
                       change_stream_name, column_name, table_name));
}

absl::Status NonexistentTrackedColumnInChangeStream(
    absl::string_view change_stream_name, absl::string_view column_name,
    absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Change Stream $0 cannot track column "
                       "$1 because it does not exist in Table $2.",
                       change_stream_name, column_name, table_name));
}

absl::Status KeyColumnInChangeStreamForClause(
    absl::string_view change_stream_name, absl::string_view key_column_name,
    absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("The FOR clause of Change Stream $0 should not list "
                       "the primary key column $1 of Table $2. Please list "
                       "non-key columns only, because primary key columns "
                       "are always tracked if the table is tracked.",
                       change_stream_name, key_column_name, table_name));
}

absl::Status TooManyChangeStreamsPerDatabase(
    absl::string_view change_stream_name, int64_t limit) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Cannot add Change Stream $0 : "
                                       "because the maximum number "
                                       "of Change Streams per Database "
                                       "(limit $1) has been reached.",
                                       change_stream_name, limit));
}

absl::Status TooManyChangeStreamsTrackingSameObject(
    absl::string_view change_stream_name, int64_t limit,
    absl::string_view object_name_string) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Failed to create or alter Change Stream $0 : "
                       "because it is not allowed to have more than "
                       "$1 Change Streams tracking the same table or "
                       "non-key column or ALL: $2.",
                       change_stream_name, limit, object_name_string));
}

absl::Status TooManyModelsPerDatabase(absl::string_view model_name,
                                      int64_t limit) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Cannot add Model $0 : "
                                       "because the maximum number "
                                       "of <Models> per Database "
                                       "(limit $1) has been reached.",
                                       model_name, limit));
}

absl::Status TooManyPropertyGraphsPerDatabase(absl::string_view graph_name,
                                              int64_t limit) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Cannot add Property Graph $0 : "
                                       "because the maximum number "
                                       "of <Property Graphs> per Database "
                                       "(limit $1) has been reached.",
                                       graph_name, limit));
}

absl::Status PropertyGraphNotFound(absl::string_view property_graph_name) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::StrCat("Property Graph `", property_graph_name, "` not found."));
}

absl::Status PropertyGraphDuplicateLabel(absl::string_view property_graph_name,
                                         std::string_view label_name) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Property Graph `", property_graph_name,
                                   "` has duplicate label name: ", label_name));
}

absl::Status PropertyGraphDuplicatePropertyDeclaration(
    absl::string_view property_graph_name,
    absl::string_view property_declaration_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat("Property Graph `", property_graph_name,
                   "` has duplicate property declaration name: ",
                   property_declaration_name));
}

absl::Status GraphElementTableLabelNotFound(
    absl::string_view property_graph_name, absl::string_view element_table_name,
    absl::string_view label_name) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::StrCat("Graph element table `", element_table_name, "` in ",
                   "property graph `", property_graph_name, "` does not have ",
                   "label `", label_name, "`."));
}

absl::Status GraphElementTablePropertyDefinitionNotFound(
    absl::string_view property_graph_name, absl::string_view element_table_name,
    absl::string_view property_definition_name) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::StrCat("Graph element table `", element_table_name, "` in ",
                   "property graph `", property_graph_name, "` does not have ",
                   "property definition `", property_definition_name, "`."));
}

absl::Status GraphEdgeTableSourceNodeTableNotFound(
    absl::string_view property_graph_name, absl::string_view edge_table_name,
    absl::string_view node_table_name) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::StrCat("Graph edge table `", edge_table_name, "` in ",
                   "property graph `", property_graph_name, "` does not have ",
                   "source node table `", node_table_name, "`."));
}

absl::Status GraphEdgeTableDestinationNodeTableNotFound(
    absl::string_view property_graph_name, absl::string_view edge_table_name,
    absl::string_view node_table_name) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::StrCat("Graph edge table `", edge_table_name, "` in ",
                   "property graph `", property_graph_name, "` does not have ",
                   "destination node table `", node_table_name, "`."));
}

absl::Status UnsupportedChangeStreamOption(absl::string_view option_name) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Invalid Change Stream Option: $0."
                                       "Supported options are retention_period "
                                       "and value_capture_type.",
                                       option_name));
}

absl::Status InvalidChangeStreamRetentionPeriodOptionValue() {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Option retention_period can only take string_value."));
}

absl::Status InvalidTimeDurationFormat(absl::string_view time_duration) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Invalid time duration format: $0.", time_duration));
}

absl::Status InvalidDataRetentionPeriod(absl::string_view time_duration) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Invalid retention_period: $0. Change Streams retention "
                       "period must be a value in the range [24h, 7d].",
                       time_duration));
}

absl::Status InvalidValueCaptureType(absl::string_view value_capture_type) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Invalid value_capture_type: $0. Change Streams only "
                       "support value capture types in OLD_AND_NEW_VALUES, "
                       "NEW_ROW, and NEW_VALUES.",
                       value_capture_type));
}

absl::Status AlterChangeStreamDropNonexistentForClause(
    absl::string_view change_stream_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "The ALTER CHANGE STREAM DROP FOR clause cannot be "
          "applied on Change Stream $0, because it does not have a FOR clause "
          "and it does not track any database object.",
          change_stream_name));
}

absl::Status TrackUntrackableTables(absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("ChangeStream cannot track internal or owned tables $0",
                       table_name));
}

absl::Status TrackUntrackableColumns(absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("ChangeStream cannot track generated columns $0",
                       column_name));
}

absl::Status UnsetTrackedObject(absl::string_view change_stream_name,
                                absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "ChangeStream $0 table entry $1 column track type not set",
          change_stream_name, table_name));
}

absl::Status InvalidChangeStreamTvfArgumentNullStartTimestamp() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "start_timestamp must not be null.");
}

absl::Status InvalidChangeStreamTvfArgumentStartTimestampTooFarInFuture(
    absl::string_view min_read_ts_string, absl::string_view max_read_ts_string,
    absl::string_view start_ts_string) {
  return absl::Status(
      absl::StatusCode::kOutOfRange,
      absl::Substitute(
          "Specified start_timestamp is too far in the future. Please specify "
          "a start_timestamp within the earliest read timestamp: $0, and the "
          "current maximum start timestamp: "
          "$1. Received start_timestamp: $2.",
          min_read_ts_string, max_read_ts_string, start_ts_string));
}

absl::Status InvalidChangeStreamTvfArgumentStartTimestampTooOld(
    absl::string_view min_read_ts_string, absl::string_view start_ts_string) {
  return absl::Status(
      absl::StatusCode::kOutOfRange,
      absl::Substitute(
          "Specified start_timestamp is too far in the past. Please specify "
          "a start_timestamp within the earliest read timestamp: $0. Received "
          "start_timestamp: $1.",
          min_read_ts_string, start_ts_string));
}

absl::Status
InvalidChangeStreamTvfArgumentStartTimestampGreaterThanEndTimestamp(
    absl::string_view start_ts_string, absl::string_view end_ts_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("If end_timestamp is specified, start_timestamp must be "
                       "<= end_timestamp. Received start_timestamp: $0, "
                       "end_timestamp: $1.",
                       start_ts_string, end_ts_string));
}

absl::Status InvalidChangeStreamTvfArgumentNullHeartbeat() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "heartbeat_milliseconds must not be null.");
}

absl::Status InvalidChangeStreamTvfArgumentOutOfRangeHeartbeat(
    int64_t min_heartbeat_num, int64_t max_heartbeat_num,
    int64_t heartbeat_num) {
  return absl::Status(
      absl::StatusCode::kOutOfRange,
      absl::Substitute("heartbeat_milliseconds must be within min $0 and "
                       "max $1. Received heartbeat_milliseconds: $2.",
                       min_heartbeat_num, max_heartbeat_num, heartbeat_num));
}

absl::Status InvalidChangeStreamTvfArgumentNonNullReadOptions() {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "read_options must be null.");
}

absl::Status InvalidChangeStreamTvfArgumentWithArgIndex(
    absl::string_view tvf_name_string, int index_num) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Invalid argument of change stream TVF $0. "
          "Argument $1 is not a literal or parameter. Implicit type "
          "conversions are not supported.",
          tvf_name_string, index_num));
}

absl::Status
InvalidChangeStreamTvfArgumentPartitionTokenInvalidChangeStreamName(
    absl::string_view partition_token_str) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Specified partition_token is invalid, as it does not belong to the "
          "Change Stream associated with the TVF. Received partition_token: "
          "$0.",
          partition_token_str));
}

absl::Status InvalidChangeStreamTvfArgumentStartTimestampForPartition(
    absl::string_view min_ts_string, absl::string_view max_ts_string,
    absl::string_view start_ts_string) {
  return absl::Status(
      absl::StatusCode::kOutOfRange,
      absl::Substitute(
          "Specified start_timestamp is invalid for the partition. Please "
          "specify a start_timestamp within $0 and $1. "
          "Received start_timestamp: $2.",
          min_ts_string, max_ts_string, start_ts_string));
}

absl::Status ChangeStreamStalePartition() {
  return absl::Status(
      absl::StatusCode::kOutOfRange,
      "This partition is no longer valid. All Change Stream records associated "
      "with this partition have expired and have been deleted, as specified by "
      "the Change Stream retention period.");
}

absl::Status IllegalChangeStreamQuerySyntax(absl::string_view tvf_name_string) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("Change stream TVF must not be combined with any other "
                       "SQL statements. "
                       "Change stream records should be read using "
                       "`SELECT ChangeRecord FROM $0(<args>)`.",
                       tvf_name_string));
}

absl::Status IllegalChangeStreamQueryPGSyntax(
    absl::string_view tvf_name_string) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("Change stream TVF must not be combined with any other "
                       "SQL statements. "
                       "Change stream records should be read using "
                       "`select * from \"spanner\".\"$0\"(<args>)`.",
                       tvf_name_string));
}

absl::Status ChangeStreamQueriesMustBeSingleUseOnly() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "Change stream queries are not supported for multi use transactions. "
      "Change stream queries must be strong reads executed via single use "
      "transactions using the ExecuteStreamingSql API.");
}
absl::Status ChangeStreamQueriesMustBeStrongReads() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "Change stream queries are not supported for multi use transactions. "
      "Change stream queries must be strong reads executed via single use "
      "transactions using the ExecuteStreamingSql API.");
}
absl::Status ChangeStreamQueriesMustBeStreaming() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "Change stream queries are not supported for the ExecuteSql API. "
      "Change stream queries must be strong reads executed via single use "
      "transactions using the ExecuteStreamingSql API.");
}

absl::Status LocalityGroupNotFound(absl::string_view locality_group_name) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::StrCat("Locality group not found: ", locality_group_name));
}

absl::Status DroppingLocalityGroupWithAssignedTableColumn(
    absl::string_view locality_group_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Locality group $0 cannot be dropped as it is referenced.",
          locality_group_name));
}

absl::Status CreatingDefaultLocalityGroup() {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      "Default locality group cannot be created.");
}

absl::Status DroppingDefaultLocalityGroup() {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      "Default locality group cannot be dropped.");
}

absl::Status InvalidLocalityGroupName(absl::string_view locality_group_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Invalid locality group name: $0. The locality group "
                       "name must not start with `_`.",
                       locality_group_name));
}

absl::Status AlterLocalityGroupWithoutOptions() {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      "Alter Locality Group must have options.");
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

absl::Status DropTableWithDependentChangeStreams(
    absl::string_view table_name, absl::string_view change_streams) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot drop table $0 with change_streams: $1.",
                       table_name, change_streams));
}

absl::Status InterleaveInNotSupported() {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "INTERLEAVE IN is not supported.");
}

absl::Status ChangeInterleavingNotAllowed(absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot change interleaving behavior of table $0.",
                       table_name));
}

absl::Status ChangeInterleavingTableNotAllowed(absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot change the interleaving table of table $0.",
                       table_name));
}

absl::Status SetOnDeleteWithoutInterleaving(absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot SET ON DELETE on table $0 that does not have "
                       "an INTERLEAVE clause.",
                       table_name));
}

absl::Status SetOnDeleteOnInterleaveInTables(absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot SET ON DELETE on an INTERLEAVE IN table $0.",
                       table_name));
}

absl::Status InterleaveInToInParentOnDeleteCascadeUnsupported(
    absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute(
          "Altering the table $0 from INTERLEAVE IN to INTERLEAVE "
          "IN PARENT ON DELETE CASCADE is not supported. Please migrate the "
          "table to INTERLEAVE IN PARENT ON DELETE NO ACTION first, then "
          "migrate it to ON DELETE CASCADE.",
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

absl::Status UnsupportedAlterDatabaseOption(absl::string_view option_name) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Invalid Alter Database Option: $0."
                                       "Supported options are witness_location "
                                       "and default_leader.",
                                       option_name));
}

absl::Status NullValueAlterDatabaseOption() {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Alter Database Option has null value."));
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

absl::Status TooManyModelColumns(absl::string_view model_name,
                                 absl::string_view column_kind, int64_t limit) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Model $0 has too many $1; the limit is $2.", model_name,
                       column_kind, limit));
}

absl::Status NoColumnsModel(absl::string_view model_name,
                            absl::string_view column_kind) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Schema discovery is not available in emulator. "
                       "Please use explicit $1 clause for model $0.",
                       model_name, column_kind));
}

absl::Status LocalModelUnsupported(absl::string_view model_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Model $0 must specify REMOTE attribute.", model_name));
}

absl::Status NoModelEndpoint(absl::string_view model_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Model $0 has invalid endpoints option. Option is required.",
          model_name));
}

absl::Status AmbiguousModelEndpoint(absl::string_view model_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Model $0 has invalid endpoint option. Option cannot be "
                       "specified together with endpoints.",
                       model_name));
}

absl::Status InvalidModelDefaultBatchSize(absl::string_view model_name,
                                          int64_t value, int64_t limit) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Model $0 has invalid default_batch_size option: $1. "
                       "Must be between 1 and $2.",
                       model_name, value, limit));
}

absl::Status ModelDuplicateColumn(absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Duplicate name in schema: $0.", column_name));
}

absl::Status ModelCaseInsensitiveDuplicateColumn(
    absl::string_view column_name, absl::string_view original_column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Schema object names differ only in case: $0, $1.",
                       column_name, original_column_name));
}

absl::Status MlInputColumnMissing(absl::string_view column_name,
                                  absl::string_view column_type) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "The schema of a table or query provided to ML.PREDICT function "
          "doesn't match the model: the schema doesn't have required column "
          "`$0` with type `$1`.",
          column_name, column_type));
}

absl::Status MlInputColumnAmbiguous(absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "The schema of a table or query provided to ML.PREDICT function is "
          "ambiguous: it contains multiple columns named `$0`.",
          column_name));
}

absl::Status MlInputColumnTypeMismatch(absl::string_view column_name,
                                       absl::string_view input_column_type,
                                       absl::string_view model_column_type) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("The schema of a table or query provided to ML.PREDICT "
                       "function doesn't match the model: column `$0` has "
                       "type `$1` while the model's column has type `$2`",
                       column_name, input_column_type, model_column_type));
}

absl::Status MlPassThroughColumnAmbiguous(absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Column name $0 is ambiguous", column_name));
}

absl::Status MlPredictRow_Argument_Null(absl::string_view arg_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("The `$0` argument to ML_PREDICT_ROW cannot be NULL",
                       arg_name));
}

absl::Status MlPredictRow_Argument_NotObject(absl::string_view arg_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "The `$0` argument to ML_PREDICT_ROW function must be a JSON object",
          arg_name));
}

absl::Status MlPredictRow_Argument_UnexpectedValueType(
    absl::string_view arg_name, absl::string_view key, absl::string_view type) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("The `$0` argument to ML_PREDICT_ROW function expects "
                       "key `$1` to contain value of type `$2`",
                       arg_name, key, type));
}

absl::Status MlPredictRow_Argument_UnexpectedKey(absl::string_view arg_name,
                                                 absl::string_view key) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("The `$0` argument to ML_PREDICT_ROW "
                                       "function does not support key: `$1`",
                                       arg_name, key));
}

absl::Status MlPredictRow_ModelEndpoint_NoEndpoints() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "The model_endpoint argument passed to ML_PREDICT_ROW function must "
      "specify either 'endpoint' or 'endpoints'");
}

absl::Status MlPredictRow_ModelEndpoint_EndpointsAmbiguous() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "The model_endpoint argument passed to ML_PREDICT_ROW function cannot "
      "specify both 'endpoint' and 'endpoints'");
}

absl::Status MlPredictRow_ModelEndpoint_InvalidBatchSize(int64_t value_num,
                                                         int64_t min_num,
                                                         int64_t max_num) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("The model_endpoint argument passed to ML_PREDICT_ROW "
                       "function contains \'default_batch_size\' with value of "
                       "`$0 outside` of allowed range [$1, $2]",
                       value_num, min_num, max_num));
}

absl::Status MlPredictRow_Args_NoInstances() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "The args argument passed to ML_PREDICT_ROW function "
                      "must contain 'instances'");
}

absl::Status EmptyStruct() {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      "Empty STRUCT is not allowed.");
}

absl::Status StructFieldNumberExceedsLimit(int64_t limit) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("The number of struct field exceeds the limit: $0.",
                       limit));
}
absl::Status MissingStructFieldName(absl::string_view struct_type) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Missing struct field name in Struct: $0.",
                       struct_type));
}

absl::Status DuplicateStructName(absl::string_view struct_type,
                                 absl::string_view field_name) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Duplicate name in Struct: $0, $1.",
                                       struct_type, field_name));
}

absl::Status CaseInsensitiveDuplicateStructName(
    absl::string_view struct_type, absl::string_view field_name,
    absl::string_view existing_field_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Case Insensitive duplicate name in Struct: $0. "
                       "Struct field names differ only in case: $1, $2.",
                       struct_type, field_name, existing_field_name));
}

absl::Status ModelColumnTypeUnsupported(absl::string_view model_name,
                                        absl::string_view column_name,
                                        absl::string_view column_type) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Type $2 is not supported in model columns. "
                       "Used in Model $0 column $1.",
                       model_name, column_name, column_type));
}

absl::Status ModelColumnNotNull(absl::string_view model_name,
                                absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("NOT NULL constraint is not supported in models."
                       " Used in Model $0 column $1.",
                       model_name, column_name));
}

absl::Status ModelColumnHidden(absl::string_view model_name,
                               absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("HIDDEN attribute is not supported in models. "
                       "Used in Model $0 column $1.",
                       model_name, column_name));
}

absl::Status ModelColumnLength(absl::string_view model_name,
                               absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Length is not supported in model columns. Please use MAX."
          " Used in Model $0 column $1.",
          model_name, column_name));
}

absl::Status ModelColumnGenerated(absl::string_view model_name,
                                  absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Generated columns are not supported in models. "
                       " Used in Model $0 column $1.",
                       model_name, column_name));
}

absl::Status ModelColumnDefault(absl::string_view model_name,
                                absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Default values are not supported in models. "
                       " Used in Model $0 column $1.",
                       model_name, column_name));
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

absl::Status ChangeStreamNotFound(absl::string_view change_stream_name) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::StrCat("Change Stream not found: ", change_stream_name));
}

absl::Status PlacementNotFound(absl::string_view placement_name) {
  return absl::Status(absl::StatusCode::kNotFound,
                      absl::StrCat("Placement not found: ", placement_name));
}

absl::Status ModelNotFound(absl::string_view model_name) {
  return absl::Status(absl::StatusCode::kNotFound,
                      absl::StrCat("Model `", model_name, "` not found."));
}

absl::Status TableValuedFunctionNotFound(absl::string_view tvf_name) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::StrCat("Table valued function not found: ", tvf_name));
}

absl::Status SequenceNotFound(absl::string_view sequence_name) {
  return absl::Status(absl::StatusCode::kNotFound,
                      absl::StrCat("Sequence not found: ", sequence_name));
}

absl::Status TypeNotFound(absl::string_view type_name) {
  return absl::Status(absl::StatusCode::kNotFound,
                      absl::StrCat("Type not found: ", type_name));
}

absl::Status DropTableWithChangeStream(
    absl::string_view table_name, int64_t change_stream_count,
    absl::string_view change_stream_name_list_string) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot drop table $0. Table $0 is tracked by $1 Change Stream: $2. "
          "Tables explicitly tracked by a Change Stream cannot be dropped. "
          "Please drop the Change Stream or modify its FOR clause to stop "
          "tracking the table explicitly before dropping the table.",
          table_name, change_stream_count, change_stream_name_list_string));
}
absl::Status DropColumnWithChangeStream(
    absl::string_view table_name, absl::string_view column_name,
    int64_t change_stream_count,
    absl::string_view change_stream_name_list_string) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot drop column $0.$1 . Column $0.$1 is tracked by $2 Change "
          "Stream: $3. "
          "Columns explicitly tracked by a Change Stream cannot be dropped. "
          "Please alter the Change Stream to stop tracking the column before "
          "dropping the column.",
          table_name, column_name, change_stream_count,
          change_stream_name_list_string));
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

absl::Status InterleavingParentChildRowExistenceConstraintValidation(
    absl::string_view parent_table_name, absl::string_view child_table_name,
    absl::string_view key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Parent-child row existence constraint validation failed for "
          "table $0 at row key: $1. To migrate this table from INTERLEAVE IN "
          "to INTERLEAVE IN PARENT, all its rows must have existing parent "
          "rows in the $2 table.",
          child_table_name, key, parent_table_name));
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

absl::Status UnknownPlacement(absl::string_view placement_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Unknown placement: $0", placement_name));
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

absl::Status ColumnNotFoundInIndex(absl::string_view index_name,
                                   absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::Substitute("Index $0 does not have stored column $1.", index_name,
                       column_name));
}

absl::Status ColumnInIndexAlreadyExists(absl::string_view index_name,
                                        absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Column $0 is already existed in index $1.", column_name,
                       index_name));
}

absl::Status IndexInDifferentSchema(absl::string_view index_name,
                                    absl::string_view indexed_table_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Index $0 does not match the schema of the indexed "
                       "table $1.",
                       index_name, indexed_table_name));
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

absl::Status ForeignKeyOnDeleteActionUnsupported(
    absl::string_view referential_action) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute(
          "Foreign key referential action ON DELETE $0 is not supported.",
          referential_action));
}

absl::Status ForeignKeyEnforcementUnsupported() {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("Foreign key enforcement option is not supported."));
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

absl::Status InvalidDropDependentCheckConstraint(
    absl::string_view type_kind, absl::string_view dependency_name,
    absl::string_view dependent_check_constraint_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot drop $0 `$1` on which there is a dependent check constraint: "
          "$2.",
          type_kind, dependency_name, dependent_check_constraint_name));
}

absl::Status CannotAlterColumnDataTypeWithDependentCheckConstraint(
    absl::string_view column_name, absl::string_view check_constraint_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot change the data type of column `$0`, which is "
                       "used by check constraint `$1`.",
                       column_name, check_constraint_name));
}

absl::Status DependentCheckConstraintBecomesInvalid(
    absl::string_view modify_action, absl::string_view dependency_name,
    absl::string_view dependent_check_constraint, absl::string_view error) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot $0 `$1`. The new definition causes "
          "the definition of check constraint `$2` to become invalid with the "
          "following diagnostic message: $3",
          modify_action, dependency_name, dependent_check_constraint, error));
}

// Generated column errors.
absl::Status GeneratedColumnsNotEnabled() {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "Generated columns are not enabled.");
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

absl::Status CannotAlterGeneratedColumnStoredAttribute(
    absl::string_view table_name, absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Cannot change the STORED attribute for generated column `$0.$1`.",
          table_name, column_name));
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

absl::Status InvalidDropDependentColumn(absl::string_view type_kind,
                                        absl::string_view dependency_name,
                                        absl::string_view dependent_column) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot drop $0 `$1` on which there is a dependent column: $2.",
          type_kind, dependency_name, dependent_column));
}

absl::Status DependentColumnBecomesInvalid(absl::string_view modify_action,
                                           absl::string_view dependency_name,
                                           absl::string_view dependent_column,
                                           absl::string_view error) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot $0 `$1`. The new definition causes "
          "the definition of column `$2` to become invalid with the "
          "following diagnostic message: $3",
          modify_action, dependency_name, dependent_column, error));
}

// Column default values errors.
absl::Status ColumnDefaultValuesNotEnabled() {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "Column DEFAULT values are not enabled.");
}

absl::Status DefaultExpressionWithColumnDependency(
    absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Default value expression of column `$0` has a reference to another "
          "column. Default expressions referencing other columns are not "
          "supported.",
          column_name));
}

absl::Status ColumnDefaultValueParseError(absl::string_view table_name,
                                          absl::string_view column_name,
                                          absl::string_view message) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Error parsing the default value of column `$0`.`$1`: $2", table_name,
          column_name, message));
}

absl::Status CannotUseCommitTimestampWithColumnDefaultValue(
    absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot use commit timestamp column `$0` as a column "
                       "with default value.",
                       column_name));
}

absl::Status DefaultPKNeedsExplicitValue(absl::string_view column_name,
                                         absl::string_view op_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Implicit use of primary key default value for column `$0` is not "
          "allowed in $1 mutations. The column must have a specific value.",
          column_name, op_name));
}

absl::Status GeneratedPKNeedsExplicitValue(absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "The value of generated primary key column `$0` must be explicitly "
          "specified or else its non-key dependent columns must be specified "
          "in Update mutations.",
          column_name));
}

absl::Status GeneratedPkModified(absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kOutOfRange,
      absl::Substitute(
          "The value of generated primary key column `$0` cannot be modified "
          "when its non-key dependent columns are updated.",
          column_name));
}

absl::Status NeedAllDependentColumnsForGpk(absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "The value of generated primary key column `$0` cannot be evaluated "
          "since value of all its dependent columns is not specified. ",
          column_name));
}

absl::Status UserSuppliedValueInNonUpdateGpk(absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "The value of generated primary key column `$0` cannot be specified "
          "in operations except update operations.",
          column_name));
}

absl::Status CannotSetDefaultValueOnGeneratedColumn(
    absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Column $0 with a DEFAULT value cannot be a "
                       "generated column and vice versa.",
                       column_name));
}

// Query errors.
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
          "disable_query_null_filtered_index_check=true} on the table, or the "
          "query statement to bypass this check in the emulator. This hint "
          "will be ignored by the production Cloud Spanner service and the "
          "emulator will accept the query and return a valid result when it is "
          "run with the check disabled.",
          index_name));
}

absl::Status NonPartitionableQuery(absl::string_view reason) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat(
          "The emulator is not able to determine whether this query is "
          "partitionable. Please test this query against Cloud Spanner and "
          "obtain a query execution plan to confirm it is partitionable. A "
          "query is partitionable only if the first operator in the query "
          "execution plan is a Distributed Union. If you confirm it is "
          "partitionable, set the hint @{spanner_emulator."
          "disable_query_partitionability_check=true} on the query statement "
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

absl::Status ReadOnlyTransactionDoesNotSupportReadWriteOnlyFunctions(
    absl::string_view functions) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "The following functions can only be used in a read-write or "
          "partitioned-dml transaction. Current transaction type is ReadOnly: "
          "$0",
          functions));
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

absl::Status CannotInsertDuplicateKeyInsertOrUpdateDml(absl::string_view key) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot affect a row second time for key: $0", key));
}

absl::Status UnsupportedUpsertQueries(absl::string_view insert_mode) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("$0 statement is not supported in Emulator",
                       insert_mode));
}

absl::Status UnsupportedReturningWithUpsertQueries(
    absl::string_view insert_mode) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("Returning clause in $0 statement is not supported "
                       "in Emulator",
                       insert_mode));
}

absl::Status UnsupportedGeneratedKeyWithUpsertQueries() {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("Insert query with ON CONFLICT clause on table with "
                       "generated key is not supported in Emulator"));
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
absl::Status ToJsonStringNonJsonTypeNotSupported(absl::string_view type_name) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("TO_JSON_STRING is not supported on values of type $0",
                       type_name));
}

absl::Status NoMatchingFunctionSignature(
    absl::string_view function_name, absl::string_view supported_signature) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("No matching signature for function $0. "
                                       "Supported signature is $0($1)",
                                       function_name, supported_signature));
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

absl::Status TooManyElementsInInList(int max_elements_in_in_list) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("An IN list exceeds the maximum allowed length of $0.",
                       max_elements_in_in_list));
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

absl::Status RowDeletionPolicyDoesNotExist(absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("ROW DELETION POLICY does not exist on table $0.",
                       table_name));
}

absl::Status RowDeletionPolicyAlreadyExists(absl::string_view column_name,
                                            absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Cannot create ROW DELETION POLICY because there is already one "
          "on column named $0 in table $1.",
          column_name, table_name));
}

absl::Status RowDeletionPolicyOnColumnDoesNotExist(
    absl::string_view column_name, absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot create ROW DELETION POLICY because there is no "
                       "column named $0 in table $1.",
                       column_name, table_name));
}

absl::Status RowDeletionPolicyOnNonTimestampColumn(
    absl::string_view column_name, absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot create ROW DELETION POLICY because column $0 in "
                       "table $1 is not of type TIMESTAMP.",
                       column_name, table_name));
}

absl::Status RowDeletionPolicyWillBreak(absl::string_view column_name,
                                        absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "ROW DELETION POLICY will break if drop/alter column named "
          "$0 in table $1. Row Deletion Policy "
          "must be set on column of type TIMESTAMP.",
          column_name, table_name));
}

absl::Status RowDeletionPolicyHasChildWithOnDeleteNoAction(
    absl::string_view table_name, absl::string_view child_table_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot create row deletion policy on $0 because there "
                       "is a descendant Table $1 with ON DELETE NO ACTION.",
                       table_name, child_table_name));
}

absl::Status RowDeletionPolicyOnAncestors(
    absl::string_view table_name, absl::string_view ancestor_table_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Cannot create or alter table $0 with ON DELETE NO ACTION because "
          "there is a row deletion policy on ancestor table $1.",
          table_name, ancestor_table_name));
}

absl::Status SynonymDoesNotExist(absl::string_view synonym,
                                 absl::string_view table_name) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("SYNONYM $0 does not exist on table $1.",
                                       synonym, table_name));
}

absl::Status SynonymAlreadyExists(absl::string_view synonym,
                                  absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Cannot add a synonym to table $1 because the table already has a "
          "synonym $0.",
          synonym, table_name));
}

absl::Status CannotAlterSynonym(absl::string_view synonym,
                                absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Cannot alter synonym $0 in table $1. The existing synonym must "
          "be dropped first.",
          synonym, table_name));
}

absl::Status ForeignKeyRowDeletionPolicyAddNotAllowed(
    absl::string_view table_name, absl::string_view foreign_keys) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Cannot add a row deletion policy to table `$0`. It is "
                       "referenced by one or more foreign keys: `$1`.",
                       table_name, foreign_keys));
}

absl::Status NonHiddenTokenlistColumn(absl::string_view table_name,
                                      absl::string_view column_name) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("TOKENLIST column $0.$1 must be HIDDEN.",
                                       table_name, column_name));
}

// create search index errors
absl::Status SearchIndexNotPartitionByokenListType(
    absl::string_view index_name, absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Search index $0 cannot be partitioned by TOKENLIST type column $1",
          index_name, column_name));
}

absl::Status SearchIndexSortMustBeNotNullError(absl::string_view column_name,
                                               absl::string_view index_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Column $0 in search index $1 must be NOT NULL.",
                       column_name, index_name));
}

absl::Status SearchIndexOrderByMustBeIntegerType(
    absl::string_view index_name, absl::string_view column_name,
    absl::string_view column_type) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Search index $0 ordered by column $1 with invalid type $2",
          index_name, column_name, column_type));
}

absl::Status VectorIndexPartitionByUnsupported(absl::string_view index_name) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("PARTITION BY is not supported in vector index `$0`.",
                       index_name));
}
absl::Status VectorIndexNonArrayKey(absl::string_view column_string,
                                    absl::string_view index_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "The key column $0 in vector index $1 must be ARRAY type.",
          column_string, index_string));
}

absl::Status VectorIndexArrayKeyMustBeFloatOrDouble(
    absl::string_view column_string, absl::string_view index_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("The key column $0 in vector index $1 must be "
                       "ARRAY<FLOAT> or ARRAY<DOUBLE>.",
                       column_string, index_string));
}

absl::Status VectorIndexArrayKeyMustHaveVectorLength(
    absl::string_view column_string, absl::string_view index_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("The key column $0 in vector index $1 must have "
                       "`vector_length=>` set.",
                       column_string, index_string));
}

absl::Status VectorIndexArrayKeyVectorLengthTooLarge(
    absl::string_view column_string, absl::string_view index_string,
    int64_t actual_len_num, int64_t max_len_num) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "The key column $0 in vector index $1 has vector_length $2, which is "
          "larger than the maximum allowed vector_length $3.",
          column_string, index_string, actual_len_num, max_len_num));
}

absl::Status VectorIndexKeyNotNullFiltered(absl::string_view column_string,
                                           absl::string_view index_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("The key column $0 in vector index $1 must be NOT NULL, "
                       "or null filtered by `WHERE $0 IS NOT NULL`.",
                       column_string, index_string));
}

absl::Status AlterVectorIndexStoredColumnUnsupported() {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("Altering a stored column is not supported yet."));
}

absl::Status AlterVectorIndexSetOptionsUnsupported() {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute(
          "Options on vector index are not allowed to be altered."));
}

absl::Status VectorIndexStoredColumnNotFound(absl::string_view index_name,
                                             absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Index $0 specifies nonexistent stored column $1.",
                       index_name, column_name));
}

absl::Status VectorIndexStoredColumnAlreadyExists(
    absl::string_view index_name, absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Index $0 specifies stored column $1 twice.", index_name,
                       column_name));
}

absl::Status VectorIndexStoredColumnIsKey(absl::string_view index_name,
                                          absl::string_view column_name,
                                          absl::string_view table_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Index $0 specifies stored column $1 which is a key of table $2.",
          index_name, column_name, table_name));
}

absl::Status VectorIndexStoredColumnAlreadyPrimaryKey(
    absl::string_view index_name, absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Index $0 specifies stored column $1 already specified "
                       "as primary key.",
                       index_name, column_name));
}

absl::Status VectorIndexNotStoredColumn(absl::string_view index_name,
                                        absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Column $0 in index $1 is not a stored column. "
                       "Only stored columns may be dropped from indices.",
                       column_name, index_name));
}

absl::Status SearchIndexTokenlistKeyOrderUnsupported(
    absl::string_view column_name, absl::string_view index_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Token key $0 in search index $1 does not support order.",
          column_name, index_name));
}

// Search function errors
absl::Status InvalidUseOfSearchRelatedFunctionWithReason(
    absl::string_view reason) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Invalid use of search related function: $0", reason));
}

absl::Status TokenListNotMatchSearch(absl::string_view function_name,
                                     absl::string_view tokenizer_name) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("$0 function's first argument must be a "
                                       "TOKENLIST column generated by $1",
                                       function_name, tokenizer_name));
}

absl::Status SearchIndexNotUsable(absl::string_view index_name,
                                  absl::string_view reason) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("The index $0 cannot be used because it $1", index_name,
                       reason));
}

absl::Status FailToParseSearchQuery(absl::string_view query,
                                    absl::string_view errors) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Error(s) parsing search query `$0`, has error(s):\n-$1",
                       query, errors));
}

absl::Status ColumnNotSearchable(absl::string_view column_type) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Invalid search query. Trying to execute search related "
                       "function on unsupported column type: $0.",
                       column_type));
}

absl::Status InvalidQueryType(absl::string_view query_type) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Invalid search query type: $0.", query_type));
}

absl::Status InvalidNgramSize(absl::string_view error) {
  return absl::Status(absl::StatusCode::kInvalidArgument, error);
}

absl::Status ProjectTokenlistNotAllowed() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "TOKENLIST is an internal-only type and cannot be "
                      "returned to the user by SQL query.");
}

absl::Status TokenlistTypeMergeConflict() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "All elements to TOKENLIST_CONCAT must be produced by "
                      "the same kind of tokenization function.");
}

absl::Status FpAlgorithmOnlySupportedOnFloats() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "TOKENIZE_NUMBER: algorithm 'floatingpoint' can only be used to "
      "tokenize floating point values");
}

absl::Status NumericIndexingUnsupportedComparisonType(
    absl::string_view function_name, absl::string_view comparison_type) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "$0: Unsupported comparison type: '$1', supported comparison types: "
          "'all', 'range', 'equality' (equality is only allowed for integrals)",
          function_name, comparison_type));
}

absl::Status NumericIndexingUnsupportedAlgorithm(
    absl::string_view function_name, absl::string_view algorithm) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "$0: Algorithm is not supported: '$1'; supported algorithms are: "
          "'auto', 'logtree', 'prefixtree', 'floatingpoint' "
          "(floatingpoint is supported only for floating point types)",
          function_name, algorithm));
}

absl::Status NumericIndexingVariableMustBeFinite(
    absl::string_view var_name, absl::string_view value_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("TOKENIZE_NUMBER: $0 must be finite, got: $1", var_name,
                       value_string));
}

absl::Status NumericIndexingMinMustBeLessThanMax(
    absl::string_view function_name, absl::string_view min_string,
    absl::string_view max_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("$0: min must be less than max, got: $1 and $2",
                       function_name, min_string, max_string));
}

absl::Status NumericIndexingGranularityMustBeFiniteAndPositive(
    absl::string_view value_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "TOKENIZE_NUMBER: granularity must be finite and positive, got: $0",
          value_string));
}

absl::Status NumericIndexingGranularityMustBeLessThanDiffBetweenMinAndMax(
    absl::string_view granularity_string, absl::string_view min_string,
    absl::string_view max_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("TOKENIZE_NUMBER: granularity ($0) must be less than or "
                       "equal to the difference between min and max ($1, $2)",
                       granularity_string, min_string, max_string));
}

absl::Status NumericIndexingGranularityTooSmallForRange(
    absl::string_view min_allowed_granularity_string,
    absl::string_view granularity_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("TOKENIZE_NUMBER: granularity cannot be less than (max "
                       "- min) / (2^64 - 3)), which is: $0; got: $1",
                       min_allowed_granularity_string, granularity_string));
}

absl::Status NumericIndexingTreeBaseNotInRange(absl::string_view tree_base) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "TOKENIZE_NUMBER: tree_base must be in the range [2, 10], got: $0",
          tree_base));
}

absl::Status NumericIndexingPrecisionNotInRange(
    absl::string_view precision_num) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("TOKENIZE_NUMBER: precision must "
                                       "be in the range [1, 15], got: $0",
                                       precision_num));
}

absl::Status InvalidRelativeSearchType(absl::string_view relative_search_type) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("Invalid relative_search_type: '$0'.",
                                       relative_search_type));
}

absl::Status SearchSubstringSupportRelativeSearchTypeArgConflict() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "TOKENIZE_SUBSTRING: only one of 'support_relative_search' and "
      "'relative_search_types' can be specified.");
}

absl::Status RelativeSearchNotSupported(
    absl::string_view relative_search_type) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("The referenced substring TOKENLIST is "
                                       "not tokenized with '$0' support.",
                                       relative_search_type));
}

absl::Status IncorrectSnippetColumnType(absl::string_view column_type) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("Unable to execute snippet function on "
                                       "unsupported column type: $0.",
                                       column_type));
}

absl::Status InvalidSnippetQueryType(absl::string_view query_type) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Invalid snippet query type: $0.", query_type));
}

absl::Status InvalidContentType(absl::string_view function_name,
                                absl::string_view content_type,
                                absl::string_view valid_types) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Invalid use of $0: content_type. \'$1\' is not a "
                       "supported content type. Supported options:[$2]",
                       function_name, content_type, valid_types));
}

absl::Status InvalidUseOfSnippetArgs(absl::string_view arg_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("SNIPPET has out of range value for $0 argument.",
                       arg_name));
}

absl::Status ProtoTypeNotFound(absl::string_view type) {
  return absl::NotFoundError(absl::Substitute("Type not found: `$0`", type));
}

absl::Status ProtoEnumTypeNotFound(absl::string_view type) {
  return absl::NotFoundError(
      absl::Substitute("Enum type not found: `$0`", type));
}

absl::Status UnrecognizedColumnType(absl::string_view column_name,
                                    absl::string_view type) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Column $0 has an unrecognized column type $1.",
                       column_name, type));
}

absl::Status InvalidEnumValue(absl::string_view column_name, int64_t int_value,
                              absl::string_view column_type,
                              absl::string_view key) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Enum validation on column $0 failed at key $1 "
                       "because type $2 cannot be set to $3",
                       column_name, key, column_type, int_value));
}

absl::Status DeletedTypeStillInUse(absl::string_view type_name,
                                   absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Removed type $0 is still being used by column $1",
                       type_name, column_name));
}

absl::Status ExtensionNotSupported(int tag_number,
                                   absl::string_view field_name) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("`$1` options has reserved extension tag `$0`.",
                       tag_number, field_name));
}

absl::Status MessageExtensionsNotSupported(absl::string_view message_name) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("Message extensions are not supported for message `$0`.",
                       message_name));
}

absl::Status MessageTypeNotSupported(absl::string_view message_name) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("Message type `$0` is not supported.", message_name));
}

absl::Status RestrictedPackagesCantBeUsed(absl::string_view type_name,
                                          absl::string_view package_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Message `$0` has reserved package `$1`.", type_name,
                       package_name));
}

absl::Status ViewsNotSupported(absl::string_view view_op_name) {
  return absl::Status(
      absl::StatusCode::kUnimplemented,
      absl::Substitute("`$0` for INVOKER RIGHTS views is not supported.",
                       view_op_name));
}

absl::Status TooManyViewsPerDatabase(absl::string_view function_name,
                                     int limit) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Cannot add Function $0 : too many "
                                       "functions (limit $1 per database).",
                                       function_name, limit));
}

absl::Status ViewRequiresInvokerSecurity(absl::string_view view_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("View `$0` is missing the SQL SECURITY clause.",
                       view_name));
}

absl::Status ViewBodyAnalysisError(absl::string_view view_name,
                                   absl::string_view error) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Error analyzing the definition of view `$0`: $1",
                       view_name, error));
}

absl::Status ViewReplaceError(absl::string_view view_name,
                              absl::string_view error) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot replace VIEW `$0` because new definition is "
                       "invalid with the following diagnostic message:\n\n$1",
                       view_name, error));
}

absl::Status ViewReplaceRecursive(absl::string_view view_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot replace VIEW `$0` because new definition is recursive.",
          view_name));
}

absl::Status DependentViewBecomesInvalid(absl::string_view modify_action,
                                         absl::string_view dependency_name,
                                         absl::string_view dependent_view_name,
                                         absl::string_view error) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot $0 `$1`. The new definition causes "
                       "the definition of VIEW `$2` to become invalid with the "
                       "following diagnostic message: $3",
                       modify_action, dependency_name, dependent_view_name,
                       error));
}

absl::Status DependentViewColumnRename(absl::string_view modify_action,
                                       absl::string_view view_name,
                                       absl::string_view dependent_view,
                                       absl::string_view old_column_name,
                                       absl::string_view new_column_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot $0 replace VIEW `$1`. Action would implicitly change the "
          "name "
          "of an output column for VIEW `$2` from `$3` to `$4`.\nTip: Give "
          "explicit names to output columns in the VIEW definition before "
          "making this change.",
          modify_action, view_name, dependent_view, old_column_name,
          new_column_name));
}

absl::Status DependentViewColumnRetype(absl::string_view modify_action,
                                       absl::string_view view_name,
                                       absl::string_view dependent_view,
                                       absl::string_view old_type,
                                       absl::string_view new_type) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot $0 `$1`. Action would implicitly change the type "
          "of an output column for VIEW `$2` from `$3` to `$4`.\nTip: Give "
          "explicit type to output columns in the VIEW definition before "
          "making this change by adding casts/conversions to the intended "
          "final output type.",
          modify_action, view_name, dependent_view, old_type, new_type));
}

absl::Status InvalidDropDependentViews(absl::string_view type_kind,
                                       absl::string_view table_name,
                                       absl::string_view dependent_view) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot drop $0 `$1` on which there are dependent views: $2.",
          type_kind, table_name, dependent_view));
}

absl::Status ViewNotFound(absl::string_view view_name) {
  return absl::Status(absl::StatusCode::kNotFound,
                      absl::Substitute("View not found: $0", view_name));
}
absl::Status WithViewsAreNotSupported() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "WITH clauses are unsupported in view definitions.");
}

// Function errors
absl::Status FunctionRequiresInvokerSecurity(absl::string_view function_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Function `$0` is missing the SQL SECURITY clause.",
                       function_name));
}

absl::Status FunctionReplaceError(absl::string_view function_name,
                                  absl::string_view error) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot replace FUNCTION `$0` because new definition is "
                       "invalid with the following diagnostic message:\n\n$1",
                       function_name, error));
}

absl::Status FunctionBodyAnalysisError(absl::string_view function_name,
                                       absl::string_view error) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Error analyzing the definition of function `$0`: $1",
                       function_name, error));
}

absl::Status ReplacingBuiltInFunction(absl::string_view operation_string,
                                      absl::string_view type_kind,
                                      absl::string_view built_in_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot $0 a $1 with the same name as builtin function "
                       "`$2`. This may be supported in a future release.",
                       operation_string, type_kind, built_in_name));
}

absl::Status FunctionTypeMismatch(absl::string_view function_name,
                                  absl::string_view expected_type,
                                  absl::string_view actual_type) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Function `$0` has type mismatch. User provided type: "
                       "`$1`. Actual type: `$2`.",
                       function_name, expected_type, actual_type));
}

absl::Status DependentFunctionBecomesInvalid(
    absl::string_view modify_action, absl::string_view dependency_name,
    absl::string_view depedent_function_name, absl::string_view error) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot $0 `$1`. The new definition causes "
          "the definition of function `$2` to become invalid with the "
          "following diagnostic message: $3",
          modify_action, dependency_name, depedent_function_name, error));
}

absl::Status InvalidDropDependentFunction(
    absl::string_view type_kind, absl::string_view dependency_name,
    absl::string_view dependent_function) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot drop $0 `$1` on which there is a dependent function: $2.",
          type_kind, dependency_name, dependent_function));
}

absl::Status FunctionNotFound(absl::string_view function_name) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::Substitute("Function not found: $0", function_name));
}

absl::Status SequenceNotSupportedInPostgreSQL() {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "Sequence is not supported in PostgreSQL dialect of "
                      "the Emulator.");
}

absl::Status UnsupportedSequenceOption(absl::string_view option_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Unsupported sequence option: $0.", option_name));
}

absl::Status InvalidSequenceOptionValue(absl::string_view option_name,
                                        absl::string_view type) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Option $0 can only take a $1 value.",
                                       option_name, type));
}

absl::Status InvalidSequenceStartWithCounterValue() {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      "`start_with_counter` has to be a positive INT64 number");
}

absl::Status SequenceSkipRangeMinMaxNotSetTogether() {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      "`skip_range_min` and `skip_range_max` have to be set together");
}

absl::Status SequenceSkippedRangeHasAtleastOnePositiveNumber() {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      "The sequence skipped range has to contain at least one positive value");
}

absl::Status NamedSchemaNotFound(absl::string_view named_schema_name) {
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::Substitute("Schema not found: $0", named_schema_name));
}

absl::Status AlterNamedSchemaNotSupported() {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "ALTER SCHEMA not supported.");
}

absl::Status DropNamedSchemaHasDependencies(
    absl::string_view named_schema_name, const std::vector<std::string>& tables,
    const std::vector<std::string>& views,
    const std::vector<std::string>& indexes,
    const std::vector<std::string>& sequences) {
  std::string dependencies;
  if (!tables.empty()) {
    dependencies +=
        absl::StrCat("\nDependent tables:", absl::StrJoin(tables, ", "));
  }
  if (!views.empty()) {
    dependencies +=
        absl::StrCat("\nDependent views:", absl::StrJoin(views, ", "));
  }
  if (!indexes.empty()) {
    dependencies +=
        absl::StrCat("\nDependent indexes:", absl::StrJoin(indexes, ", "));
  }
  if (!sequences.empty()) {
    dependencies +=
        absl::StrCat("\nDependent sequences:", absl::StrJoin(sequences, ", "));
  }
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::StrCat(
          absl::Substitute("Cannot drop schema $0 while it has dependencies.",
                           named_schema_name),
          dependencies));
}

absl::Status DropNamedSchemaHasViews(absl::string_view named_schema_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot drop schema $0 while it has dependent views.",
                       named_schema_name));
}

absl::Status DropNamedSchemaHasIndexes(absl::string_view named_schema_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute("Cannot drop schema $0 while it has dependent indexes.",
                       named_schema_name));
}

absl::Status DropNamedSchemaHasSequences(absl::string_view named_schema_name) {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      absl::Substitute(
          "Cannot drop schema $0 while it has dependent sequences.",
          named_schema_name));
}

absl::Status SequenceSkipRangeMinLargerThanMax() {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      "`skip_range_min` has to be lesser than or equal to `skip_range_max`");
}

absl::Status UnsupportedSequenceKind(absl::string_view kind) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Invalid Sequence Kind: $0.", kind));
}

absl::Status SequenceNeedsAccessToSchema() {
  return absl::Status(
      absl::StatusCode::kInternal,
      "Sequence function needs access to the schema in the Emulator");
}

absl::Status SequenceExhausted(absl::string_view name) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::Substitute("Sequence $0 is exhausted", name));
}

absl::Status DdlInvalidArgumentError(absl::string_view message) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::Substitute("$0", message));
}

absl::Status DdlUnavailableError() {
  return absl::Status(
      absl::StatusCode::kUnavailable,
      "Error processing PostgreSQL DDL statements, retry may succeed.");
}

absl::Status UnsupportedVersionRetentionPeriodOptionValues() {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      "The version_retention_period option only supports string or null.");
}

absl::Status ColumnIsNotIdentityColumn(absl::string_view table_name,
                                       absl::string_view column_name) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Column is not an identity column in table ",
                                   table_name, ": ", column_name));
}

absl::Status DefaultSequenceKindAlreadySet() {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      "The default_sequence_kind option may not be unset once set.");
}

absl::Status UnsupportedDefaultSequenceKindOptionValues() {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      "The default_sequence_kind option only supports string or null.");
}

absl::Status UnspecifiedIdentityColumnSequenceKind(
    absl::string_view column_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat(
          "The sequence kind of an identity column ", column_name,
          " is not specified. Please specify the sequence kind explicitly or "
          "set the database option `default_sequence_kind`."));
}

absl::Status ChangeDefaultTimeZoneOnNonEmptyDatabase() {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      "The 'default_time_zone' database option cannot be "
                      "changed on a database with user defined tables.");
}

absl::Status UnsupportedDefaultTimeZoneOptionValues() {
  return absl::Status(
      absl::StatusCode::kFailedPrecondition,
      "The default_time_zone option only supports string or null.");
}

absl::Status InvalidDefaultTimeZoneOption(absl::string_view time_zone) {
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      absl::StrCat("Database has invalid default_time_zone "
                                   "option.  Invalid time zone name: ",
                                   time_zone));
}

absl::Status InvalidColumnIdentifierFormat(
    absl::string_view column_path_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat(
          "The column identifier's format is invalid: ", column_path_string,
          ". It should be either \"<schema>.<table>.<column>\" or "
          "\"<table>.<column>\"."));
}

absl::Status TableNotFoundInIdentityFunction(absl::string_view table_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat(
          "Table not found for the GET_TABLE_COLUMN_IDENTITY_STATE function: ",
          table_string));
}

absl::Status ColumnNotFoundInIdentityFunction(absl::string_view table_string,
                                              absl::string_view column_string) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Identity column not found in table ",
                                   table_string, ": ", column_string));
}

absl::Status UnspecifiedSequenceKind() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "The sequence does not have a valid sequence kind. Please "
      "specify the sequence kind explicitly or set the database option "
      "`default_sequence_kind`.");
}

absl::Status CannotSetSequenceClauseAndOptionTogether(
    absl::string_view sequence_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat("The sequence properties can only be set in either clauses "
                   "and options for the sequence ",
                   sequence_string));
}

absl::Status CannotAlterToIdentityColumn(absl::string_view table_string,
                                         absl::string_view column_string) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Cannot alter column `", table_string, ".",
                                   column_string, "` to an identity column."));
}

absl::Status CannotAlterColumnToDropIdentity(absl::string_view table_string,
                                             absl::string_view column_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat("Cannot alter column `", table_string, ".", column_string,
                   "` to drop the identity property."));
}

absl::Status CannotAlterIdentityColumnToGeneratedOrDefaultColumn(
    absl::string_view table_string, absl::string_view column_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::StrCat("Cannot alter an identity column `", table_string, ".",
                   column_string, "` to a generated or default column."));
}

absl::Status OptionsError(absl::string_view error_string) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      absl::StrCat("Bad Index options: ", error_string));
}

// FOR UPDATE-related errors.
absl::Status ForUpdateUnsupportedInReadOnlyTransactions() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "FOR UPDATE is not supported in this transaction type.");
}

absl::Status ForUpdateUnsupportedInSearchQueries() {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      "FOR UPDATE is not supported in search queries");
}

absl::Status ForUpdateCannotCombineWithLockScannedRanges() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "FOR UPDATE cannot be combined with statement-level lock hints");
}

absl::Status ApproxDistanceFunctionOptionsRequired(
    absl::string_view function_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Argument `options` is required for function $0.",
                       absl::AsciiStrToUpper(function_string)));
}

absl::Status ApproxDistanceFunctionOptionMustBeLiteral(
    absl::string_view function_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("Argument `options` of function $0 must be a literal.",
                       absl::AsciiStrToUpper(function_string)));
}

absl::Status ApproxDistanceFunctionInvalidJsonOption(
    absl::string_view function_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "Argument `options` of function $0 is invalid. It must have a single "
          "`num_leaves_to_search` field with an unsigned integer value.",
          absl::AsciiStrToUpper(function_string)));
}

absl::Status ApproxDistanceInvalidShape(absl::string_view function_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "The use of function $0 is not supported in this "
          "query. This function can only be used to sort vectors in a table "
          "based solely on their approximate distances to another constant "
          "vector with an ORDER BY clause. The ORDER BY must be followed by a "
          "mandatory LIMIT clause. Please use $1 in all other cases.",
          absl::AsciiStrToUpper(function_string),
          absl::AsciiStrToUpper(
              absl::StripPrefix(function_string, "approx_"))));
}

absl::Status ApproxDistanceLengthMismatch(absl::string_view function_string,
                                          int input_length, int index_length) {
  return absl::InvalidArgumentError(absl::Substitute(
      "$0: Array length mismatch: $1 and $2.",
      absl::AsciiStrToUpper(function_string), input_length, index_length));
}

absl::Status VectorIndexesUnusable(absl::string_view distance_type_name,
                                   absl::string_view ann_func_column_name,
                                   absl::string_view ann_func_name) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "No usable vector index can be found for this query: $0.",
          absl::StrCat("a vector index with distance type ", distance_type_name,
                       " must be defined on column ", ann_func_column_name,
                       " for function ", absl::AsciiStrToUpper(ann_func_name),
                       "; also make sure that the vector index is not in "
                       "backfilling, and the "
                       "query has a filter that matches that of the vector "
                       "index")));
}

absl::Status VectorIndexesUnusableNotNullFiltered(
    absl::string_view index_string, absl::string_view column_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("The index $0 cannot be used because it filters nulls "
                       "that are required to answer the query for column $1.",
                       index_string, column_string));
}

absl::Status VectorIndexesUnusableForceIndexWrongDistanceType(
    absl::string_view index_string, absl::string_view distance_type_name,
    absl::string_view ann_func_name, absl::string_view column_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("The index $0 cannot be used because it is a vector "
                       "index of distance type $1 but the query requires "
                       "distance computation function $2 on column $3.",
                       index_string, distance_type_name,
                       absl::AsciiStrToUpper(ann_func_name), column_string));
}

absl::Status VectorIndexesUnusableForceIndexWrongColumn(
    absl::string_view index_string, absl::string_view ann_func_name,
    absl::string_view column_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute("The index $0 cannot be used because it is not a vector "
                       "index on column $1 which is required for $2.",
                       index_string, column_string,
                       absl::AsciiStrToUpper(ann_func_name)));
}

absl::Status NotVectorIndexes(absl::string_view index_string) {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      absl::Substitute(
          "The index $0 cannot be used because it is not a vector index.",
          index_string));
}

absl::Status RepeatableReadOnlySupportedInReadWriteTransactions() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "REPEATABLE_READ isolation level is only allowed on read-write "
      "transactions.");
}

absl::Status ReadLockModeInRepeatableReadMustBeUnspecified() {
  return absl::Status(
      absl::StatusCode::kInvalidArgument,
      "Read lock mode must not be set on read-write transactions using the "
      "REPEATABLE_READ isolation level.");
}

absl::Status RenameTableNotSupportedInPostgreSQL() {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "RENAME TABLE is not supported in PostgreSQL dialect.");
}

}  // namespace error
}  // namespace emulator
}  // namespace spanner
}  // namespace google
