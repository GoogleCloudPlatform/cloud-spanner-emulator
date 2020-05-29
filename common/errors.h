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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_ERRORS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_ERRORS_H_

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "backend/common/ids.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace error {

// Generic errors.
absl::Status Internal(absl::string_view msg);

// Project errors.
absl::Status InvalidProjectURI(absl::string_view uri);

// Instance config errors.
absl::Status InvalidInstanceConfigURI(absl::string_view uri);
absl::Status InstanceConfigNotFound(absl::string_view config_id);

// Instance errors.
absl::Status InvalidInstanceURI(absl::string_view uri);
absl::Status InstanceNotFound(absl::string_view uri);
absl::Status InstanceAlreadyExists(absl::string_view uri);
absl::Status InstanceNameMismatch(absl::string_view uri);
absl::Status InstanceUpdatesNotSupported();
absl::Status InvalidInstanceName(absl::string_view instance_id);

// Database errors.
absl::Status InvalidDatabaseURI(absl::string_view uri);
absl::Status DatabaseNotFound(absl::string_view uri);
absl::Status DatabaseAlreadyExists(absl::string_view uri);
absl::Status CreateDatabaseMissingCreateStatement();
absl::Status InvalidCreateDatabaseStatement(absl::string_view statement);
absl::Status UpdateDatabaseMissingStatements();
absl::Status TooManyDatabasesPerInstance(absl::string_view instance_uri);
absl::Status InvalidDatabaseName(absl::string_view database_id);

// Operation errors.
absl::Status InvalidOperationId(absl::string_view id);
absl::Status InvalidOperationURI(absl::string_view uri);
absl::Status OperationAlreadyExists(absl::string_view uri);
absl::Status OperationNotFound(absl::string_view uri);

// IAM errors.
absl::Status IAMPoliciesNotSupported();

// Label errors
absl::Status TooManyLabels(int num);
absl::Status BadLabelKey(absl::string_view key);
absl::Status BadLabelValue(absl::string_view key, absl::string_view value);

// Session errors.
absl::Status InvalidSessionURI(absl::string_view uri);
absl::Status SessionNotFound(absl::string_view uri);
absl::Status TooFewSessions(int session_count);

// Missing required field in proto error.
absl::Status MissingRequiredFieldError(absl::string_view field);

// Type proto errors.
absl::Status UnspecifiedType(absl::string_view proto);
absl::Status ArrayTypeMustSpecifyElementType(absl::string_view proto);

// Value proto errors.
absl::Status ValueProtoTypeMismatch(absl::string_view proto,
                                    absl::string_view expected_type);
absl::Status CouldNotParseStringAsInteger(absl::string_view str);
absl::Status CouldNotParseStringAsDouble(absl::string_view str);
absl::Status CouldNotParseStringAsTimestamp(absl::string_view str,
                                            absl::string_view error);
absl::Status TimestampMustBeInUTCTimeZone(absl::string_view str);
absl::Status CouldNotParseStringAsDate(absl::string_view str);
absl::Status InvalidDate(absl::string_view str);
absl::Status CouldNotParseStringAsBytes(absl::string_view str);
absl::Status TimestampOutOfRange(absl::string_view time);
absl::Status MultipleValuesForColumn(absl::string_view column);

// Key proto errors.
absl::Status WrongNumberOfKeyParts(absl::string_view table_or_index_name,
                                   int expected_key_parts, int found_key_parts,
                                   absl::string_view supplied_key);
absl::Status KeyRangeMissingStart();
absl::Status KeyRangeMissingEnd();

// Mutation proto errors.
absl::Status BadDeleteRange(absl::string_view start_key,
                            absl::string_view limit_key);
absl::Status MutationTableRequired();

// Transaction errors.
absl::Status AbortConcurrentTransaction(int64_t requestor_id, int64_t holder_id);
absl::Status TransactionNotFound(backend::TransactionID id);
absl::Status TransactionClosed(backend::TransactionID id);
absl::Status InvalidTransactionID(backend::TransactionID id);
absl::Status InvalidTransactionType(absl::string_view msg);
absl::Status InvalidTransactionUsage(absl::string_view msg,
                                     backend::TransactionID id);
absl::Status CannotReturnReadTimestampForReadWriteTransaction();
absl::Status InvalidReadOptionForMultiUseTransaction(
    absl::string_view timestamp_bound);
absl::Status InvalidModeForReadOnlySingleUseTransaction();
absl::Status DmlDoesNotSupportSingleUseTransaction();
absl::Status PartitionReadDoesNotSupportSingleUseTransaction();
absl::Status PartitionReadNeedsReadOnlyTxn();
absl::Status CannotCommitRollbackReadOnlyOrPartitionedDmlTransaction();
absl::Status CannotReusePartitionedDmlTransaction();
absl::Status InvalidOperationUsingPartitionedDmlTransaction();
absl::Status CannotCommitAfterRollback();
absl::Status CannotRollbackAfterCommit();
absl::Status CannotReadOrQueryAfterCommitOrRollback();
absl::Status ReadTimestampPastVersionGCLimit(absl::Time timestamp);
absl::Status AbortDueToConcurrentSchemaChange(backend::TransactionID id);

// DDL errors.
absl::Status EmptyDDLStatement();
absl::Status DDLStatementWithErrors(absl::string_view ddl_string,
                                    const std::vector<std::string>& errors);

// Schema validation errors.
absl::Status InvalidSchemaName(absl::string_view object_kind,
                               absl::string_view identifier);
absl::Status InvalidConstraintName(absl::string_view constraint_type,
                                   absl::string_view constraint_name,
                                   absl::string_view reserved_prefix);
absl::Status CannotNameIndexPrimaryKey();
absl::Status CannotCreateIndexOnArrayColumns(absl::string_view index_name,
                                             absl::string_view column_name);
absl::Status InvalidPrimaryKeyColumnType(absl::string_view column_name,
                                         absl::string_view type);
absl::Status InvalidColumnLength(absl::string_view column_name,
                                 int64_t specified_length, int64_t min_length,
                                 int64_t max_length);
absl::Status UnallowedCommitTimestampOption(absl::string_view column_name);
absl::Status InvalidColumnSizeReduction(absl::string_view column_name,
                                        int64_t specified_length,
                                        int64_t existing_length,
                                        absl::string_view key);
absl::Status ColumnNotNull(absl::string_view column_name,
                           absl::string_view key);
absl::Status CannotChangeColumnType(absl::string_view column_name,
                                    absl::string_view old_type,
                                    absl::string_view new_type);
absl::Status AddingNotNullColumn(absl::string_view table_name,
                                 absl::string_view column_name);
absl::Status InvalidDropColumnWithDependency(absl::string_view column_name,
                                             absl::string_view table_name,
                                             absl::string_view index_name);
absl::Status CannotChangeKeyColumn(absl::string_view column_name,
                                   absl::string_view reason);
absl::Status CannotChangeKeyColumnWithChildTables(
    absl::string_view column_name);
absl::Status InvalidDropKeyColumn(absl::string_view colum_name,
                                  absl::string_view table_name);
absl::Status TooManyColumns(absl::string_view object_type,
                            absl::string_view object_name, int64_t limit);
absl::Status TooManyKeys(absl::string_view object_type,
                         absl::string_view object_name, int64_t key_count,
                         int64_t limit);
absl::Status NoColumnsTable(absl::string_view object_type,
                            absl::string_view object_name);
absl::Status TooManyIndicesPerTable(absl::string_view index_name,
                                    absl::string_view table_name, int64_t limit);
absl::Status DeepNesting(absl::string_view object_type,
                         absl::string_view object_name, int limit);
absl::Status DropTableWithInterleavedTables(absl::string_view table_name,
                                            absl::string_view child_tables);
absl::Status DropTableWithDependentIndices(absl::string_view table_name,
                                           absl::string_view indexes);
absl::Status SetOnDeleteWithoutInterleaving(absl::string_view table_name);
absl::Status NonExistentKeyColumn(absl::string_view object_type,
                                  absl::string_view object_name,
                                  absl::string_view key_column);
absl::Status DuplicateColumnName(absl::string_view column_name);
absl::Status MultipleRefsToKeyColumn(absl::string_view object_type,
                                     absl::string_view object_name,
                                     absl::string_view key_column);
absl::Status IncorrectParentKeyPosition(absl::string_view child_object_type,
                                        absl::string_view child_object_name,
                                        absl::string_view parent_key_column,
                                        int position);
absl::Status MustReferenceParentKeyColumn(absl::string_view child_object_type,
                                          absl::string_view child_object_name,
                                          absl::string_view parent_key_column);
absl::Status IncorrectParentKeyOrder(absl::string_view child_object_type,
                                     absl::string_view child_object_name,
                                     absl::string_view parent_key_column,
                                     absl::string_view child_key_order);
absl::Status IncorrectParentKeyType(absl::string_view child_object_type,
                                    absl::string_view child_object_name,
                                    absl::string_view parent_key_column,
                                    absl::string_view child_key_type,
                                    absl::string_view parent_key_type);
absl::Status IncorrectParentKeyLength(absl::string_view child_object_type,
                                      absl::string_view child_object_name,
                                      absl::string_view parent_key_column,
                                      absl::string_view child_key_length,
                                      absl::string_view parent_key_length);
absl::Status IncorrectParentKeyNullability(absl::string_view child_object_type,
                                           absl::string_view child_object_name,
                                           absl::string_view parent_key_column,
                                           absl::string_view parent_nullability,
                                           absl::string_view child_nullability);
absl::Status IndexWithNoKeys(absl::string_view index_name);
absl::Status IndexRefsKeyAsStoredColumn(absl::string_view index_name,
                                        absl::string_view column_name);
absl::Status IndexRefsColumnTwice(absl::string_view index_name,
                                  absl::string_view key_column);
absl::Status IndexInterleaveTableNotFound(absl::string_view index_name,
                                          absl::string_view table_name);
absl::Status IndexRefsUnsupportedColumn(absl::string_view index_name,
                                        absl::string_view type);
absl::Status IndexInterleaveTableUnacceptable(absl::string_view index_name,
                                              absl::string_view indexed_table,
                                              absl::string_view parent_table);
absl::Status IndexRefsTableKeyAsStoredColumn(absl::string_view index_name,
                                             absl::string_view stored_column,
                                             absl::string_view base_table);
absl::Status IndexRefsNonExistentColumn(absl::string_view index_name,
                                        absl::string_view column_name);
absl::Status AlteringParentColumn(absl::string_view column_name);
absl::Status ChangingNullConstraintOnIndexedColumn(
    absl::string_view column_name, absl::string_view index_name);
absl::Status ConcurrentSchemaChangeOrReadWriteTxnInProgress();

// Schema access errors.
absl::Status TableNotFound(absl::string_view table_name);
absl::Status TableNotFoundAtTimestamp(absl::string_view table_name,
                                      absl::Time timestamp);
absl::Status IndexNotFound(absl::string_view index_name);
absl::Status ColumnNotFound(absl::string_view table_name,
                            absl::string_view column_name);
absl::Status ColumnNotFoundAtTimestamp(absl::string_view table_name,
                                       absl::string_view column_name,
                                       absl::Time timestamp);
absl::Status ColumnValueTypeMismatch(absl::string_view table_name,
                                     absl::string_view column_type,
                                     absl::string_view value_type);
absl::Status CannotParseKeyValue(absl::string_view table_name,
                                 absl::string_view column,
                                 absl::string_view column_type);
absl::Status MutationColumnAndValueSizeMismatch(int columns_size,
                                                int values_size);
absl::Status SchemaObjectAlreadyExists(absl::string_view schema_object,
                                       absl::string_view name);

// Commit timestamp errors.
absl::Status CommitTimestampInFuture(absl::Time timestamp);
absl::Status CannotReadPendingCommitTimestamp(absl::string_view table_name);
absl::Status CommitTimestampNotInFuture(absl::string_view column,
                                        absl::string_view key,
                                        absl::Time timestamp);
absl::Status PendingCommitTimestampAllOrNone(int64_t index_num);

// Time errors.
absl::Status InvalidTime(absl::string_view msg);

// Read argument errors.
absl::Status StalenessMustBeNonNegative();
absl::Status InvalidMinReadTimestamp(absl::Time min_read_timestamp);
absl::Status InvalidExactReadTimestamp(absl::Time exact_read_timestamp);
absl::Status StrongReadOptionShouldBeTrue();
absl::Status InvalidReadLimit();
absl::Status InvalidReadLimitWithPartitionToken();

// Constraint errors.
absl::Status RowAlreadyExists(absl::string_view table_name,
                              absl::string_view key);
absl::Status RowNotFound(absl::string_view table_name, absl::string_view key);
absl::Status ParentKeyNotFound(absl::string_view parent_table_name,
                               absl::string_view child_table_name,
                               absl::string_view key);
absl::Status ChildKeyExists(absl::string_view parent_table_name,
                            absl::string_view child_table_name,
                            absl::string_view key);
absl::Status NullValueForNotNullColumn(absl::string_view table_name,
                                       absl::string_view column_name);
absl::Status NullValueForNotNullColumn(absl::string_view table_name,
                                       absl::string_view column_name,
                                       absl::string_view key);
absl::Status InvalidColumnLength(absl::string_view table_name,
                                 absl::string_view column_name,
                                 int max_column_length);
absl::Status InvalidStringEncoding(absl::string_view table_name,
                                   absl::string_view column_name);
absl::Status UTF8StringColumn(absl::string_view column_name,
                              absl::string_view key);
absl::Status ValueExceedsLimit(absl::string_view column_name, int value_size,
                               int max_column_size);
absl::Status NonNullValueNotSpecifiedForInsert(absl::string_view table_name,
                                               absl::string_view column_name);
absl::Status KeyTooLarge(absl::string_view table_name, int64_t key_size,
                         int64_t max_key_size);
absl::Status IndexKeyTooLarge(absl::string_view index_name, int64_t key_size,
                              int64_t max_key_size);

// Index errors.
absl::Status UniqueIndexConstraintViolation(absl::string_view index_name,
                                            absl::string_view key);

absl::Status UniqueIndexViolationOnIndexCreation(absl::string_view index_name,
                                                 absl::string_view key);

absl::Status IndexTableDoesNotMatchBaseTable(absl::string_view base_table,
                                             absl::string_view indexed_table,
                                             absl::string_view index);

absl::Status IndexNotFound(absl::string_view index, absl::string_view table);

absl::Status ColumnNotFoundInIndex(absl::string_view index,
                                   absl::string_view indexed_table,
                                   absl::string_view column);

// Query errors.
absl::Status UnableToInferUndeclaredParameter(absl::string_view parameter_name);
absl::Status InvalidHint(absl::string_view hint_string);
absl::Status InvalidHintValue(absl::string_view hint_string,
                              absl::string_view value_string);
absl::Status EmulatorDoesNotSupportQueryPlans();
absl::Status InvalidStatementHintValue(absl::string_view hint_string,
                                       absl::string_view hint_value);
absl::Status MultipleValuesForSameHint(absl::string_view hint_string);
absl::Status InvalidHintForNode(absl::string_view hint_string,
                                absl::string_view supported_node);
absl::Status InvalidBatchDmlRequest();
absl::Status BatchDmlOnlySupportsReadWriteTransaction();
absl::Status ExecuteBatchDmlOnlySupportsDmlStatements(int index,
                                                      absl::string_view query);
absl::Status ReadOnlyTransactionDoesNotSupportDml(
    absl::string_view transaction_type);

// Partition Read errors.
absl::Status InvalidBytesPerBatch(absl::string_view message_name);
absl::Status InvalidMaxPartitionCount(absl::string_view message_name);
absl::Status InvalidPartitionToken();
absl::Status ReadFromDifferentSession();
absl::Status ReadFromDifferentTransaction();
absl::Status ReadFromDifferentParameters();
absl::Status InvalidPartitionedQueryMode();

}  // namespace error
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_ERRORS_H_
