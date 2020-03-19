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

#include "zetasql/base/status.h"
#include "absl/strings/string_view.h"
#include "backend/common/ids.h"
#include "zetasql/base/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace error {

// Generic errors.
zetasql_base::Status Internal(absl::string_view msg);

// Project errors.
zetasql_base::Status InvalidProjectURI(absl::string_view uri);

// Instance config errors.
zetasql_base::Status InvalidInstanceConfigURI(absl::string_view uri);
zetasql_base::Status InstanceConfigNotFound(absl::string_view config_id);

// Instance errors.
zetasql_base::Status InvalidInstanceURI(absl::string_view uri);
zetasql_base::Status InstanceNotFound(absl::string_view uri);
zetasql_base::Status InstanceAlreadyExists(absl::string_view uri);
zetasql_base::Status InstanceNameMismatch(absl::string_view uri);
zetasql_base::Status InstanceUpdatesNotSupported();
zetasql_base::Status InvalidInstanceName(absl::string_view instance_id);

// Database errors.
zetasql_base::Status InvalidDatabaseURI(absl::string_view uri);
zetasql_base::Status DatabaseNotFound(absl::string_view uri);
zetasql_base::Status DatabaseAlreadyExists(absl::string_view uri);
zetasql_base::Status CreateDatabaseMissingCreateStatement();
zetasql_base::Status InvalidCreateDatabaseStatement(absl::string_view statement);
zetasql_base::Status UpdateDatabaseMissingStatements();
zetasql_base::Status TooManyDatabasesPerInstance(absl::string_view instance_uri);
zetasql_base::Status InvalidDatabaseName(absl::string_view database_id);

// Operation errors.
zetasql_base::Status InvalidOperationURI(absl::string_view uri);
zetasql_base::Status OperationAlreadyExists(absl::string_view uri);
zetasql_base::Status OperationNotFound(absl::string_view uri);

// IAM errors.
zetasql_base::Status IAMPoliciesNotSupported();

// Label errors
zetasql_base::Status TooManyLabels(int num);
zetasql_base::Status BadLabelKey(absl::string_view key);
zetasql_base::Status BadLabelValue(absl::string_view key, absl::string_view value);

// Session errors.
zetasql_base::Status InvalidSessionURI(absl::string_view uri);
zetasql_base::Status SessionNotFound(absl::string_view uri);

// Missing required field in proto error.
zetasql_base::Status MissingRequiredFieldError(absl::string_view field);

// Type proto errors.
zetasql_base::Status UnspecifiedType(absl::string_view proto);
zetasql_base::Status ArrayTypeMustSpecifyElementType(absl::string_view proto);

// Value proto errors.
zetasql_base::Status ValueProtoTypeMismatch(absl::string_view proto,
                                    absl::string_view expected_type);
zetasql_base::Status CouldNotParseStringAsInteger(absl::string_view str);
zetasql_base::Status CouldNotParseStringAsDouble(absl::string_view str);
zetasql_base::Status CouldNotParseStringAsTimestamp(absl::string_view str,
                                            absl::string_view error);
zetasql_base::Status TimestampMustBeInUTCTimeZone(absl::string_view str);
zetasql_base::Status CouldNotParseStringAsDate(absl::string_view str);
zetasql_base::Status InvalidDate(absl::string_view str);
zetasql_base::Status CouldNotParseStringAsBytes(absl::string_view str);
zetasql_base::Status TimestampOutOfRange(absl::string_view time);
zetasql_base::Status MultipleValuesForColumn(absl::string_view column);

// Key proto errors.
zetasql_base::Status WrongNumberOfKeyParts(absl::string_view table_or_index_name,
                                   int expected_key_parts, int found_key_parts,
                                   absl::string_view supplied_key);
zetasql_base::Status KeyRangeMissingStart();
zetasql_base::Status KeyRangeMissingEnd();

// Mutation proto errors.
zetasql_base::Status BadDeleteRange(absl::string_view start_key,
                            absl::string_view limit_key);
zetasql_base::Status MutationTableRequired();

// Transaction errors.
zetasql_base::Status AbortConcurrentTransaction(int64_t requestor_id, int64_t holder_id);
zetasql_base::Status TransactionNotFound(backend::TransactionID id);
zetasql_base::Status TransactionClosed(backend::TransactionID id);
zetasql_base::Status InvalidTransactionID(backend::TransactionID id);
zetasql_base::Status InvalidTransactionType(absl::string_view msg);
zetasql_base::Status InvalidTransactionUsage(absl::string_view msg,
                                     backend::TransactionID id);
zetasql_base::Status CannotReturnReadTimestampForReadWriteTransaction();
zetasql_base::Status InvalidReadOptionForMultiUseTransaction(
    absl::string_view timestamp_bound);
zetasql_base::Status InvalidModeForReadOnlySingleUseTransaction();
zetasql_base::Status DmlDoesNotSupportSingleUseTransaction();
zetasql_base::Status PartitionReadDoesNotSupportSingleUseTransaction();
zetasql_base::Status PartitionReadOnlySupportsReadOnlyTransaction();
zetasql_base::Status CannotCommitRollbackReadOnlyTransaction();
zetasql_base::Status CannotCommitAfterRollback();
zetasql_base::Status CannotRollbackAfterCommit();
zetasql_base::Status CannotReadOrQueryAfterCommitOrRollback();
zetasql_base::Status ReadTimestampPastVersionGCLimit(absl::Time timestamp);
zetasql_base::Status AbortDueToConcurrentSchemaChange(backend::TransactionID id);

// DDL errors.
zetasql_base::Status EmptyDDLStatement();
zetasql_base::Status DDLStatementWithErrors(absl::string_view ddl_string,
                                    const std::vector<std::string>& errors);

// Schema validation errors.
zetasql_base::Status InvalidSchemaName(absl::string_view object_kind,
                               absl::string_view identifier,
                               absl::string_view reason = "");
zetasql_base::Status CannotNameIndexPrimaryKey();
zetasql_base::Status CannotCreateIndexOnArrayColumns(absl::string_view index_name,
                                             absl::string_view column_name);
zetasql_base::Status InvalidPrimaryKeyColumnType(absl::string_view column_name,
                                         absl::string_view type);
zetasql_base::Status InvalidColumnLength(absl::string_view column_name,
                                 int64_t specified_length, int64_t min_length,
                                 int64_t max_length);
zetasql_base::Status UnallowedCommitTimestampOption(absl::string_view column_name);
zetasql_base::Status InvalidColumnSizeReduction(absl::string_view column_name,
                                        int64_t specified_length,
                                        int64_t existing_length,
                                        absl::string_view key);
zetasql_base::Status ColumnNotNull(absl::string_view column_name,
                           absl::string_view key);
zetasql_base::Status CannotChangeColumnType(absl::string_view column_name,
                                    absl::string_view old_type,
                                    absl::string_view new_type);
zetasql_base::Status AddingNotNullColumn(absl::string_view table_name,
                                 absl::string_view column_name);
zetasql_base::Status InvalidDropColumnWithDependency(absl::string_view column_name,
                                             absl::string_view table_name,
                                             absl::string_view index_name);
zetasql_base::Status CannotChangeKeyColumn(absl::string_view column_name,
                                   absl::string_view reason);
zetasql_base::Status InvalidDropKeyColumn(absl::string_view colum_name,
                                  absl::string_view table_name);
zetasql_base::Status TooManyColumns(absl::string_view object_type,
                            absl::string_view object_name, int64_t limit);
zetasql_base::Status TooManyKeys(absl::string_view object_type,
                         absl::string_view object_name, int64_t key_count,
                         int64_t limit);
zetasql_base::Status NoColumnsTable(absl::string_view object_type,
                            absl::string_view object_name);
zetasql_base::Status TooManyIndicesPerTable(absl::string_view index_name,
                                    absl::string_view table_name, int64_t limit);
zetasql_base::Status DeepNesting(absl::string_view object_type,
                         absl::string_view object_name, int limit);
zetasql_base::Status DropTableWithInterleavedTables(absl::string_view table_name,
                                            absl::string_view child_tables);
zetasql_base::Status DropTableWithDependentIndices(absl::string_view table_name,
                                           absl::string_view indexes);
zetasql_base::Status SetOnDeleteWithoutInterleaving(absl::string_view table_name);
zetasql_base::Status NonExistentKeyColumn(absl::string_view object_type,
                                  absl::string_view object_name,
                                  absl::string_view key_column);
zetasql_base::Status DuplicateColumnName(absl::string_view column_name);
zetasql_base::Status MultipleRefsToKeyColumn(absl::string_view object_type,
                                     absl::string_view object_name,
                                     absl::string_view key_column);
zetasql_base::Status IncorrectParentKeyPosition(absl::string_view child_object_type,
                                        absl::string_view child_object_name,
                                        absl::string_view parent_key_column,
                                        int position);
zetasql_base::Status MustReferenceParentKeyColumn(absl::string_view child_object_type,
                                          absl::string_view child_object_name,
                                          absl::string_view parent_key_column);
zetasql_base::Status IncorrectParentKeyOrder(absl::string_view child_object_type,
                                     absl::string_view child_object_name,
                                     absl::string_view parent_key_column,
                                     absl::string_view child_key_order);
zetasql_base::Status IncorrectParentKeyType(absl::string_view child_object_type,
                                    absl::string_view child_object_name,
                                    absl::string_view parent_key_column,
                                    absl::string_view child_key_type,
                                    absl::string_view parent_key_type);
zetasql_base::Status IncorrectParentKeyLength(absl::string_view child_object_type,
                                      absl::string_view child_object_name,
                                      absl::string_view parent_key_column,
                                      absl::string_view child_key_length,
                                      absl::string_view parent_key_length);
zetasql_base::Status IncorrectParentKeyNullability(absl::string_view child_object_type,
                                           absl::string_view child_object_name,
                                           absl::string_view parent_key_column,
                                           absl::string_view parent_nullability,
                                           absl::string_view child_nullability);
zetasql_base::Status IndexWithNoKeys(absl::string_view index_name);
zetasql_base::Status IndexRefsKeyAsStoredColumn(absl::string_view index_name,
                                        absl::string_view column_name);
zetasql_base::Status IndexRefsColumnTwice(absl::string_view index_name,
                                  absl::string_view key_column);
zetasql_base::Status IndexInterleaveTableNotFound(absl::string_view index_name,
                                          absl::string_view table_name);
zetasql_base::Status IndexRefsUnsupportedColumn(absl::string_view index_name,
                                        absl::string_view type);
zetasql_base::Status IndexInterleaveTableUnacceptable(absl::string_view index_name,
                                              absl::string_view indexed_table,
                                              absl::string_view parent_table);
zetasql_base::Status IndexRefsTableKeyAsStoredColumn(absl::string_view index_name,
                                             absl::string_view stored_column,
                                             absl::string_view base_table);
zetasql_base::Status IndexRefsNonExistentColumn(absl::string_view index_name,
                                        absl::string_view column_name);
zetasql_base::Status ChangingNullConstraintOnIndexedColumn(
    absl::string_view column_name, absl::string_view index_name);
zetasql_base::Status ConcurrentSchemaChangeOrReadWriteTxnInProgress();

// Schema access errors.
zetasql_base::Status TableNotFound(absl::string_view table_name);
zetasql_base::Status TableNotFoundAtTimestamp(absl::string_view table_name,
                                      absl::Time timestamp);
zetasql_base::Status IndexNotFound(absl::string_view index_name);
zetasql_base::Status ColumnNotFound(absl::string_view table_name,
                            absl::string_view column_name);
zetasql_base::Status ColumnNotFoundAtTimestamp(absl::string_view table_name,
                                       absl::string_view column_name,
                                       absl::Time timestamp);
zetasql_base::Status ColumnValueTypeMismatch(absl::string_view table_name,
                                     absl::string_view column_type,
                                     absl::string_view value_type);
zetasql_base::Status CannotParseKeyValue(absl::string_view table_name,
                                 absl::string_view column,
                                 absl::string_view column_type);
zetasql_base::Status MutationColumnAndValueSizeMismatch(int columns_size,
                                                int values_size);
zetasql_base::Status SchemaObjectAlreadyExists(absl::string_view schema_object,
                                       absl::string_view name);

// Commit timestamp errors.
zetasql_base::Status CommitTimestampInFuture(absl::Time timestamp);
zetasql_base::Status CannotReadPendingCommitTimestamp(absl::string_view table_name);
zetasql_base::Status CommitTimestampNotInFuture(absl::string_view column,
                                        absl::string_view key,
                                        absl::Time timestamp);
zetasql_base::Status PendingCommitTimestampAllOrNone(int64_t index_num);

// Time errors.
zetasql_base::Status InvalidTime(absl::string_view msg);

// Read argument errors.
zetasql_base::Status StalenessMustBeNonNegative();
zetasql_base::Status InvalidMinReadTimestamp(absl::Time min_read_timestamp);
zetasql_base::Status InvalidExactReadTimestamp(absl::Time exact_read_timestamp);
zetasql_base::Status StrongReadOptionShouldBeTrue();

// Constraint errors.
zetasql_base::Status RowAlreadyExists(absl::string_view table_name,
                              absl::string_view key);
zetasql_base::Status RowNotFound(absl::string_view table_name, absl::string_view key);
zetasql_base::Status ParentKeyNotFound(absl::string_view parent_table_name,
                               absl::string_view child_table_name,
                               absl::string_view key);
zetasql_base::Status ChildKeyExists(absl::string_view parent_table_name,
                            absl::string_view child_table_name,
                            absl::string_view key);
zetasql_base::Status NullValueForNotNullColumn(absl::string_view table_name,
                                       absl::string_view column_name);
zetasql_base::Status NullValueForNotNullColumn(absl::string_view table_name,
                                       absl::string_view column_name,
                                       absl::string_view key);
zetasql_base::Status InvalidColumnLength(absl::string_view table_name,
                                 absl::string_view column_name,
                                 int max_column_length);
zetasql_base::Status InvalidStringEncoding(absl::string_view table_name,
                                   absl::string_view column_name);
zetasql_base::Status UTF8StringColumn(absl::string_view column_name,
                              absl::string_view key);
zetasql_base::Status ValueExceedsLimit(absl::string_view column_name, int value_size,
                               int max_column_size);
zetasql_base::Status NonNullValueNotSpecifiedForInsert(absl::string_view table_name,
                                               absl::string_view column_name);

// Index errors.
zetasql_base::Status UniqueIndexConstraintViolation(absl::string_view index_name,
                                            absl::string_view key);

zetasql_base::Status UniqueIndexViolationOnIndexCreation(absl::string_view index_name,
                                                 absl::string_view key);

zetasql_base::Status IndexTableDoesNotMatchBaseTable(absl::string_view base_table,
                                             absl::string_view indexed_table,
                                             absl::string_view index);

zetasql_base::Status IndexNotFound(absl::string_view index, absl::string_view table);

zetasql_base::Status ColumnNotFoundInIndex(absl::string_view index,
                                   absl::string_view indexed_table,
                                   absl::string_view column);

// Query errors.
zetasql_base::Status UnimplementedUntypedParameterBinding(absl::string_view param_name);
zetasql_base::Status InvalidHint(absl::string_view hint_string);
zetasql_base::Status InvalidHintValue(absl::string_view hint_string,
                              absl::string_view value_string);
zetasql_base::Status EmulatorDoesNotSupportQueryPlans();
zetasql_base::Status InvalidStatementHintValue(absl::string_view hint_string,
                                       absl::string_view hint_value);
zetasql_base::Status MultipleValuesForSameHint(absl::string_view hint_string);
zetasql_base::Status InvalidHintForNode(absl::string_view hint_string,
                                absl::string_view supported_node);
zetasql_base::Status InvalidBatchDmlRequest();
zetasql_base::Status ExecuteBatchDmlOnlySupportsDmlStatements(int index,
                                                      absl::string_view query);
zetasql_base::Status DmlRequiresReadWriteTransaction(
    absl::string_view transaction_type);

}  // namespace error
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_ERRORS_H_
