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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INSERT_ON_CONFLICT_DML_EXECUTION_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INSERT_ON_CONFLICT_DML_EXECUTION_H_

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/access/read.h"
#include "backend/datamodel/key.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "backend/query/query_engine_util.h"
#include "backend/schema/catalog/table.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class InsertOnConflictDoUpdateRewriter
    : public zetasql::ResolvedASTDeepCopyVisitor {
 public:
  explicit InsertOnConflictDoUpdateRewriter(
      const absl::flat_hash_map<int, std::pair<const zetasql::Type*, int>>&
          column_ids_referenced_from_insert_row,
      const zetasql::ResolvedColumn* struct_column_holder)
      : column_ids_referenced_from_insert_row_(
            column_ids_referenced_from_insert_row),
        struct_column_holder_(struct_column_holder) {}

  absl::Status VisitResolvedColumnRef(
      const zetasql::ResolvedColumnRef* node) override;

 private:
  // Map of column id(s) of columns referenced from the insert row (i.e. of the
  // form `excluded.<column_name>`) to their Type and index in the
  // source STRUCT column.
  // Used to build the expression to extract the column value from the source
  // STRUCT column `struct_column_`.
  absl::flat_hash_map<int, std::pair<const zetasql::Type*, int>>
      column_ids_referenced_from_insert_row_;
  // Column containing the source STRUCT having the column values from the
  // insert row.
  const zetasql::ResolvedColumn* struct_column_holder_;
};

class InsertOnConflictToInsertOrIgnoreRewriter
    : public zetasql::ResolvedASTDeepCopyVisitor {
 public:
  absl::Status VisitResolvedInsertStmt(
      const zetasql::ResolvedInsertStmt* node) override;
};

// Resolves the conflict target from the ON CONFLICT clause.
// Populates the conflict target key columns (either primary key columns or
// unique index key columns) and the unique constraint name.
absl::Status ResolveConflictTarget(
    const Table* table,
    const zetasql::ResolvedOnConflictClause* on_conflict_clause,
    std::vector<std::string>& conflict_target_key_columns,
    std::string* conflict_target_unique_constraint_name);

// This method extracts the INSERT subsection from the input ON CONFLICT DML,
// and rewrites it into a INSERT OR IGNORE stmt AST.
// For example, if the input SQL is
// "INSERT INTO t1 (key, val) VALUES (1, 2) ON CONFLICT(c1) ...",
// then this function will return an AST equivalent to the SQL
// "INSERT OR IGNORE INTO t1 (key, val) VALUES (1, 2)"
absl::StatusOr<std::unique_ptr<const zetasql::ResolvedInsertStmt>>
BuildInsertOrIgnoreStmtFromInsertOnConflictStmt(
    const zetasql::ResolvedInsertStmt* insert_on_conflict_stmt);

// Given the insert statement and insert row column information, populate the
// column information required to build and execute the insert or update action
// in INSERT ON CONFLICT DML.
absl::Status ExtractColumnInfoAndRowsForInsertOrUpdateAction(
    const zetasql::ResolvedInsertStmt* insert_stmt,
    const std::vector<std::string>& conflict_target_key_columns,
    const ExecuteUpdateResult& all_insert_rows_in_dml,
    const std::set<Key>& original_table_row_keys,
    std::map<Key, std::vector<zetasql::Value>>* insert_row_map,
    std::vector<Key>* existing_rows, std::vector<Key>* new_rows,
    std::vector<std::string>* columns_in_mutation,
    absl::flat_hash_set<int>* insert_column_index_in_mutation_for_insert,
    absl::flat_hash_set<int>* insert_column_index_in_mutation_for_update);

// Returns the original table rows that are present in the table before the
// INSERT statement was executed.
absl::StatusOr<std::set<Key>> GetOriginalTableRows(
    const Table* table, const std::string& unique_constraint_name,
    const std::vector<std::string>& key_columns, RowReader* reader);

// Invoked in the workflow to execute INSERT...ON CONFLICT statement.
// Builds ResolvedInsertStmt AST for an insert statement to insert rows into the
// table, corresponding to query like:
// INSERT INTO table (<insert_column_list>)
// VALUES (<insert_row1>), (<insert_row2>)...
// The insert column list is same as the original INSERT statement `insert_stmt`
//
// `insert_stmt` is the original INSERT...ON CONFLICT statement.
//
// `new_rows` are the new rows that do not violate the conflict target and
// should be inserted in the table. Each element in `new_rows` is a Key of the
// conflict target.
// If conflict target is primary key, then the element is the table primary key.
// If conflict target is a UNIQUE index, then it is the unique index key.
//
// `insert_rows_map` is a map of _all_ the insert rows in the original INSERT
// statement keyed by conflict target key.
//
// `mutation_column_names` are names of the columns in an insert row.
// Note that it is super set of columns in the output INSERT statement.
// The specific indices that are part of output INSERT statement is specified by
// `insert_column_index_in_row`.
//
// `insert_column_index_in_row` are the indices in the insert row vector that
// should be included as insert column and insert row in the output INSERT
// statement i.e. in <insert_column_list> and <insert_rowN>
//
// Example:
// Table schema:
// CREATE TABLE t1 (c1 INT64, c2 INT64, c3 INT64 AS (c2*10) STORED);
// Original INSERT statement:
// INSERT INTO t1 (c1, c2) VALUES (1, 10), (2, 20), (3, 30)
// ON CONFLICT(c1) DO NOTHING;
//
// `new_rows`: c1: {{1}, {3}}
//
// `insert_rows_map`:
//   {1} -> {1, 10, 100}
//   {2} -> {2, 20, 200}
//   {3} -> {3, 30, 300}
//
// `mutation_column_names`: {c1, c2, c3}
//
// `insert_column_index_in_row`: {0, 1}
//
// Output INSERT statement:
// INSERT INTO t1 (c1, c2) VALUES (1, 10), (3, 30)
absl::StatusOr<std::unique_ptr<const zetasql::ResolvedInsertStmt>>
BuildInsertDMLForNewRows(
    const zetasql::ResolvedInsertStmt* insert_stmt,
    const std::vector<Key>& new_rows,
    const std::map<Key, std::vector<zetasql::Value>>& insert_rows_map,
    const std::vector<std::string>& mutation_column_names,
    const absl::flat_hash_set<int>& insert_column_index_in_row);

// Invoked in the workflow to execute INSERT...ON CONFLICT DO UPDATE statement.
// Builds ResolvedUpdateStmt AST for an update statement to update columns in
// the existing rows in the table, corresponding to query like:
// UPDATE table SET <update_column_list>
// FROM <table-valued-function>
// WHERE <update_where_expression>
//
// `insert_stmt` is the original INSERT...ON CONFLICT DO UPDATE statement.
//
// `existing_rows` are the existing rows that should be updated in the table.
// Each element in `existing_rows` is a Key of the conflict target.
// If conflict target is primary key, then the element is the table primary key.
// If conflict target is a UNIQUE index, then it is the unique index key.
//
// `insert_rows_map` is a map of _all_ the insert rows in the original INSERT
// statement keyed by conflict target key.
//
// `mutation_column_names` are names of the columns in an insert row.
// Note that it is super set of columns in the output INSERT statement.
// The specific indices that are part of output INSERT statement is specified by
// `insert_column_index_in_row`.
//
// `insert_column_index_in_row` are the indices in the insert row vector that
// should be included in the FROM clause of the UPDATE statement.
//
// `conflict_target_column_names` are names of the conflict target key columns.
// Used to build join condition in the UPDATE statement with the FROM clause.
//
// Example:
// Table schema:
// CREATE TABLE t1 (c1 INT64, c2 INT64, c3 INT64 AS (c2*10) STORED);
// Original INSERT statement:
// INSERT INTO t1 (c1, c2) VALUES (1, 10), (2, 20), (3, 30)
// ON CONFLICT(c1) DO UPDATE SET c2 = excluded.c2 + t1.c2
// WHERE c3 > excluded.c3;  // Note that c3 is not in the insert column list.
//
// `existing_rows`: c1: {{2}, {3}}
//
// `insert_rows_map`:
//   {1} -> {1, 10, 100}
//   {2} -> {2, 20, 200}
//   {3} -> {3, 30, 300}
//
// `mutation_column_names`: {c1, c2, c3}
//
// `insert_column_index_in_row`: {0, 1, 2}
//
// Output UPDATE statement:
// UPDATE t1 SET c2 = a.c2 + t1.c2
// FROM UNNEST([STRUCT(2 AS c1, 20 AS c2, 200 AS c3),
//              STRUCT(3 AS c1, 30 AS c2, 300 AS c3)]) as a
// WHERE t1.c1 = a.c1   // Join condition from conflict target key columns
//   AND t1.c3 > a.c3;  // condition from ON CONFLICT DO UPDATE WHERE clause
absl::StatusOr<std::unique_ptr<const zetasql::ResolvedUpdateStmt>>
BuildUpdateDMLForExistingRows(
    const zetasql::ResolvedInsertStmt* insert_stmt,
    zetasql::TypeFactory& type_factory, zetasql::Catalog& catalog,
    const std::vector<Key>& existing_rows,
    const std::map<Key, std::vector<zetasql::Value>>& insert_rows_map,
    const std::vector<std::string>& mutation_column_names,
    const absl::flat_hash_set<int>& insert_column_index_in_row,
    const std::vector<std::string>& conflict_target_key_columns);

absl::Status CollectReturningRows(
    std::unique_ptr<RowCursor> returning_row_cursor,
    InsertOnConflictReturningRows& returning_rows);

// This is required for backward compatibility in PostgreSQL dialect.
absl::StatusOr<bool> CanTranslateToInsertOrUpdateMode(
    const zetasql::ResolvedInsertStmt& insert_stmt);

// If INSERT ON CONFLICT DO UPDATE DML can be translated to INSERT OR UPDATE
// DML, analyze in the query into OR_UPDATE mode in PG forward transformer by
// disabling the feature FEATURE_INSERT_ON_CONFLICT_CLAUSE.
absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>>
AnalyzeAsInsertOrUpdateDML(const std::string& sql, Catalog* catalog,
                           zetasql::AnalyzerOptions& analyzer_options,
                           zetasql::TypeFactory* type_factory,
                           const FunctionCatalog* function_catalog);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INSERT_ON_CONFLICT_DML_EXECUTION_H_
