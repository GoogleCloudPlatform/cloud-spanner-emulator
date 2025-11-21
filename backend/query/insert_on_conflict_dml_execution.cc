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

#include "backend/query/insert_on_conflict_dml_execution.h"

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/datamodel/key.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "backend/query/query_engine_util.h"
#include "backend/schema/catalog/table.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

constexpr char kReservedIndexNameForPrimaryKey[] = "PRIMARY_KEY";

namespace {
using InsertRow = std::vector<zetasql::Value>;
using zetasql::ResolvedColumnRef;
using zetasql::ResolvedDMLValue;
using zetasql::ResolvedInsertStmt;
using zetasql::ResolvedOnConflictClauseEnums;
}  // namespace

absl::Status InsertOnConflictDoUpdateRewriter::VisitResolvedColumnRef(
    const ResolvedColumnRef* node) {
  // If the column reference is not from the insert row i.e. it is not of the
  // form `excluded.<column_name>` then no rewrite is needed.
  if (!column_ids_referenced_from_insert_row_.contains(
          node->column().column_id())) {
    return CopyVisitResolvedColumnRef(node);
  }

  std::pair<const zetasql::Type*, int> column_info =
      column_ids_referenced_from_insert_row_.at(node->column().column_id());
  auto array_element_column_ref = zetasql::MakeResolvedColumnRef(
      struct_column_holder_->type(), *struct_column_holder_, false);
  // Build RESOLVED_GET_STRUCT_FIELD to extract the column value from the
  // source STRUCT column.
  auto get_struct_field = zetasql::MakeResolvedGetStructField(
      column_info.first, std::move(array_element_column_ref),
      column_info.second);
  PushNodeToStack(std::move(get_struct_field));
  return absl::OkStatus();
}

absl::Status InsertOnConflictToInsertOrIgnoreRewriter::VisitResolvedInsertStmt(
    const ResolvedInsertStmt* node) {
  ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedInsertStmt(node));
  zetasql::ResolvedInsertStmt* insert_stmt =
      GetUnownedTopOfStack<zetasql::ResolvedInsertStmt>();
  insert_stmt->set_on_conflict_clause(nullptr);
  insert_stmt->set_insert_mode(ResolvedInsertStmt::OR_IGNORE);
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<const zetasql::ResolvedInsertStmt>>
BuildInsertOrIgnoreStmtFromInsertOnConflictStmt(
    const ResolvedInsertStmt* insert_on_conflict_stmt) {
  ZETASQL_RET_CHECK_NE(insert_on_conflict_stmt->on_conflict_clause(), nullptr);
  InsertOnConflictToInsertOrIgnoreRewriter rewriter;
  ZETASQL_RETURN_IF_ERROR(insert_on_conflict_stmt->Accept(&rewriter));
  return rewriter.ConsumeRootNode<zetasql::ResolvedInsertStmt>();
}

absl::Status ExtractColumnInfoAndRowsForInsertOrUpdateAction(
    const zetasql::ResolvedInsertStmt* insert_stmt,
    const std::vector<std::string>& conflict_target_key_columns,
    const ExecuteUpdateResult& all_insert_rows_in_dml,
    const std::set<Key>& original_table_row_keys,
    std::map<Key, InsertRow>* insert_row_map, std::vector<Key>* existing_rows,
    std::vector<Key>* new_rows, std::vector<std::string>* columns_in_mutation,
    absl::flat_hash_set<int>* insert_column_index_in_mutation_for_insert,
    absl::flat_hash_set<int>* insert_column_index_in_mutation_for_update) {
  ZETASQL_RET_CHECK_NE(insert_stmt, nullptr);
  ZETASQL_RET_CHECK(!conflict_target_key_columns.empty());
  ZETASQL_RET_CHECK_EQ(all_insert_rows_in_dml.mutation.ops().size(), 1);
  ZETASQL_RET_CHECK(all_insert_rows_in_dml.mutation.ops().at(0).type ==
            MutationOpType::kInsert);
  const MutationOp& mutation_op = all_insert_rows_in_dml.mutation.ops().at(0);

  ZETASQL_RET_CHECK_NE(insert_row_map, nullptr);
  ZETASQL_RET_CHECK_NE(existing_rows, nullptr);
  ZETASQL_RET_CHECK_NE(new_rows, nullptr);
  ZETASQL_RET_CHECK_NE(columns_in_mutation, nullptr);
  ZETASQL_RET_CHECK_NE(insert_column_index_in_mutation_for_insert, nullptr);
  ZETASQL_RET_CHECK_NE(insert_column_index_in_mutation_for_update, nullptr);

  absl::flat_hash_set<std::string> insert_column_list_for_insert;
  for (const auto& column : insert_stmt->insert_column_list()) {
    insert_column_list_for_insert.insert(column.name());
  }

  absl::flat_hash_set<std::string> insert_row_column_list_for_update;
  for (const auto& column : insert_stmt->table_scan()->column_list()) {
    insert_row_column_list_for_update.insert(column.name());
  }

  absl::flat_hash_map<std::string, int> key_column_index_in_mutation;
  for (int i = 0; i < mutation_op.columns.size(); ++i) {
    std::string column = mutation_op.columns[i];
    columns_in_mutation->push_back(column);
    if (insert_column_list_for_insert.contains(column)) {
      insert_column_index_in_mutation_for_insert->insert(i);
    }
    if (insert_row_column_list_for_update.contains(column)) {
      insert_column_index_in_mutation_for_update->insert(i);
    }
    if (absl::c_find(conflict_target_key_columns, column) ==
        conflict_target_key_columns.end()) {
      continue;
    }
    key_column_index_in_mutation[column] = i;
  }

  std::set<Key> seen_keys;
  bool has_duplicate_keys = false;
  for (const auto& insert_row : mutation_op.rows) {
    Key insert_row_key;
    for (const auto& key_column : conflict_target_key_columns) {
      insert_row_key.AddColumn(
          insert_row.at(key_column_index_in_mutation.at(key_column)));
    }
    // We do not allow duplicate insert rows in INSERT ON CONFLICT DO
    // UPDATE.
    if (insert_stmt->on_conflict_clause()->conflict_action() ==
            ResolvedOnConflictClauseEnums::UPDATE &&
        seen_keys.find(insert_row_key) != seen_keys.end()) {
      return error::CannotInsertDuplicateKeyInsertOrUpdateDml(
          insert_row_key.DebugString());
    }
    if (insert_row_map->find(insert_row_key) == insert_row_map->end()) {
      insert_row_map->insert({insert_row_key, insert_row});
    } else {
      has_duplicate_keys = true;
    }

    if (original_table_row_keys.find(insert_row_key) !=
        original_table_row_keys.end()) {
      existing_rows->push_back(insert_row_key);
    } else {
      // Returning error on duplicate keys in ON CONFLICT DO UPDATE is handled
      // above.
      // Skip duplicate keys in ON CONFLICT DO NOTHING as allowed by the
      // semantics
      if (seen_keys.find(insert_row_key) != seen_keys.end()) {
        continue;
      }
      new_rows->push_back(insert_row_key);
    }
    seen_keys.insert(insert_row_key);
  }

  if (!has_duplicate_keys) {
    ZETASQL_RET_CHECK_EQ(existing_rows->size() + new_rows->size(),
                 insert_row_map->size());
    if (insert_stmt->query() == nullptr) {
      ZETASQL_RET_CHECK_EQ(insert_row_map->size(),
                   all_insert_rows_in_dml.modify_row_count);
    }
  }
  return absl::OkStatus();
}

bool UniqueConstraintMatchesWithConflictTargetColumns(
    const absl::flat_hash_set<std::string>& pk_or_index_constraint_columns,
    const zetasql::ResolvedColumnList& ast_conflict_target_column_list) {
  absl::flat_hash_set<std::string> matched_conflict_target_columns;
  for (const zetasql::ResolvedColumn& column :
       ast_conflict_target_column_list) {
    if (pk_or_index_constraint_columns.contains(column.name())) {
      matched_conflict_target_columns.insert(column.name());
    } else {
      // This column is not in the unique constraint, this is not our match.
      // Return.
      return false;
    }
  }

  return (matched_conflict_target_columns.size() ==
          pk_or_index_constraint_columns.size());
}

absl::Status ResolveConflictTarget(
    const Table* table,
    const zetasql::ResolvedOnConflictClause* on_conflict_clause,
    std::vector<std::string>& conflict_target_key_columns,
    std::string* conflict_target_unique_constraint_name) {
  // Empty conflict target is not supported.
  if (on_conflict_clause->conflict_target_column_list_size() == 0 &&
      on_conflict_clause->unique_constraint_name().empty()) {
    return error::UnsupportedUpsertQueries(
        "ON CONFLICT clause with empty conflict target in INSERT");
  }

  if (!on_conflict_clause->unique_constraint_name().empty()) {
    const Index* index =
        table->FindIndex(on_conflict_clause->unique_constraint_name());
    if (index == nullptr) {
      return error::IndexNotFound(on_conflict_clause->unique_constraint_name(),
                                  table->Name());
    }
    if (!index->is_unique() || index->is_vector_index() ||
        index->is_search_index()) {
      return error::UnsupportedUpsertQueries(
          "ON CONFLICT clause with non-unique index in INSERT");
    }
    // Found the unique index.
    *conflict_target_unique_constraint_name = index->Name();
    for (const auto& key : index->key_columns()) {
      conflict_target_key_columns.push_back(key->column()->Name());
    }
    return absl::OkStatus();
  }

  // 1. Check if the conflict target is the primary key.
  absl::flat_hash_set<std::string> pk_or_index_constraint_columns;
  for (int i = 0; i < table->primary_key().size(); i++) {
    pk_or_index_constraint_columns.insert(
        table->primary_key()[i]->column()->Name());
  }
  if (UniqueConstraintMatchesWithConflictTargetColumns(
          pk_or_index_constraint_columns,
          on_conflict_clause->conflict_target_column_list())) {
    // Conflict target is PK.
    *conflict_target_unique_constraint_name = kReservedIndexNameForPrimaryKey;
    for (const auto& column : table->primary_key()) {
      conflict_target_key_columns.push_back(column->column()->Name());
    }
    return absl::OkStatus();
  }

  // 2. Conflict target columns still not matched. Check if the conflict
  // target is a unique index.
  for (const auto& index : table->indexes()) {
    if (!index->is_unique() || index->is_vector_index() ||
        index->is_search_index()) {
      continue;
    }
    pk_or_index_constraint_columns.clear();
    for (int i = 0; i < index->key_columns().size(); ++i) {
      pk_or_index_constraint_columns.insert(
          index->key_columns()[i]->column()->Name());
    }
    if (UniqueConstraintMatchesWithConflictTargetColumns(
            pk_or_index_constraint_columns,
            on_conflict_clause->conflict_target_column_list())) {
      if (index->is_null_filtered() ||
          !index->null_filtered_columns().empty()) {
        return error::NullFilteredIndexAsConflictTargetIsNotFound(
            index->Name());
      }
      // Set the matched UNIQUE index as the conflict target.
      *conflict_target_unique_constraint_name = index->Name();
      for (const auto& index_key : index->key_columns()) {
        conflict_target_key_columns.push_back(index_key->column()->Name());
      }
      break;
    }
  }
  if ((*conflict_target_unique_constraint_name).empty()) {
    return error::ConflictTargetNotFound();
  }
  ZETASQL_RET_CHECK(!conflict_target_key_columns.empty());
  ZETASQL_RET_CHECK(!conflict_target_unique_constraint_name->empty());
  return absl::OkStatus();
}

absl::StatusOr<std::set<Key>> GetOriginalTableRows(
    const Table* table, const std::string& unique_constraint_name,
    const std::vector<std::string>& key_columns, RowReader* reader) {
  std::set<Key> original_table_row_keys;
  ReadArg read_arg;
  read_arg.table = table->Name();
  if (unique_constraint_name != kReservedIndexNameForPrimaryKey) {
    read_arg.index = unique_constraint_name;
  }
  read_arg.key_set = KeySet::All();
  read_arg.columns = key_columns;
  std::unique_ptr<RowCursor> cursor;
  ZETASQL_RETURN_IF_ERROR(reader->Read(read_arg, &cursor));
  ZETASQL_RET_CHECK(cursor->Status().ok());
  while (cursor->Next()) {
    Key row_key;
    for (int i = 0; i < cursor->NumColumns(); ++i) {
      row_key.AddColumn(cursor->ColumnValue(i));
    }
    original_table_row_keys.insert(row_key);
  }
  return original_table_row_keys;
}

inline absl::StatusOr<std::unique_ptr<const zetasql::ResolvedTableScan>>
CopyTableScanAST(const zetasql::ResolvedTableScan* table_scan) {
  std::vector<zetasql::ResolvedColumn> column_list;
  for (int i = 0; i < table_scan->column_list().size(); ++i) {
    zetasql::ResolvedColumn elem = table_scan->column_list()[i];
    column_list.push_back(elem);
  }
  auto table_scan_copy = MakeResolvedTableScan(column_list, table_scan->table(),
                                               nullptr, table_scan->alias());

  table_scan_copy->set_column_index_list(table_scan->column_index_list());
  return std::move(table_scan_copy);
}

inline absl::StatusOr<std::unique_ptr<const zetasql::ResolvedReturningClause>>
CopyReturningClause(const zetasql::ResolvedInsertStmt* insert_stmt) {
  if (insert_stmt->returning() == nullptr) {
    return nullptr;
  }
  // `excluded` alias is not allowed in returning clause. Hence,
  // `column_ids_referenced_from_insert_row` is empty.
  InsertOnConflictDoUpdateRewriter rewriter({}, nullptr);
  ZETASQL_RETURN_IF_ERROR(insert_stmt->returning()->Accept(&rewriter));
  return rewriter.ConsumeRootNode<zetasql::ResolvedReturningClause>();
}

inline absl::Status CopyGeneratedColumnExprs(
    const zetasql::ResolvedInsertStmt* insert_stmt,
    std::vector<std::unique_ptr<const zetasql::ResolvedExpr>>&
        generated_column_exprs) {
  // Columns in `excluded` alias is not referenced in generated column exprs.
  // Hence, `column_ids_referenced_from_insert_row` is empty.
  InsertOnConflictDoUpdateRewriter rewriter({}, nullptr);
  for (const auto& expr : insert_stmt->generated_column_expr_list()) {
    ZETASQL_RETURN_IF_ERROR(expr->Accept(&rewriter));
    ZETASQL_ASSIGN_OR_RETURN(auto expr_copy,
                     rewriter.ConsumeRootNode<zetasql::ResolvedExpr>());
    generated_column_exprs.push_back(std::move(expr_copy));
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<const zetasql::ResolvedInsertStmt>>
BuildInsertDMLForNewRows(
    const zetasql::ResolvedInsertStmt* insert_stmt,
    const std::vector<Key>& new_rows,
    const std::map<Key, InsertRow>& insert_rows_map,
    const std::vector<std::string>& mutation_column_names,
    const absl::flat_hash_set<int>& insert_column_index_in_row) {
  ZETASQL_RET_CHECK(insert_stmt->on_conflict_clause() != nullptr);

  // Copy table scan for new insert statement.
  ZETASQL_ASSIGN_OR_RETURN(auto table_scan_copy,
                   CopyTableScanAST(insert_stmt->table_scan()));

  absl::flat_hash_map<std::string, zetasql::ResolvedColumn>
      insert_column_name_to_type_map;
  for (const auto& column : insert_stmt->insert_column_list()) {
    insert_column_name_to_type_map[column.name()] = column;
  }

  // Build insert row list for the new INSERT statement from `new_rows`.
  std::vector<std::unique_ptr<const zetasql::ResolvedInsertRow>>
      new_insert_row_list;
  for (const auto& new_row : new_rows) {
    ZETASQL_RET_CHECK(insert_rows_map.find(new_row) != insert_rows_map.end());
    InsertRow insert_row = insert_rows_map.at(new_row);
    std::vector<std::unique_ptr<const zetasql::ResolvedDMLValue>> row_values;
    for (int i = 0; i < insert_row.size(); ++i) {
      // Skip columns from the input row that are not part of
      // insert column list.
      if (!insert_column_index_in_row.contains(i)) {
        continue;
      }
      std::string insert_column_name = mutation_column_names[i];
      ZETASQL_RET_CHECK(insert_column_name_to_type_map.contains(insert_column_name));
      row_values.push_back(
          zetasql::MakeResolvedDMLValue(zetasql::MakeResolvedLiteral(
              insert_column_name_to_type_map[insert_column_name].type(),
              insert_row[i])));
    }
    new_insert_row_list.push_back(
        zetasql::MakeResolvedInsertRow(std::move(row_values)));
  }

  // Copy returning clause and generated column exprs in the new insert
  // statement.
  ZETASQL_ASSIGN_OR_RETURN(auto returning_clause, CopyReturningClause(insert_stmt));
  std::vector<std::unique_ptr<const zetasql::ResolvedExpr>>
      generated_column_exprs;
  ZETASQL_RETURN_IF_ERROR(
      CopyGeneratedColumnExprs(insert_stmt, generated_column_exprs));

  auto insert_stmt_copy = zetasql::MakeResolvedInsertStmt(
      std::move(table_scan_copy), insert_stmt->insert_mode(),
      /*assert_rows_modified=*/nullptr,
      /*returning=*/std::move(returning_clause),
      insert_stmt->insert_column_list(), {},
      /*query=*/nullptr,
      /*query_output=*/{}, std::move(new_insert_row_list),
      /*on_conflict_clause=*/nullptr,
      insert_stmt->topologically_sorted_generated_column_id_list(),
      std::move(generated_column_exprs));
  std::vector<zetasql::ResolvedStatement::ObjectAccess> object_access(
      insert_stmt->table_scan()->column_list().size(),
      zetasql::ResolvedStatement::READ_WRITE);
  insert_stmt_copy->set_column_access_list(object_access);

  ZETASQL_VLOG(1) << "Executed INSERT statement: " << insert_stmt_copy->DebugString();
  return std::move(insert_stmt_copy);
}

absl::StatusOr<std::unique_ptr<const zetasql::ResolvedArrayScan>>
BuildFromArrayScanClause(
    zetasql::TypeFactory& type_factory, zetasql::Catalog& catalog,
    const zetasql::ResolvedInsertStmt* insert_stmt,
    const std::vector<Key>& existing_rows,
    const std::map<Key, InsertRow>& insert_rows_map,
    const std::vector<std::string>& mutation_column_names,
    const absl::flat_hash_set<int>& from_array_scan_column_index_in_row,
    absl::flat_hash_map<std::string, std::pair<const zetasql::Type*, int>>&
        struct_field_info_by_name) {
  absl::flat_hash_map<std::string, zetasql::ResolvedColumn>
      table_scan_column_name_to_type_map;
  for (const auto& column : insert_stmt->table_scan()->column_list()) {
    table_scan_column_name_to_type_map[column.name()] = column;
  }

  // All table scan columns are included in the struct.
  std::vector<zetasql::StructType::StructField> struct_fields;
  for (const auto& column : insert_stmt->table_scan()->column_list()) {
    struct_fields.push_back(
        zetasql::StructType::StructField(column.name(), column.type()));
    struct_field_info_by_name[column.name()] =
        std::make_pair(column.type(), struct_fields.size() - 1);
  }
  const zetasql::StructType* struct_type;
  ZETASQL_RETURN_IF_ERROR(type_factory.MakeStructType(struct_fields, &struct_type));
  zetasql::AnalyzerOptions analyzer_options;
  zetasql::FunctionCallBuilder function_builder(analyzer_options, catalog,
                                                  type_factory);

  // Construct STRUCT value for each row in `existing_rows`.
  std::vector<std::unique_ptr<const zetasql::ResolvedExpr>> array_elements;
  for (const auto& existing_row : existing_rows) {
    ZETASQL_RET_CHECK(insert_rows_map.find(existing_row) != insert_rows_map.end());
    InsertRow insert_row = insert_rows_map.at(existing_row);
    std::vector<std::unique_ptr<const zetasql::ResolvedExpr>>
        field_value_exprs;
    for (int i = 0; i < insert_row.size(); ++i) {
      if (!from_array_scan_column_index_in_row.contains(i)) {
        continue;
      }
      std::string column_name = mutation_column_names[i];
      ZETASQL_RET_CHECK(table_scan_column_name_to_type_map.contains(column_name));
      field_value_exprs.push_back(zetasql::MakeResolvedLiteral(
          table_scan_column_name_to_type_map[column_name].type(),
          insert_row[i]));
    }
    array_elements.push_back(zetasql::MakeResolvedMakeStruct(
        struct_type, std::move(field_value_exprs)));
  }

  // Construct ARRAY<STRUCT> value with each row in `existing_rows` as the array
  // element.
  const zetasql::Type* array_of_structs_type = nullptr;
  ZETASQL_RETURN_IF_ERROR(
      type_factory.MakeArrayType(struct_type, &array_of_structs_type));
  ZETASQL_ASSIGN_OR_RETURN(
      auto array_of_structs_expr,
      function_builder.MakeArray(struct_type, std::move(array_elements)));

  // Construct array element column holder.
  // Emulator does not maintain column_id allocator. Choose an arbitrary large
  // value to avoid collision with existing column ids.
  int array_element_column_id = 100000;
  auto array_element_column = zetasql::ResolvedColumn(
      array_element_column_id, zetasql::IdString::MakeGlobal("$array"),
      zetasql::IdString::MakeGlobal("excluded"), array_of_structs_type);

  auto element_column = zetasql::ResolvedColumn(
      array_element_column_id, zetasql::IdString::MakeGlobal("$array"),
      zetasql::IdString::MakeGlobal("excluded"), struct_type);

  return zetasql::MakeResolvedArrayScan(
      {array_element_column}, /*input_scan=*/nullptr,
      /*array_expr=*/std::move(array_of_structs_expr), element_column,
      /*array_offset_column=*/nullptr, /*join_expr=*/nullptr,
      /*is_outer=*/false);
}

absl::StatusOr<std::unique_ptr<const zetasql::ResolvedUpdateStmt>>
BuildUpdateDMLForExistingRows(
    const zetasql::ResolvedInsertStmt* insert_stmt,
    zetasql::TypeFactory& type_factory, zetasql::Catalog& catalog,
    const std::vector<Key>& existing_rows,
    const std::map<Key, InsertRow>& insert_rows_map,
    const std::vector<std::string>& mutation_column_names,
    const absl::flat_hash_set<int>& insert_column_index_in_row,
    const std::vector<std::string>& conflict_target_key_columns) {
  ZETASQL_RET_CHECK(insert_stmt->on_conflict_clause() != nullptr);

  // Build FROM clause of the output UPDATE statement.
  // This is a ResolvedArrayScan node of the form:
  // FROM UNNEST([STRUCT(col_val1 as col_name1, col_val2 as col_name2, ...),
  //              ...]) as alias
  // The struct must include all table scan columns in the original INSERT
  // statement.
  absl::flat_hash_map<std::string, std::pair<const zetasql::Type*, int>>
      struct_field_info_by_name;
  ZETASQL_ASSIGN_OR_RETURN(auto from_array_scan,
                   BuildFromArrayScanClause(
                       type_factory, catalog, insert_stmt, existing_rows,
                       insert_rows_map, mutation_column_names,
                       insert_column_index_in_row, struct_field_info_by_name));
  ZETASQL_RET_CHECK(from_array_scan != nullptr);
  ZETASQL_RET_CHECK_EQ(from_array_scan->element_column_list_size(), 1);
  const zetasql::ResolvedColumn& array_element_column =
      from_array_scan->element_column_list(0);

  // Map of column id referenced via excluded alias (i.e reference value from
  // the insert row of original INSERT ON CONFLICT statement) in the DO UPDATE
  // clause.
  // Used in InsertOnConflictDoUpdateRewriter to replace ColumnRef
  // `excluded.column_name` in SET and WHERE expression with corresponding
  // column expression from FROM clause.
  absl::flat_hash_map<int, std::pair<const zetasql::Type*, int>>
      excluded_alias_column_ids;
  if (insert_stmt->on_conflict_clause()->insert_row_scan() != nullptr) {
    for (const auto& column :
         insert_stmt->on_conflict_clause()->insert_row_scan()->column_list()) {
      excluded_alias_column_ids[column.column_id()] =
          struct_field_info_by_name[column.name()];
    }
  }

  // Build WHERE clause for the output UPDATE statement.
  absl::flat_hash_map<std::string, zetasql::ResolvedColumn>
      table_scan_column_name_to_type_map;
  for (const auto& column : insert_stmt->table_scan()->column_list()) {
    table_scan_column_name_to_type_map[column.name()] = column;
  }

  zetasql::AnalyzerOptions analyzer_options;
  zetasql::FunctionCallBuilder function_builder(analyzer_options, catalog,
                                                  type_factory);
  // Includes join condition between FROM and table scan on conflict target
  // key columns.
  // Optionally AND's with the condition in ON CONFLICT DO UPDATE WHERE
  // clause if present.
  std::vector<std::unique_ptr<const zetasql::ResolvedExpr>>
      where_expr_predicates;
  for (const auto& key_column_name : conflict_target_key_columns) {
    // Table scan must have the key column.
    ZETASQL_RET_CHECK(table_scan_column_name_to_type_map.contains(key_column_name));
    std::pair<const zetasql::Type*, int> excluded_column_info =
        struct_field_info_by_name[key_column_name];

    auto array_element_column_ref = zetasql::MakeResolvedColumnRef(
        array_element_column.type(), array_element_column, false);
    auto get_struct_field = zetasql::MakeResolvedGetStructField(
        excluded_column_info.first, std::move(array_element_column_ref),
        excluded_column_info.second);
    ZETASQL_ASSIGN_OR_RETURN(
        auto predicate,
        function_builder.Equal(
            zetasql::MakeResolvedColumnRef(
                table_scan_column_name_to_type_map[key_column_name].type(),
                table_scan_column_name_to_type_map[key_column_name], false),
            std::move(get_struct_field)));
    ZETASQL_VLOG(1) << "Key join predicate: " << predicate->DebugString();
    where_expr_predicates.push_back(std::move(predicate));
  }

  // If ON CONFLICT DO UPDATE WHERE is present, add the condition to the
  // predicate list.
  if (insert_stmt->on_conflict_clause()->update_where_expression() != nullptr) {
    InsertOnConflictDoUpdateRewriter rewriter(excluded_alias_column_ids,
                                              &array_element_column);
    ZETASQL_RETURN_IF_ERROR(
        insert_stmt->on_conflict_clause()->update_where_expression()->Accept(
            &rewriter));
    ZETASQL_ASSIGN_OR_RETURN(auto rewritten_where_expr,
                     rewriter.ConsumeRootNode<zetasql::ResolvedExpr>());
    where_expr_predicates.push_back(std::move(rewritten_where_expr));
  }

  std::unique_ptr<const zetasql::ResolvedExpr> update_where_expr;
  if (where_expr_predicates.size() > 1) {
    ZETASQL_ASSIGN_OR_RETURN(update_where_expr,
                     function_builder.And(std::move(where_expr_predicates)));
  } else {
    update_where_expr = std::move(where_expr_predicates[0]);
  }
  ZETASQL_VLOG(1) << "Output UPDATE WHERE expression: "
          << update_where_expr->DebugString();

  // Build SET clause for the output UPDATE statement from the DO UPDATE SET
  // clause in the original INSERT ON CONFLICT statement.
  std::vector<std::unique_ptr<const zetasql::ResolvedUpdateItem>>
      rewritten_update_item_list;
  for (const auto& update_item :
       insert_stmt->on_conflict_clause()->update_item_list()) {
    InsertOnConflictDoUpdateRewriter rewriter(excluded_alias_column_ids,
                                              &array_element_column);
    ZETASQL_RETURN_IF_ERROR(update_item->Accept(&rewriter));
    ZETASQL_ASSIGN_OR_RETURN(auto new_update_item,
                     rewriter.ConsumeRootNode<zetasql::ResolvedUpdateItem>());
    ZETASQL_VLOG(1) << "Output UPDATE SET item: " << new_update_item->DebugString();
    rewritten_update_item_list.push_back(std::move(new_update_item));
  }

  ZETASQL_ASSIGN_OR_RETURN(auto table_scan_copy,
                   CopyTableScanAST(insert_stmt->table_scan()));

  // Copy returning clause and generated column exprs in the update
  // statement.
  ZETASQL_ASSIGN_OR_RETURN(auto returning_clause, CopyReturningClause(insert_stmt));
  std::vector<std::unique_ptr<const zetasql::ResolvedExpr>>
      generated_column_exprs;
  ZETASQL_RETURN_IF_ERROR(
      CopyGeneratedColumnExprs(insert_stmt, generated_column_exprs));

  auto update_stmt = zetasql::MakeResolvedUpdateStmt(
      std::move(table_scan_copy), /*assert_rows_modified=*/nullptr,
      /*returning=*/std::move(returning_clause),
      /*array_offset_column=*/nullptr, std::move(update_where_expr),
      std::move(rewritten_update_item_list), std::move(from_array_scan),
      insert_stmt->topologically_sorted_generated_column_id_list(),
      std::move(generated_column_exprs));
  std::vector<zetasql::ResolvedStatement::ObjectAccess> object_access(
      update_stmt->table_scan()->column_list().size(),
      zetasql::ResolvedStatement::READ_WRITE);
  update_stmt->set_column_access_list(object_access);

  return std::move(update_stmt);
}

absl::Status CollectReturningRows(
    std::unique_ptr<RowCursor> returning_row_cursor,
    InsertOnConflictReturningRows& returning_rows) {
  if (returning_row_cursor == nullptr) {
    return absl::OkStatus();
  }

  while (returning_row_cursor->Next()) {
    // Collect the names of the columns in the returning row cursor.
    bool collect_column_metadata = returning_rows.column_names.empty();
    std::vector<zetasql::Value> row;
    row.reserve(returning_row_cursor->NumColumns());
    for (int i = 0; i < returning_row_cursor->NumColumns(); ++i) {
      if (collect_column_metadata) {
        returning_rows.column_names.push_back(
            returning_row_cursor->ColumnName(i));
        returning_rows.column_types.push_back(
            returning_row_cursor->ColumnType(i));
      }
      row.push_back(returning_row_cursor->ColumnValue(i));
    }
    returning_rows.rows.push_back(row);
  }
  return absl::OkStatus();
}

absl::StatusOr<bool> CanTranslateToInsertOrUpdateMode(
    const zetasql::ResolvedInsertStmt& insert_stmt) {
  const zetasql::ResolvedOnConflictClause* on_conflict_clause =
      insert_stmt.on_conflict_clause();
  ZETASQL_RET_CHECK_NE(on_conflict_clause, nullptr);
  ZETASQL_RET_CHECK_NE(on_conflict_clause->conflict_action(),
               ResolvedOnConflictClauseEnums::NOTHING);

  // Rules for translating ON CONFLICT DO UPDATE to INSERT OR UPDATE statement.
  // 1. Conflict target must be a primary key.
  // 2. Update should not be conditional ie. DO UPDATE should not have a WHERE
  //    clause.
  // 3. SET clause has all columns that are in the INSERT column list including
  //    primary key columns.
  // 4. SET expression is same as the insert row value and set using
  //    `excluded`.<column_name> expression.
  //
  // E.g.
  //   INSERT INTO T(key, val) VALUES (1, 2)
  //   ON CONFLICT(key) DO UPDATE SET key = excluded.key, val = excluded.val;
  // is equivalent to
  //   INSERT OR UPDATE T(key, val) VALUES (1, 2);
  //
  // Few examples of queries that are not INSERT OR UPDATE equivalent.
  //   1. Columns are not updated to their insert row value.
  //   INSERT INTO T(key, val) VALUES (1, 2)
  //   ON CONFLICT(key) DO UPDATE SET val = excluded.val + 1,
  //                                  key = excluded.key;
  // or
  //   2. Update is conditional.
  //   INSERT INTO T(key, val) VALUES (1, 2)
  //   ON CONFLICT(key) DO UPDATE SET key = excluded.key, val = excluded.val
  //   WHERE val > 0;
  // or
  //   3. Subset of insert columns are updated.
  //   INSERT INTO T(key, val, stringval) VALUES (1, 2, '2')
  //   ON CONFLICT(key) DO UPDATE SET key = excluded.key, val = excluded.val;

  if (on_conflict_clause->update_where_expression() != nullptr) {
    return false;
  }

  // Conflict target is a UNIQUE index.
  if (!on_conflict_clause->unique_constraint_name().empty()) {
    return false;
  }

  // All INSERT columns are not updated.
  if (insert_stmt.insert_column_list_size() !=
      on_conflict_clause->update_item_list().size()) {
    return false;
  }

  // `insert_row_scan` has columns from the insert row that were referenced
  // in SET...WHERE clause using the `excluded` alias.
  //
  // No insert row columns were referenced.
  if (on_conflict_clause->insert_row_scan() == nullptr) {
    return false;
  }

  // Set of INSERT columns for fast lookup.
  absl::flat_hash_set<std::string> insert_columns;
  for (const auto& insert_column : insert_stmt.insert_column_list()) {
    insert_columns.insert(insert_column.name());
  }
  // Map of referenced columns expression.
  absl::flat_hash_map<std::string, const zetasql::ResolvedColumn*>
      insert_row_column_name_to_column;
  for (const auto& insert_tuple_column :
       on_conflict_clause->insert_row_scan()->column_list()) {
    insert_row_column_name_to_column[insert_tuple_column.name()] =
        &insert_tuple_column;
  }

  // Return TRUE only if update target columns are set to its column value from
  // the insert row.
  for (const auto& update_item : on_conflict_clause->update_item_list()) {
    // Nested updates are disallowed.
    ZETASQL_RET_CHECK(update_item->element_column() == nullptr);
    // Target may be a proto update.
    if (!update_item->target()->Is<ResolvedColumnRef>()) {
      return false;
    }

    // Update column list is not in the insert column list.
    const zetasql::ResolvedColumn& update_column =
        update_item->target()->GetAs<ResolvedColumnRef>()->column();
    if (!insert_columns.contains(update_column.name())) {
      return false;
    }

    // Update column is not read from the insert row in SET clause. Such
    // columns will then not be in `on_conflict_clause->insert_row_scan()`.
    if (!insert_row_column_name_to_column.contains(update_column.name())) {
      return false;
    }

    // SET RHS value is not a column reference.
    if (!update_item->set_value()->Is<ResolvedDMLValue>() ||
        !update_item->set_value()
             ->GetAs<ResolvedDMLValue>()
             ->value()
             ->Is<ResolvedColumnRef>()) {
      return false;
    }

    // Column is not updated its own column value from the insert row.
    if (update_item->set_value()
            ->GetAs<ResolvedDMLValue>()
            ->value()
            ->GetAs<ResolvedColumnRef>()
            ->column() !=
        *insert_row_column_name_to_column[update_column.name()]) {
      return false;
    }
  }
  return true;
}

absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>>
AnalyzeAsInsertOrUpdateDML(const std::string& sql, Catalog* catalog,
                           zetasql::AnalyzerOptions& analyzer_options,
                           zetasql::TypeFactory* type_factory,
                           const FunctionCatalog* function_catalog) {
  // Defer to OR_IGNORE translation of ON CONFLICT DO NOTHING DML in
  // PostgreSQL.
  analyzer_options.mutable_language()->DisableLanguageFeature(
      zetasql::FEATURE_INSERT_ON_CONFLICT_CLAUSE);
  auto analyzer_output = AnalyzePostgreSQL(sql, catalog, analyzer_options,
                                           type_factory, function_catalog);
  // Re-enable after the step for the rest of the execution.
  analyzer_options.mutable_language()->EnableLanguageFeature(
      zetasql::FEATURE_INSERT_ON_CONFLICT_CLAUSE);
  return analyzer_output;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
