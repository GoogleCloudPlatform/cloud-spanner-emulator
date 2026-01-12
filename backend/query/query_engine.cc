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

#include "backend/query/query_engine.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_helpers.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/match.h"
#include "absl/strings/substitute.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/common/case.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_set.h"
#include "backend/datamodel/value.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/ann_functions_rewriter.h"
#include "backend/query/ann_validator.h"
#include "backend/query/catalog.h"
#include "backend/query/change_stream/change_stream_query_validator.h"
#include "backend/query/dml_query_validator.h"
#include "backend/query/feature_filter/query_size_limits_checker.h"
#include "backend/query/function_catalog.h"
#include "backend/query/hint_rewriter.h"
#include "backend/query/index_hint_validator.h"
#include "backend/query/insert_on_conflict_dml_execution.h"
#include "backend/query/partitionability_validator.h"
#include "backend/query/partitioned_dml_validator.h"
#include "backend/query/query_context.h"
#include "backend/query/query_engine_options.h"
#include "backend/query/query_engine_util.h"
#include "backend/query/query_validator.h"
#include "backend/query/queryable_column.h"
#include "backend/query/queryable_view.h"
#include "backend/schema/catalog/schema.h"
#include "backend/transaction/commit_timestamp.h"
#include "common/config.h"
#include "common/constants.h"
#include "common/errors.h"
#include "common/feature_flags.h"
#include "frontend/converters/values.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

// A RowCursor backed by vectors (one per each row) of values.
class VectorsRowCursor : public RowCursor {
 public:
  VectorsRowCursor(
      const std::vector<std::string> column_names,
      const std::vector<const zetasql::Type*> column_types,
      const std::vector<std::vector<zetasql::Value>> column_values)
      : column_names_(column_names),
        column_types_(column_types),
        column_values_(column_values) {
    assert(column_names_.size() == column_types_.size());
    for (int i = 0; i < column_values.size(); ++i) {
      assert(column_names_.size() == column_values[i].size());
    }
  }

  bool Next() override { return ++row_index_ < column_values_.size(); }

  absl::Status Status() const override { return absl::OkStatus(); }

  int NumColumns() const override { return column_names_.size(); }

  const std::string ColumnName(int i) const override {
    return column_names_[i];
  }

  const zetasql::Type* ColumnType(int i) const override {
    return column_types_[i];
  }

  const zetasql::Value ColumnValue(int i) const override {
    return column_values_[row_index_][i];
  }

 private:
  size_t row_index_ = -1;
  std::vector<std::string> column_names_;
  std::vector<const zetasql::Type*> column_types_;
  std::vector<std::vector<zetasql::Value>> column_values_;
};

zetasql::EvaluatorOptions CommonEvaluatorOptions(
    zetasql::TypeFactory* type_factory, const std::string time_zone,
    bool return_all_insert_rows_insert_ignore_dml = false) {
  zetasql::EvaluatorOptions options;
  options.type_factory = type_factory;
  absl::TimeZone time_zone_obj;
  absl::LoadTimeZone(time_zone, &time_zone_obj);
  options.default_time_zone = time_zone_obj;
  options.scramble_undefined_orderings = true;
  options.return_all_insert_rows_insert_ignore_dml =
      return_all_insert_rows_insert_ignore_dml;
  return options;
}

absl::StatusOr<zetasql::AnalyzerOptions> MakeAnalyzerOptionsWithParameters(
    const zetasql::ParameterValueMap& params, const std::string time_zone) {
  zetasql::AnalyzerOptions options = MakeGoogleSqlAnalyzerOptions(time_zone);
  for (const auto& [name, value] : params) {
    ZETASQL_RETURN_IF_ERROR(options.AddQueryParameter(name, value.type()));
  }
  // Enable SQL Graph language feature.
  options.mutable_language()->EnableLanguageFeature(
      zetasql::FEATURE_V_1_4_SQL_GRAPH);
  options.mutable_language()->EnableLanguageFeature(
      zetasql::FEATURE_V_1_4_SQL_GRAPH_ADVANCED_QUERY);
  options.mutable_language()->EnableLanguageFeature(
      zetasql::FEATURE_V_1_4_SQL_GRAPH_BOUNDED_PATH_QUANTIFICATION);
  options.mutable_language()->EnableLanguageFeature(
      zetasql::FEATURE_V_1_4_SQL_GRAPH_PATH_TYPE);
  options.mutable_language()->EnableLanguageFeature(
      zetasql::FEATURE_V_1_4_SQL_GRAPH_PATH_MODE);
  options.mutable_language()->EnableLanguageFeature(
      zetasql::FEATURE_V_1_4_SQL_GRAPH_DYNAMIC_ELEMENT_TYPE);
  options.mutable_language()->EnableLanguageFeature(
      zetasql::FEATURE_V_1_4_SQL_GRAPH_DYNAMIC_LABEL_PROPERTIES_IN_DDL);
  options.mutable_language()->EnableLanguageFeature(
      zetasql::FEATURE_V_1_4_SQL_GRAPH_DYNAMIC_LABEL_EXTENSION_IN_DDL);
  if (EmulatorFeatureFlags::instance().flags().enable_insert_on_conflict_dml) {
    options.mutable_language()->EnableLanguageFeature(
        zetasql::FEATURE_INSERT_ON_CONFLICT_CLAUSE);
  } else {
    options.mutable_language()->DisableLanguageFeature(
        zetasql::FEATURE_INSERT_ON_CONFLICT_CLAUSE);
  }
  ZETASQL_RETURN_IF_ERROR(
      options.mutable_language()->EnableReservableKeyword("GRAPH_TABLE"));
  return options;
}

// TODO : Replace with a better error transforming mechanism,
// ideally hooking into ZetaSQL to control error messages.
absl::Status MaybeTransformZetaSQLDMLError(absl::Status error) {
  // For inserts to a primary key columm.
  if (absl::StartsWith(error.message(),
                       "Failed to insert row with primary key")) {
    absl::Status error_status(absl::StatusCode::kAlreadyExists,
                              error.message());
    // Inserting a key that already exists is a constraint error and will cause
    // the transaction to become invalidated.
    error_status.SetPayload(kConstraintError, absl::Cord(""));
    return error_status;
  }

  // For updates to a primary key column.
  if (absl::StartsWith(error.message(), "Cannot modify a primary key column")) {
    return absl::Status(absl::StatusCode::kInvalidArgument, error.message());
  }

  return error;
}

bool IsGenerated(const zetasql::Column* column) {
  return column->GetAs<QueryableColumn>()->wrapped_column()->is_generated();
}

// Simple visitor to traverse an expression and check if it references
// a pending commit timestamp value column (that is, a column set to
// PENDING_COMMIT_TIMESTAMP() in this DML). Used to validate THEN RETURN.
class ReturningCommitTimestampChecker : public zetasql::ResolvedASTVisitor {
 public:
  explicit ReturningCommitTimestampChecker(
      const CaseInsensitiveStringSet& commit_timestamp_dml_value_columns)
      : commit_timestamp_dml_value_columns_(
            commit_timestamp_dml_value_columns) {}

  absl::Status VisitResolvedColumnRef(
      const zetasql::ResolvedColumnRef* node) override {
    if (commit_timestamp_dml_value_columns_.contains(node->column().name())) {
      return error::PendingCommitTimestampDmlValueOnly();
    }
    return absl::OkStatus();
  }

 private:
  // Not owned.
  const CaseInsensitiveStringSet& commit_timestamp_dml_value_columns_;
};

absl::StatusOr<std::unique_ptr<RowCursor>> BuildReturningRowResult(
    const zetasql::ResolvedReturningClause* returning,
    const CaseInsensitiveStringSet& pending_ts_columns,
    std::unique_ptr<zetasql::EvaluatorTableIterator> iterator) {
  if (iterator == nullptr) {
    return nullptr;
  }

  // Pending commit timestamp values cannot be in the returning clause and
  // cannot be referenced in a returning clause expression.
  ZETASQL_RET_CHECK_NE(returning, nullptr);
  ReturningCommitTimestampChecker checker(pending_ts_columns);
  // First check computed expressions. Also collect the set of column ids for
  // all computed columns.
  absl::flat_hash_set<int> computed_column_ids;
  for (const auto& resolved_named_expr : returning->expr_list()) {
    ZETASQL_RETURN_IF_ERROR(resolved_named_expr->Accept(&checker));
    computed_column_ids.insert(resolved_named_expr->column().column_id());
  }
  // Next, check for output columns that are column references (not computed).
  for (const auto& named_column : returning->output_column_list()) {
    const zetasql::ResolvedColumn& column = named_column->column();
    if (!computed_column_ids.contains(column.column_id()) &&
        pending_ts_columns.contains(column.name())) {
      return error::PendingCommitTimestampDmlValueOnly();
    }
  }

  std::vector<std::vector<zetasql::Value>> values;
  while (iterator->NextRow()) {
    values.emplace_back();
    values.back().reserve(iterator->NumColumns());
    for (int i = 0; i < iterator->NumColumns(); ++i) {
      values.back().push_back(iterator->GetValue(i));
    }
  }
  ZETASQL_RETURN_IF_ERROR(iterator->Status());

  std::vector<std::string> names;
  std::vector<const zetasql::Type*> types;
  for (int i = 0; i < iterator->NumColumns(); ++i) {
    names.push_back(iterator->GetColumnName(i));
    types.push_back(iterator->GetColumnType(i));
  }
  return std::make_unique<VectorsRowCursor>(names, types, values);
}

// Builds a INSERT mutation and returns it along with a count of inserted rows
// and an indication whether the input to the insert statement contained any
// rows or not. The latter can be used to determine whether an update count of
// 0 means that an OR IGNORE clause filtered away all rows, or whether the input
// query for the INSERT statement yielded zero rows.
absl::StatusOr<std::tuple<Mutation, int64_t, bool>> BuildInsert(
    std::unique_ptr<zetasql::EvaluatorTableModifyIterator> iterator,
    MutationOpType op_type,
    const std::vector<zetasql::ResolvedColumn>& insert_columns,
    const CaseInsensitiveStringSet& pending_ts_columns, bool is_upsert_query,
    DatabaseDialect database_dialect, bool include_all_returned_columns) {
  // Only include non-generated primary key columns and insert columns in the
  // INSERT mutation.
  CaseInsensitiveStringSet insert_column_names;
  for (const auto& insert_column : insert_columns) {
    insert_column_names.insert(insert_column.name());
  }

  const zetasql::Table* table = iterator->table();
  absl::flat_hash_set<int> excluded_column_idx;
  std::vector<std::string> column_names;
  std::optional<std::vector<int>> key_offsets = table->PrimaryKey();
  for (int i = 0; i < table->NumColumns(); ++i) {
    bool is_key = key_offsets.has_value() &&
                  std::find(key_offsets->begin(), key_offsets->end(), i) !=
                      key_offsets->end();
    const auto& column_name = table->GetColumn(i)->Name();
    if (IsGenerated(table->GetColumn(i))) {
      if (is_key) {
        // TODO: b/310194797 - GSQL reference implementation returns NULL for
        // generated keys or columns in POSTGRES dialect. GSQL AST constructed
        // from Spangres transformer does not contain nodes to compute the
        // generated columns unlike when ZetaSQL Analyzer is used for
        // ZetaSQL queries.
        if (database_dialect == DatabaseDialect::POSTGRESQL &&
            is_upsert_query) {
          return error::UnsupportedGeneratedKeyWithUpsertQueries();
        }
      }
      if (include_all_returned_columns) {
        column_names.push_back(column_name);
      } else {
        excluded_column_idx.insert(i);
      }
      continue;
    }
    if (!insert_column_names.contains(column_name) && !is_key) {
      excluded_column_idx.insert(i);
      continue;
    }
    column_names.push_back(column_name);
  }

  // Keys of insert rows
  std::set<Key> seen_keys;
  std::vector<ValueList> values;
  bool has_rows = false;
  while (iterator->NextRow()) {
    has_rows = true;
    values.emplace_back();
    Key row_key;
    for (int i = 0; i < table->NumColumns(); ++i) {
      zetasql::Value column_value = iterator->GetColumnValue(i);
      const auto& column_name = table->GetColumn(i)->Name();
      if (pending_ts_columns.find(column_name) != pending_ts_columns.end()) {
        column_value =
            zetasql::Value::StringValue(kCommitTimestampIdentifier);
      }
      // Build the primary key (including generated keys) for each insert row
      // for INSERT OR UPDATE statement to check for unsupported scenarios
      // below. For instance, multiple insert rows with same primary key in
      // INSERT OR UPDATE query is not supported. `row_key` is used to identify
      // this and return appropriate error.
      if (op_type == MutationOpType::kInsertOrUpdate &&
          key_offsets.has_value() &&
          std::find(key_offsets->begin(), key_offsets->end(), i) !=
              key_offsets->end()) {
        row_key.AddColumn(column_value);
      }
      if (excluded_column_idx.contains(i)) {
        continue;
      }
      values.back().push_back(column_value);
    }
    if (op_type == MutationOpType::kInsertOrUpdate) {
      // Spanner returns error when multiple insert rows have same key in
      // INSERT OR UPDATE statement.
      if (seen_keys.find(row_key) != seen_keys.end()) {
        return error::CannotInsertDuplicateKeyInsertOrUpdateDml(
            row_key.DebugString());
      } else {
        seen_keys.insert(row_key);
      }
    }
  }

  Mutation mutation;
  // `values` can be empty if INSERT OR IGNORE query contains insert rows that
  // already existed. In this case, return empty mutation with rows modified
  // count as 0.
  if (values.empty()) {
    return std::make_tuple(mutation, 0, has_rows);
  }
  mutation.AddWriteOp(op_type, table->FullName(), column_names, values,
                      /*origin_is_dml=*/true);
  return std::make_tuple(mutation, values.size(), has_rows);
}

// Returns true if the provided column-value pair contains a pending commit
// timestamp sentinel.
bool IsPendingCommitTimestampSentinel(const Schema* schema,
                                      const zetasql::Table* table,
                                      const zetasql::Column* column,
                                      const zetasql::Value& value) {
  return IsPendingCommitTimestamp(
      schema->FindTable(table->FullName())->FindColumn(column->Name()), value);
}

// Builds a UPDATE mutation and returns it along with a count of updated rows.
std::pair<Mutation, int64_t> BuildUpdate(
    std::unique_ptr<zetasql::EvaluatorTableModifyIterator> iterator,
    MutationOpType op_type, const CaseInsensitiveStringSet& pending_ts_columns,
    const CaseInsensitiveStringSet& updated_columns, const Schema* schema) {
  const zetasql::Table* table = iterator->table();
  absl::flat_hash_set<int> generated_columns;
  std::vector<std::string> column_names;
  for (int i = 0; i < table->NumColumns(); ++i) {
    if (IsGenerated(table->GetColumn(i))) {
      generated_columns.insert(i);
      continue;
    }
    column_names.push_back(table->GetColumn(i)->Name());
  }

  std::vector<ValueList> values;
  while (iterator->NextRow()) {
    values.emplace_back();
    for (int i = 0; i < table->NumColumns(); ++i) {
      if (generated_columns.contains(i)) {
        continue;
      }
      const zetasql::Column* column = table->GetColumn(i);
      zetasql::Value value = iterator->GetColumnValue(i);
      if (pending_ts_columns.contains(column->Name()) ||
          // Also replace previously written commit timestamp sentinels with
          // the string representation. Otherwise, these previously written
          // sentinels will appear to ReadWriteTransaction to be user-specified
          // timestamps from the future (which MaybeSetCommitTimestampSentinel
          // will then reject).
          (!updated_columns.contains(column->Name()) &&
           IsPendingCommitTimestampSentinel(schema, table, column, value))) {
        values.back().push_back(
            zetasql::Value::StringValue(kCommitTimestampIdentifier));
      } else {
        values.back().push_back(std::move(value));
      }
    }
  }

  Mutation mutation;
  mutation.AddWriteOp(op_type, table->FullName(), column_names, values,
                      /*origin_is_dml=*/true);
  return std::make_pair(mutation, values.size());
}

// Builds a DELETE mutation and returns it along with a count of deleted rows.
std::pair<Mutation, int64_t> BuildDelete(
    std::unique_ptr<zetasql::EvaluatorTableModifyIterator> iterator) {
  const zetasql::Table* table = iterator->table();

  KeySet key_set;
  if (!table->PrimaryKey().has_value() && iterator->NextRow()) {
    // There is no primary key in the case of a singleton table. Delete
    // mutation will contain an empty key set in such a case if there is a row
    // to be deleted.
    key_set.AddKey(Key{});
  } else {
    while (iterator->NextRow()) {
      ValueList key_values;
      for (int i = 0; i < table->PrimaryKey()->size(); ++i) {
        key_values.push_back(iterator->GetOriginalKeyValue(i));
      }
      key_set.AddKey(Key{key_values});
    }
  }

  Mutation mutation;
  mutation.AddDeleteOp(table->FullName(), key_set);
  return std::make_pair(mutation, key_set.keys().size());
}

// Returns true if the ResolvedDMLValue is a call to PENDING_COMMIT_TIMESTAMP()
bool IsPendingCommitTimestamp(const zetasql::ResolvedDMLValue& dml_value) {
  if (dml_value.value()->Is<zetasql::ResolvedFunctionCall>()) {
    const zetasql::ResolvedFunctionCall* fn =
        dml_value.value()->GetAs<zetasql::ResolvedFunctionCall>();
    if (fn->function()->Name() == kPendingCommitTimestampFunctionName) {
      // Touch the argument list so that the ResolvedAST code does not claim we
      // missed it.
      return fn->argument_list().empty();
    }
  }
  return false;
}

bool IsDefaultValue(const zetasql::ResolvedDMLValue& dml_value) {
  return dml_value.value()->Is<zetasql::ResolvedDMLDefault>();
}

absl::StatusOr<CaseInsensitiveStringSet> PendingCommitTimestampColumnsInInsert(
    const zetasql::Table* table,
    const std::vector<zetasql::ResolvedColumn>& insert_columns,
    const std::vector<std::unique_ptr<const zetasql::ResolvedInsertRow>>&
        insert_rows) {
  int64_t num_columns = insert_columns.size();
  CaseInsensitiveStringSet pending_ts_columns;
  for (const auto& insert_row : insert_rows) {
    for (int i = 0; i < num_columns; ++i) {
      if (IsPendingCommitTimestamp(*insert_row->value_list()[i])) {
        pending_ts_columns.insert(insert_columns.at(i).name());
      }
    }
  }

  for (const auto& insert_row : insert_rows) {
    for (int i = 0; i < num_columns; ++i) {
      const auto& dml_value = insert_row->value_list()[i];
      const auto& column_name = insert_columns.at(i).name();
      if (pending_ts_columns.find(column_name) != pending_ts_columns.end() &&
          !IsPendingCommitTimestamp(*dml_value)) {
        return error::PendingCommitTimestampAllOrNone(i + 1);
      }
    }
  }
  return pending_ts_columns;
}

absl::StatusOr<CaseInsensitiveStringSet> PendingCommitTimestampColumnsInUpdate(
    const zetasql::Table* table,
    const std::vector<std::unique_ptr<const zetasql::ResolvedUpdateItem>>&
        update_item_list) {
  CaseInsensitiveStringSet pending_ts_columns;
  for (const auto& update_item : update_item_list) {
    if (update_item->set_value() &&
        IsPendingCommitTimestamp(*update_item->set_value())) {
      std::string column_name = update_item->target()
                                    ->GetAs<zetasql::ResolvedColumnRef>()
                                    ->column()
                                    .name();
      pending_ts_columns.insert(column_name);
    }
  }
  return pending_ts_columns;
}

// Extracts the set of column names being updated.
absl::StatusOr<CaseInsensitiveStringSet> ColumnsInUpdate(
    const std::vector<std::unique_ptr<const zetasql::ResolvedUpdateItem>>&
        update_item_list) {
  CaseInsensitiveStringSet columns;
  for (const auto& update_item : update_item_list) {
    if (update_item->set_value()) {
      std::vector<const zetasql::ResolvedNode*> column_refs;
      update_item->target()->GetDescendantsWithKinds(
          {zetasql::RESOLVED_COLUMN_REF}, &column_refs);
      ZETASQL_RET_CHECK_EQ(column_refs.size(), 1);
      std::string column_name = column_refs[0]
                                    ->GetAs<zetasql::ResolvedColumnRef>()
                                    ->column()
                                    .name();
      columns.insert(std::move(column_name));
    }
  }
  return columns;
}

absl::StatusOr<ExecuteUpdateResult> EvaluateResolvedInsert(
    const zetasql::ResolvedInsertStmt* insert_statement,
    const zetasql::ParameterValueMap& parameters,
    zetasql::TypeFactory* type_factory, DatabaseDialect database_dialect,
    const std::string time_zone,
    bool return_all_insert_rows_insert_ignore_dml = false) {
  if (insert_statement->insert_mode() ==
      zetasql::ResolvedInsertStmt::OR_REPLACE) {
    return error::UnsupportedUpsertQueries("Insert or replace");
  }
  if (return_all_insert_rows_insert_ignore_dml) {
    ZETASQL_RET_CHECK(insert_statement->insert_mode() ==
              zetasql::ResolvedInsertStmt::OR_IGNORE)
        << insert_statement->DebugString();
  }
  bool is_upsert_query = insert_statement->insert_mode() ==
                             zetasql::ResolvedInsertStmt::OR_IGNORE ||
                         insert_statement->insert_mode() ==
                             zetasql::ResolvedInsertStmt::OR_UPDATE;
  MutationOpType op_type = MutationOpType::kInsert;
  if (is_upsert_query) {
    std::string insert_mode_name = (insert_statement->insert_mode() ==
                                    zetasql::ResolvedInsertStmt::OR_IGNORE)
                                       ? "Insert or ignore"
                                       : "Insert or update";
    if (!EmulatorFeatureFlags::instance().flags().enable_upsert_queries) {
      return error::UnsupportedUpsertQueries(insert_mode_name);
    }
    if (!EmulatorFeatureFlags::instance()
             .flags()
             .enable_upsert_queries_with_returning &&
        insert_statement->returning() != nullptr) {
      return error::UnsupportedReturningWithUpsertQueries(insert_mode_name);
    }
    if (insert_statement->insert_mode() ==
        zetasql::ResolvedInsertStmt::OR_UPDATE) {
      op_type = MutationOpType::kInsertOrUpdate;
    }
  }

  auto prepared_insert = std::make_unique<zetasql::PreparedModify>(
      insert_statement,
      CommonEvaluatorOptions(type_factory, time_zone,
                             return_all_insert_rows_insert_ignore_dml));
  ZETASQL_ASSIGN_OR_RETURN(auto analyzer_options,
                   MakeAnalyzerOptionsWithParameters(parameters, time_zone));
  ZETASQL_RETURN_IF_ERROR(prepared_insert->Prepare(analyzer_options));

  std::unique_ptr<zetasql::EvaluatorTableIterator> returning_iter;
  // `returning_iter` can be NULL if there is no THEN RETURN clause.
  auto status_or = prepared_insert->Execute(parameters, {}, &returning_iter);
  if (!status_or.ok()) {
    return MaybeTransformZetaSQLDMLError(status_or.status());
  }
  auto iterator = std::move(status_or).value();

  ZETASQL_ASSIGN_OR_RETURN(
      auto pending_ts_columns,
      PendingCommitTimestampColumnsInInsert(
          iterator->table(), insert_statement->insert_column_list(),
          insert_statement->row_list()));

  ZETASQL_ASSIGN_OR_RETURN(
      auto cursor,
      BuildReturningRowResult(insert_statement->returning(), pending_ts_columns,
                              std::move(returning_iter)));

  // When `return_all_insert_rows_insert_ignore_dml` is true, then the
  // mutation should have all table scan columns (not only insert columns)
  // including all generated columns.
  ZETASQL_ASSIGN_OR_RETURN(
      const auto& mutation_and_count,
      BuildInsert(std::move(iterator), op_type,
                  (return_all_insert_rows_insert_ignore_dml)
                      ? insert_statement->table_scan()->column_list()
                      : insert_statement->insert_column_list(),
                  pending_ts_columns, is_upsert_query, database_dialect,
                  return_all_insert_rows_insert_ignore_dml));

  // Resulting mutation count can be 0 only if all insert rows already
  // existed and none were inserted due to OR_IGNORE insert mode, or if the
  // insert statement used a SELECT statement with a WHERE clause.
  // Validate the invariants if either the count is zero or result mutations are
  // empty.
  if (std::get<2>(mutation_and_count) &&
      (std::get<1>(mutation_and_count) == 0 ||
       std::get<0>(mutation_and_count).ops().empty())) {
    ZETASQL_RET_CHECK(insert_statement->insert_mode() ==
              zetasql::ResolvedInsertStmt::OR_IGNORE);
    ZETASQL_RET_CHECK(std::get<0>(mutation_and_count).ops().empty());
    ZETASQL_RET_CHECK_EQ(std::get<1>(mutation_and_count), 0);
  }
  return ExecuteUpdateResult{std::get<0>(mutation_and_count),
                             std::get<1>(mutation_and_count),
                             std::move(cursor)};
}

absl::StatusOr<ExecuteUpdateResult> EvaluateResolvedUpdate(
    const zetasql::ResolvedUpdateStmt* update_statement,
    const zetasql::ParameterValueMap& parameters,
    zetasql::TypeFactory* type_factory, const Schema* schema,
    const std::string time_zone) {
  ZETASQL_ASSIGN_OR_RETURN(auto updated_columns,
                   ColumnsInUpdate(update_statement->update_item_list()));

  auto prepared_update = std::make_unique<zetasql::PreparedModify>(
      update_statement, CommonEvaluatorOptions(type_factory, time_zone));
  ZETASQL_ASSIGN_OR_RETURN(auto analyzer_options,
                   MakeAnalyzerOptionsWithParameters(parameters, time_zone));
  ZETASQL_RETURN_IF_ERROR(prepared_update->Prepare(analyzer_options));

  std::unique_ptr<zetasql::EvaluatorTableIterator> returning_iter;
  auto status_or = prepared_update->Execute(parameters, {}, &returning_iter);
  if (!status_or.ok()) {
    return MaybeTransformZetaSQLDMLError(status_or.status());
  }
  auto iterator = std::move(status_or).value();
  ZETASQL_ASSIGN_OR_RETURN(
      auto pending_ts_columns,
      PendingCommitTimestampColumnsInUpdate(
          iterator->table(), update_statement->update_item_list()));
  ZETASQL_ASSIGN_OR_RETURN(
      auto cursor,
      BuildReturningRowResult(update_statement->returning(), pending_ts_columns,
                              std::move(returning_iter)));
  const auto& mutation_and_count =
      BuildUpdate(std::move(iterator), MutationOpType::kUpdate,
                  pending_ts_columns, updated_columns, schema);
  return ExecuteUpdateResult{mutation_and_count.first,
                             mutation_and_count.second, std::move(cursor)};
}

absl::StatusOr<ExecuteUpdateResult> EvaluateResolvedDelete(
    const zetasql::ResolvedDeleteStmt* delete_statement,
    const zetasql::ParameterValueMap& parameters,
    zetasql::TypeFactory* type_factory, const std::string time_zone) {
  auto prepared_delete = std::make_unique<zetasql::PreparedModify>(
      delete_statement, CommonEvaluatorOptions(type_factory, time_zone));
  ZETASQL_ASSIGN_OR_RETURN(auto analyzer_options,
                   MakeAnalyzerOptionsWithParameters(parameters, time_zone));
  ZETASQL_RETURN_IF_ERROR(prepared_delete->Prepare(analyzer_options));

  std::unique_ptr<zetasql::EvaluatorTableIterator> returning_iter;
  ZETASQL_ASSIGN_OR_RETURN(auto iterator,
                   prepared_delete->Execute(parameters, {}, &returning_iter));
  ZETASQL_ASSIGN_OR_RETURN(auto cursor,
                   BuildReturningRowResult(delete_statement->returning(),
                                           /*pending_ts_columns=*/{},
                                           std::move(returning_iter)));

  const auto& mutation_and_count = BuildDelete(std::move(iterator));
  return ExecuteUpdateResult{mutation_and_count.first,
                             mutation_and_count.second, std::move(cursor)};
}

// Uses googlesql/public/evaluator to evaluate a DML statement represented by a
// resolved AST and returns a pair of mutation and count of modified rows.
absl::StatusOr<ExecuteUpdateResult> EvaluateUpdate(
    const zetasql::ResolvedStatement* resolved_statement,
    zetasql::Catalog* catalog, const zetasql::ParameterValueMap& parameters,
    zetasql::TypeFactory* type_factory, DatabaseDialect database_dialect,
    const Schema* schema, const std::string time_zone,
    bool return_all_insert_rows_insert_ignore_dml = false) {
  switch (resolved_statement->node_kind()) {
    case zetasql::RESOLVED_INSERT_STMT:
      return EvaluateResolvedInsert(
          resolved_statement->GetAs<zetasql::ResolvedInsertStmt>(),
          parameters, type_factory, database_dialect, time_zone,
          return_all_insert_rows_insert_ignore_dml);
    case zetasql::RESOLVED_UPDATE_STMT:
      return EvaluateResolvedUpdate(
          resolved_statement->GetAs<zetasql::ResolvedUpdateStmt>(),
          parameters, type_factory, schema, time_zone);
    case zetasql::RESOLVED_DELETE_STMT:
      return EvaluateResolvedDelete(
          resolved_statement->GetAs<zetasql::ResolvedDeleteStmt>(),
          parameters, type_factory, time_zone);
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unsupported support node kind "
                       << ResolvedNodeKind_Name(
                              resolved_statement->node_kind());
      break;
  }
}

absl::StatusOr<std::unique_ptr<RowCursor>> ResolveCallStatement() {
  std::vector<std::string> names;
  std::vector<const zetasql::Type*> types;
  std::vector<std::vector<zetasql::Value>> values;
  names.push_back("result");
  types.push_back(zetasql::types::BoolType());
  return std::make_unique<VectorsRowCursor>(names, types, values);
}

// Uses googlesql/public/evaluator to evaluate a query statement represented by
// a resolved AST and returns a row cursor.
absl::StatusOr<std::unique_ptr<RowCursor>> EvaluateQuery(
    const zetasql::ResolvedStatement* resolved_statement,
    const zetasql::ParameterValueMap& params,
    zetasql::TypeFactory* type_factory, int64_t* num_output_rows,
    const v1::ExecuteSqlRequest_QueryMode query_mode,
    const std::string time_zone) {
  if (resolved_statement->node_kind() == zetasql::RESOLVED_CALL_STMT) {
    // Evaluation of a CALL statement is currently a no-op. This is added to
    // ensure the emulator doesn't error out when the customer tries the CALL
    // statement.
    return ResolveCallStatement();
  }
  ZETASQL_RET_CHECK_EQ(resolved_statement->node_kind(), zetasql::RESOLVED_QUERY_STMT)
      << "input is not a query statement";

  auto prepared_query = std::make_unique<zetasql::PreparedQuery>(
      resolved_statement->GetAs<zetasql::ResolvedQueryStmt>(),
      CommonEvaluatorOptions(type_factory, time_zone));
  // Call PrepareQuery to set the AnalyzerOptions that we used to Analyze the
  // statement.
  ZETASQL_ASSIGN_OR_RETURN(auto analyzer_options,
                   MakeAnalyzerOptionsWithParameters(params, time_zone));
  ZETASQL_RETURN_IF_ERROR(prepared_query->Prepare(analyzer_options));

  // Get the query metadata from the prepared query.
  std::vector<std::string> names;
  std::vector<const zetasql::Type*> types;
  auto columns = prepared_query->GetColumns();
  for (auto& column : columns) {
    names.push_back(column.first);
    types.push_back(column.second);
  }
  std::vector<std::vector<zetasql::Value>> values;

  if (query_mode == v1::ExecuteSqlRequest::PLAN) {
    // Return the query metadata when the query is executed in PLAN mode.
    // This allows clients to use PLAN to get the metadata of the query without
    // having to execute the query and/or supply values for all query
    // parameters. This is used by some drivers (e.g. JDBC) and by PGAdapter.
    return std::make_unique<VectorsRowCursor>(names, types, values);
  } else {
    // Finally execute the query.
    ZETASQL_ASSIGN_OR_RETURN(auto iterator, prepared_query->Execute(params));

    while (iterator->NextRow()) {
      values.emplace_back();
      values.back().reserve(iterator->NumColumns());
      for (int i = 0; i < iterator->NumColumns(); ++i) {
        values.back().push_back(iterator->GetValue(i));
      }
    }
    ZETASQL_RETURN_IF_ERROR(iterator->Status());
    *num_output_rows = values.size();
    return std::make_unique<VectorsRowCursor>(names, types, values);
  }
}

absl::StatusOr<std::map<std::string, zetasql::Value>> ExtractParameters(
    const Query& query, const zetasql::AnalyzerOutput* analyzer_output) {
  // Allow the loop below to look up undeclared parameters without worrying
  // about case. ZetaSQL will return the undeclared parameters using the
  // spelling provided in the query, but the undeclared_params map uses the
  // spelling from the query params section of the request.
  //
  // For example:
  //     SELECT CAST(@pBool AS bool) with a supplied parameter "pbool".
  //
  // Do this here instead of the frontend so that the frontend does not need to
  // know any details about case normalization.
  //
  // This map takes pointers to elements inside of query.undeclared_params,
  // which is const so the pointers should not be invalidated.
  CaseInsensitiveStringMap<const google::protobuf::Value*> undeclared_params;
  for (const auto& [name, value] : query.undeclared_params)
    undeclared_params[name] = &value;

  // Build new parameter map which includes all unresolved parameters.
  auto params = query.declared_params;
  for (const auto& [name, type] : analyzer_output->undeclared_parameters()) {
    auto it = undeclared_params.find(name);

    // ZetaSQL will return an undeclared parameter error for any parameters that
    // do not have values specified (ie, when it == end()).
    if (it != undeclared_params.end()) {
      auto parsed_value = frontend::ValueFromProto(*it->second, type);

      // If the value does not parse as the given type, the error code is
      // kInvalidArgument, not kFailedPrecondition, for example.
      if (!parsed_value.ok()) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            parsed_value.status().message());
      }

      params[name] = parsed_value.value();
    }
  }

  return params;
}

bool IsDMLStmt(const zetasql::ResolvedNodeKind& query_kind) {
  return query_kind == zetasql::RESOLVED_INSERT_STMT ||
         query_kind == zetasql::RESOLVED_UPDATE_STMT ||
         query_kind == zetasql::RESOLVED_DELETE_STMT;
}

bool IsDMLStmtWitoutSelect(
    const zetasql::ResolvedStatement* resolved_statement) {
  const zetasql::ResolvedNodeKind& query_kind =
      resolved_statement->node_kind();
  if (!IsDMLStmt(query_kind)) return false;

  if (query_kind == zetasql::RESOLVED_INSERT_STMT &&
      resolved_statement->GetAs<zetasql::ResolvedInsertStmt>()->query() !=
          nullptr) {
    // This is an INSERT SELECT query.
    return false;
  }
  return true;
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedStatement>>
ExtractValidatedResolvedStatementAndOptions(
    const zetasql::AnalyzerOutput* analyzer_output,
    const QueryContext& context, bool in_partition_query = false,
    QueryEngineOptions* query_engine_options = nullptr) {
  ZETASQL_RET_CHECK_NE(analyzer_output->resolved_statement(), nullptr);

  // Rewrite query hints to use only the 'spanner' prefix.
  HintRewriter rewriter;
  ZETASQL_RETURN_IF_ERROR(analyzer_output->resolved_statement()->Accept(&rewriter));
  ZETASQL_ASSIGN_OR_RETURN(auto statement,
                   rewriter.ConsumeRootNode<zetasql::ResolvedStatement>());

  // Validate the query and extract and return any options specified
  // through hint if the caller requested them.
  QueryEngineOptions options;
  std::unique_ptr<QueryValidator> query_validator =
      IsDMLStmtWitoutSelect(analyzer_output->resolved_statement())
          ? std::make_unique<DMLQueryValidator>(context, &options)
          : std::make_unique<QueryValidator>(context, &options);
  // In Cloud Spanner, batch query is using PartitionQuery function.
  query_validator->set_in_partition_query(in_partition_query);

  ZETASQL_RETURN_IF_ERROR(statement->Accept(query_validator.get()));
  if (query_engine_options != nullptr) {
    *query_engine_options = options;
  }

  // Validate the index hints.
  bool allow_search_indexes_in_transaction =
      IsSearchQueryAllowed(&options, context);
  bool in_select_for_update_query =
      IsSelectForUpdateQuery(*(analyzer_output->resolved_statement()));
  IndexHintValidator index_hint_validator{
      context.schema,
      options.disable_query_null_filtered_index_check ||
          config::disable_query_null_filtered_index_check(),
      allow_search_indexes_in_transaction, in_partition_query,
      in_select_for_update_query};
  ZETASQL_RETURN_IF_ERROR(statement->Accept(&index_hint_validator));

  ANNFunctionsRewriter ann_functions_rewriter;
  ZETASQL_RETURN_IF_ERROR(statement->Accept(&ann_functions_rewriter));
  ZETASQL_ASSIGN_OR_RETURN(
      statement,
      ann_functions_rewriter.ConsumeRootNode<zetasql::ResolvedStatement>());

  if (!ann_functions_rewriter.ann_functions().empty()) {
    ANNValidator ann_validator(context.schema);
    ZETASQL_RETURN_IF_ERROR(statement->Accept(&ann_validator));
    // Check if all the ANN functions passed the validation.
    for (const auto& ann_function : ann_functions_rewriter.ann_functions()) {
      if (!ann_validator.ann_functions().contains(ann_function)) {
        return error::ApproxDistanceInvalidShape(
            ann_function->function()->Name());
      }
    }
  }

  // Check the query size limits
  // https://cloud.google.com/spanner/quotas#query_limits
  QuerySizeLimitsChecker checker;
  ZETASQL_RETURN_IF_ERROR(checker.CheckQueryAgainstLimits(statement.get()));

  return statement;
}

// Implements ResolvedASTVisitor to get the target table that various DML
// statements modify.
class ExtractDmlTargetTableVisitor : public zetasql::ResolvedASTVisitor {
 public:
  std::optional<std::string> target_table() const { return target_table_; }

 private:
  absl::Status VisitResolvedInsertStmt(
      const zetasql::ResolvedInsertStmt* node) override {
    target_table_ = node->table_scan()->table()->Name();
    return absl::OkStatus();
  }
  absl::Status VisitResolvedDeleteStmt(
      const zetasql::ResolvedDeleteStmt* node) override {
    target_table_ = node->table_scan()->table()->Name();
    return absl::OkStatus();
  }
  absl::Status VisitResolvedUpdateStmt(
      const zetasql::ResolvedUpdateStmt* node) override {
    target_table_ = node->table_scan()->table()->Name();
    return absl::OkStatus();
  }

  std::optional<std::string> target_table_;
};

// A QueryEvaluator instance against a specific QueryEngine and QueryContext.
class QueryEvaluatorForEngine : public QueryEvaluator {
 public:
  QueryEvaluatorForEngine(const QueryEngine& query_engine,
                          const QueryContext& query_context)
      : query_engine_(query_engine), query_context_(query_context) {}
  ~QueryEvaluatorForEngine() override = default;

  absl::StatusOr<std::unique_ptr<RowCursor>> Evaluate(
      const std::string& query) override {
    Query q{/*sql=*/query, /*declared_params=*/{}, /*undeclared_params=*/{}};

    ZETASQL_ASSIGN_OR_RETURN(auto result,
                     query_engine_.ExecuteSql(q, query_context_,
                                              v1::ExecuteSqlRequest::NORMAL));
    return std::move(result.rows);
  }

 private:
  const QueryEngine& query_engine_;
  const QueryContext& query_context_;
};

}  // namespace

absl::StatusOr<std::string> QueryEngine::GetDmlTargetTable(
    const Query& query, const Schema* schema) const {
  ZETASQL_ASSIGN_OR_RETURN(auto analyzer_options,
                   MakeAnalyzerOptionsWithParameters(
                       query.declared_params,
                       GetTimeZone(function_catalog_.GetLatestSchema())));
  analyzer_options.set_prune_unused_columns(true);
  Catalog catalog(schema, &function_catalog_, type_factory_, analyzer_options);
  ZETASQL_ASSIGN_OR_RETURN(
      auto analyzer_output,
      Analyze(query.sql, &catalog, analyzer_options, type_factory_));
  ZETASQL_ASSIGN_OR_RETURN(auto params,
                   ExtractParameters(query, analyzer_output.get()));
  ZETASQL_ASSIGN_OR_RETURN(auto statement,
                   ExtractValidatedResolvedStatementAndOptions(
                       analyzer_output.get(), {.schema = schema}));

  ExtractDmlTargetTableVisitor visitor;
  ZETASQL_RETURN_IF_ERROR(statement->Accept(&visitor));
  if (!visitor.target_table()) {
    return absl::InvalidArgumentError(absl::Substitute(
        "The given query does not contain a DML statement: $0", query.sql));
  }
  return *visitor.target_table();
}

absl::StatusOr<QueryResult> QueryEngine::ExecuteSql(
    const Query& query, const QueryContext& context) const {
  return ExecuteSql(query, context, v1::ExecuteSqlRequest::NORMAL);
}

std::string QueryEngine::GetTimeZone(const Schema* schema) {
  std::string time_zone = kDefaultTimeZone;
  if (schema != nullptr) {
    time_zone = schema->default_time_zone();
  }
  return time_zone;
}

absl::StatusOr<QueryResult> QueryEngine::ExecuteInsertOnConflictDml(
    const Query& query, const zetasql::ResolvedStatement* resolved_statement,
    const std::map<std::string, zetasql::Value>& params,
    google::spanner::emulator::backend::Catalog& catalog,
    zetasql::AnalyzerOptions& analyzer_options,
    const QueryContext& context) const {
  QueryResult result;
  ZETASQL_RET_CHECK(
      EmulatorFeatureFlags::instance().flags().enable_insert_on_conflict_dml);
  const zetasql::ResolvedInsertStmt* insert_statement =
      resolved_statement->GetAs<zetasql::ResolvedInsertStmt>();
  const auto& on_conflict_clause = insert_statement->on_conflict_clause();
  const Table* table = context.schema->FindTable(
      insert_statement->table_scan()->table()->Name());
  if (table == nullptr) {
    return error::TableNotFound(
        insert_statement->table_scan()->table()->Name());
  }

  // 1. In PostgreSQL dialect, we will try to translate the INSERT ON CONFLICT
  // DO UPDATE DML to INSERT OR UPDATE DML, if eligible, for backward
  // compatibility. This is consistent with logic in Cloud Spanner.
  if (on_conflict_clause->conflict_action() ==
          zetasql::ResolvedOnConflictClauseEnums::UPDATE &&
      context.schema->dialect() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSIGN_OR_RETURN(bool can_translate_to_insert_or_update_mode,
                     CanTranslateToInsertOrUpdateMode(*insert_statement));

    if (can_translate_to_insert_or_update_mode) {
      ZETASQL_ASSIGN_OR_RETURN(
          auto insert_or_update_dml,
          AnalyzeAsInsertOrUpdateDML(query.sql, &catalog, analyzer_options,
                                     type_factory_, &function_catalog_));

      ZETASQL_ASSIGN_OR_RETURN(auto insert_or_update_stmt,
                       ExtractValidatedResolvedStatementAndOptions(
                           insert_or_update_dml.get(), context));

      ZETASQL_ASSIGN_OR_RETURN(
          auto insert_or_update_result,
          EvaluateUpdate(insert_or_update_stmt.get(), &catalog, params,
                         type_factory_, context.schema->dialect(),
                         context.schema,
                         GetTimeZone(function_catalog_.GetLatestSchema())));
      ZETASQL_RETURN_IF_ERROR(context.writer->Write(insert_or_update_result.mutation));
      result.modified_row_count = insert_or_update_result.modify_row_count;
      result.rows = std::move(insert_or_update_result.returning_row_cursor);
      return result;
    }
  }

  // 2. Get the conflict target in the DML. Its either a primary key or a unique
  // index.
  // Set to kReservedIndexNameForPrimaryKey or the unique index name.
  std::string conflict_target_unique_constraint_name;
  // Set to key columns of the conflict target.
  std::vector<std::string> conflict_target_key_columns;
  ZETASQL_RETURN_IF_ERROR(ResolveConflictTarget(
      table, on_conflict_clause, conflict_target_key_columns,
      &conflict_target_unique_constraint_name));

  // 3. Extract the INSERT part of the DML and run in OR_IGNORE insert
  // mode to get *all* insert rows (new and existing).
  ZETASQL_ASSIGN_OR_RETURN(
      auto insert_or_ignore_stmt,
      BuildInsertOrIgnoreStmtFromInsertOnConflictStmt(insert_statement));

  ZETASQL_ASSIGN_OR_RETURN(
      auto insert_or_ignore_result,
      EvaluateUpdate(insert_or_ignore_stmt.get(), &catalog, params,
                     type_factory_, context.schema->dialect(), context.schema,
                     GetTimeZone(function_catalog_.GetLatestSchema()),
                     /*return_all_insert_rows_insert_ignore_dml=*/true));

  // 4. Get all table rows keys. The key columns are the conflict target key
  // columns.
  ZETASQL_ASSIGN_OR_RETURN(
      std::set<Key> original_table_row_keys,
      GetOriginalTableRows(table, conflict_target_unique_constraint_name,
                           conflict_target_key_columns, context.reader));

  // 5. Populate the column information required for insert or update action.
  absl::flat_hash_set<int> insert_column_index_in_mutation_for_insert;
  absl::flat_hash_set<int> insert_column_index_in_mutation_for_update;
  std::vector<std::string> columns_in_mutation;
  std::map<Key, std::vector<zetasql::Value>> insert_row_map;
  std::vector<Key> existing_rows;
  std::vector<Key> new_rows;
  ZETASQL_RETURN_IF_ERROR(ExtractColumnInfoAndRowsForInsertOrUpdateAction(
      insert_statement, conflict_target_key_columns, insert_or_ignore_result,
      original_table_row_keys, &insert_row_map, &existing_rows, &new_rows,
      &columns_in_mutation, &insert_column_index_in_mutation_for_insert,
      &insert_column_index_in_mutation_for_update));

  InsertOnConflictReturningRows returning_rows;
  // 6. Build and execute INSERT DML with insert rows that do not violate the
  // conflict target.
  if (!new_rows.empty()) {
    ZETASQL_ASSIGN_OR_RETURN(
        auto insert_new_rows_stmt,
        BuildInsertDMLForNewRows(insert_statement, *type_factory_, catalog,
                                 new_rows, insert_row_map, columns_in_mutation,
                                 insert_column_index_in_mutation_for_insert));

    ZETASQL_ASSIGN_OR_RETURN(
        auto insert_stmt_execute_update_result,
        EvaluateUpdate(insert_new_rows_stmt.get(), &catalog, params,
                       type_factory_, context.schema->dialect(), context.schema,
                       GetTimeZone(function_catalog_.GetLatestSchema())));

    ZETASQL_RETURN_IF_ERROR(
        context.writer->Write(insert_stmt_execute_update_result.mutation));
    result.modified_row_count +=
        insert_stmt_execute_update_result.modify_row_count;
    ZETASQL_RETURN_IF_ERROR(CollectReturningRows(
        std::move(insert_stmt_execute_update_result.returning_row_cursor),
        returning_rows));
  }

  // 7. Build and execute UPDATE DML with insert rows that violates the conflict
  // target and hence the respective table rows should be updated.
  if (on_conflict_clause->conflict_action() ==
          zetasql::ResolvedOnConflictClauseEnums::UPDATE &&
      !existing_rows.empty()) {
    ZETASQL_ASSIGN_OR_RETURN(auto update_existing_rows_resolved_stmt,
                     BuildUpdateDMLForExistingRows(
                         insert_statement, *type_factory_, catalog,
                         existing_rows, insert_row_map, columns_in_mutation,
                         insert_column_index_in_mutation_for_update,
                         conflict_target_key_columns));

    ZETASQL_ASSIGN_OR_RETURN(
        auto update_stmt_execute_update_result,
        EvaluateUpdate(update_existing_rows_resolved_stmt.get(), &catalog,
                       params, type_factory_, context.schema->dialect(),
                       context.schema,
                       GetTimeZone(function_catalog_.GetLatestSchema())));
    ZETASQL_RETURN_IF_ERROR(
        context.writer->Write(update_stmt_execute_update_result.mutation));
    result.modified_row_count +=
        update_stmt_execute_update_result.modify_row_count;
    ZETASQL_RETURN_IF_ERROR(CollectReturningRows(
        std::move(update_stmt_execute_update_result.returning_row_cursor),
        returning_rows));
  }
  // 8. Finally, combine returning rows from both actions and return the result.
  if (!returning_rows.rows.empty()) {
    result.rows = std::make_unique<VectorsRowCursor>(
        returning_rows.column_names, returning_rows.column_types,
        returning_rows.rows);
  }
  return result;
}

absl::StatusOr<QueryResult> QueryEngine::ExecuteSql(
    const Query& query, const QueryContext& context,
    v1::ExecuteSqlRequest_QueryMode query_mode) const {
  absl::Time start_time = absl::Now();

  ZETASQL_ASSIGN_OR_RETURN(auto analyzer_options,
                   MakeAnalyzerOptionsWithParameters(
                       query.declared_params,
                       GetTimeZone(function_catalog_.GetLatestSchema())));
  analyzer_options.set_prune_unused_columns(true);

  QueryEvaluatorForEngine view_evaluator(*this, context);
  Catalog catalog{
      context.schema,
      &function_catalog_,
      type_factory_,
      analyzer_options,
      context.reader,
      &view_evaluator,
      query.change_stream_internal_lookup,
  };

  std::unique_ptr<const zetasql::AnalyzerOutput> analyzer_output;
  if (context.schema->dialect() == database_api::DatabaseDialect::POSTGRESQL &&
      !query.change_stream_internal_lookup.has_value()) {
    ZETASQL_ASSIGN_OR_RETURN(analyzer_output,
                     AnalyzePostgreSQL(query.sql, &catalog, analyzer_options,
                                       type_factory_, &function_catalog_));

  } else {
    ZETASQL_ASSIGN_OR_RETURN(analyzer_output, Analyze(query.sql, &catalog,
                                              analyzer_options, type_factory_));
  }

  ZETASQL_ASSIGN_OR_RETURN(auto params,
                   ExtractParameters(query, analyzer_output.get()));

  ZETASQL_ASSIGN_OR_RETURN(auto resolved_statement,
                   ExtractValidatedResolvedStatementAndOptions(
                       analyzer_output.get(), context));

  // Change stream queries are not directly executed via this generic ExecuteSql
  // function in query engine. If a change stream query reaches here, it is from
  // an incorrect API(only ExecuteStreamingSql is allowed).
  ChangeStreamQueryValidator validator{
      context.schema, start_time,
      absl::flat_hash_map<std::string, zetasql::Value>(params.begin(),
                                                         params.end())};
  ZETASQL_ASSIGN_OR_RETURN(auto is_change_stream,
                   validator.IsChangeStreamQuery(resolved_statement.get()));
  if (is_change_stream) {
    return error::ChangeStreamQueriesMustBeStreaming();
  }

  QueryResult result;
  if (!IsDMLStmt(analyzer_output->resolved_statement()->node_kind())) {
    ZETASQL_ASSIGN_OR_RETURN(
        auto cursor,
        EvaluateQuery(resolved_statement.get(), params, type_factory_,
                      &result.num_output_rows, query_mode,
                      GetTimeZone(function_catalog_.GetLatestSchema())));
    result.rows = std::move(cursor);
  } else {
    ZETASQL_RET_CHECK_NE(context.writer, nullptr);
    analyzer_options.set_prune_unused_columns(false);
    if (context.schema->dialect() ==
        database_api::DatabaseDialect::POSTGRESQL) {
      ZETASQL_ASSIGN_OR_RETURN(analyzer_output,
                       AnalyzePostgreSQL(query.sql, &catalog, analyzer_options,
                                         type_factory_, &function_catalog_));
    } else {
      ZETASQL_ASSIGN_OR_RETURN(
          analyzer_output,
          Analyze(query.sql, &catalog, analyzer_options, type_factory_));
    }
    ZETASQL_ASSIGN_OR_RETURN(resolved_statement,
                     ExtractValidatedResolvedStatementAndOptions(
                         analyzer_output.get(), context));

    // Only execute the SQL statement if the user did not request PLAN mode.
    if (query_mode != v1::ExecuteSqlRequest::PLAN) {
      bool is_insert_on_conflict_stmt =
          resolved_statement->Is<zetasql::ResolvedInsertStmt>() &&
          resolved_statement->GetAs<zetasql::ResolvedInsertStmt>()
                  ->on_conflict_clause() != nullptr;
      if (!is_insert_on_conflict_stmt) {
        ZETASQL_ASSIGN_OR_RETURN(
            auto execute_update_result,
            EvaluateUpdate(resolved_statement.get(), &catalog, params,
                           type_factory_, context.schema->dialect(),
                           context.schema,
                           GetTimeZone(function_catalog_.GetLatestSchema())));
        ZETASQL_RETURN_IF_ERROR(context.writer->Write(execute_update_result.mutation));
        result.modified_row_count = execute_update_result.modify_row_count;
        result.rows = std::move(execute_update_result.returning_row_cursor);
      } else {
        if (!EmulatorFeatureFlags::instance()
                 .flags()
                 .enable_insert_on_conflict_dml) {
          std::string error_message =
              (context.schema->dialect() ==
               database_api::DatabaseDialect::POSTGRESQL)
                  ? "This ON CONFLICT clause in INSERT"
                  : "ON CONFLICT clause in INSERT";
          return error::UnsupportedUpsertQueries(error_message);
        }
        ZETASQL_ASSIGN_OR_RETURN(result, ExecuteInsertOnConflictDml(
                                     query, resolved_statement.get(), params,
                                     catalog, analyzer_options, context));
      }
    } else {
      // Add the columns and types of the returning clause to the result.
      auto returning_clause = GetReturningClause(resolved_statement.get());
      if (returning_clause != nullptr) {
        std::vector<std::string> names;
        std::vector<const zetasql::Type*> types;
        for (auto& column : returning_clause->output_column_list()) {
          names.push_back(column->column().name());
          types.push_back(column->column().type());
        }
        std::vector<std::vector<zetasql::Value>> values;
        result.rows = std::make_unique<VectorsRowCursor>(names, types, values);
      }
    }
  }
  // Add both undeclared and declared parameters to the result.
  result.parameter_types = analyzer_output->undeclared_parameters();
  for (auto const& param : query.declared_params) {
    result.parameter_types.insert({param.first, param.second.type()});
  }
  result.elapsed_time = absl::Now() - start_time;
  return result;
}

const zetasql::ResolvedReturningClause* GetReturningClause(
    const zetasql::ResolvedStatement* resolved_statement) {
  const zetasql::ResolvedReturningClause* returning_clause;
  switch (resolved_statement->node_kind()) {
    case zetasql::RESOLVED_INSERT_STMT:
      returning_clause =
          resolved_statement->GetAs<zetasql::ResolvedInsertStmt>()
              ->returning();
      break;
    case zetasql::RESOLVED_UPDATE_STMT:
      returning_clause =
          resolved_statement->GetAs<zetasql::ResolvedUpdateStmt>()
              ->returning();
      break;
    case zetasql::RESOLVED_DELETE_STMT:
      returning_clause =
          resolved_statement->GetAs<zetasql::ResolvedDeleteStmt>()
              ->returning();
      break;
    default:
      returning_clause = nullptr;
  }
  return returning_clause;
}

absl::Status QueryEngine::IsPartitionable(const Query& query,
                                          const QueryContext& context) const {
  ZETASQL_ASSIGN_OR_RETURN(auto analyzer_options,
                   MakeAnalyzerOptionsWithParameters(
                       query.declared_params,
                       GetTimeZone(function_catalog_.GetLatestSchema())));
  analyzer_options.set_prune_unused_columns(true);
  Catalog catalog{context.schema, &function_catalog_, type_factory_,
                  analyzer_options};

  std::unique_ptr<const zetasql::AnalyzerOutput> analyzer_output;
  if (context.schema->dialect() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSIGN_OR_RETURN(analyzer_output,
                     AnalyzePostgreSQL(query.sql, &catalog, analyzer_options,
                                       type_factory_, &function_catalog_));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(analyzer_output, Analyze(query.sql, &catalog,
                                              analyzer_options, type_factory_));
  }

  QueryEngineOptions options;
  ZETASQL_ASSIGN_OR_RETURN(auto resolved_statement,
                   ExtractValidatedResolvedStatementAndOptions(
                       analyzer_output.get(), context,
                       /*in_partition_query=*/true, &options));
  if (options.disable_query_partitionability_check) {
    return absl::OkStatus();
  }

  // Perform partitionability checks on the query
  PartitionabilityValidator part_validator{context.schema};
  return resolved_statement->Accept(&part_validator);
}

absl::Status QueryEngine::IsValidPartitionedDML(
    const Query& query, const QueryContext& context) const {
  if (!IsDMLQuery(query.sql)) {
    return error::InvalidOperationUsingPartitionedDmlTransaction();
  }

  ZETASQL_ASSIGN_OR_RETURN(auto analyzer_options,
                   MakeAnalyzerOptionsWithParameters(
                       query.declared_params,
                       GetTimeZone(function_catalog_.GetLatestSchema())));
  analyzer_options.set_prune_unused_columns(true);
  Catalog catalog{context.schema, &function_catalog_, type_factory_,
                  analyzer_options};
  ZETASQL_ASSIGN_OR_RETURN(
      auto analyzer_output,
      Analyze(query.sql, &catalog, analyzer_options, type_factory_));

  ZETASQL_ASSIGN_OR_RETURN(auto resolved_statement,
                   ExtractValidatedResolvedStatementAndOptions(
                       analyzer_output.get(), context,
                       /*in_partition_query=*/true));

  // Check that the DML statement is partitionable.
  PartitionedDMLValidator validator;
  return resolved_statement->Accept(&validator);
}

bool IsDMLQuery(const std::string& query) {
  zetasql::ResolvedNodeKind query_kind = zetasql::GetStatementKind(query);
  return IsDMLStmt(query_kind);
}

absl::StatusOr<ChangeStreamQueryValidator::ChangeStreamMetadata>
QueryEngine::TryGetChangeStreamMetadata(const Query& query,
                                        const Schema* schema,
                                        bool in_read_write_txn) {
  const absl::Time start_time = absl::Now();
  ZETASQL_ASSIGN_OR_RETURN(auto analyzer_options,
                   MakeAnalyzerOptionsWithParameters(query.declared_params,
                                                     GetTimeZone(schema)));
  analyzer_options.set_prune_unused_columns(true);
  zetasql::TypeFactory type_factory;
  FunctionCatalog function_catalog(
      &type_factory, kCloudSpannerEmulatorFunctionCatalogName, schema);
  Catalog catalog{schema, &function_catalog, &type_factory, analyzer_options};
  std::unique_ptr<const zetasql::AnalyzerOutput> analyzer_output;
  if (schema->dialect() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSIGN_OR_RETURN(analyzer_output,
                     AnalyzePostgreSQL(query.sql, &catalog, analyzer_options,
                                       &type_factory, &function_catalog));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(analyzer_output, Analyze(query.sql, &catalog,
                                              analyzer_options, &type_factory));
  }

  ZETASQL_ASSIGN_OR_RETURN(auto params,
                   ExtractParameters(query, analyzer_output.get()));

  ZETASQL_ASSIGN_OR_RETURN(
      auto resolved_statement,
      ExtractValidatedResolvedStatementAndOptions(
          analyzer_output.get(),
          QueryContext{.schema = schema,
                       .allow_read_write_only_functions = in_read_write_txn}));

  ChangeStreamQueryValidator validator{
      schema, start_time,
      absl::flat_hash_map<std::string, zetasql::Value>(params.begin(),
                                                         params.end())};
  ZETASQL_ASSIGN_OR_RETURN(auto is_change_stream,
                   validator.IsChangeStreamQuery(resolved_statement.get()));

  if (is_change_stream) {
    ZETASQL_RETURN_IF_ERROR(resolved_statement->Accept(&validator));
  }
  return validator.change_stream_metadata();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
