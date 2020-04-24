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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_helpers.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/memory/memory.h"
#include "zetasql/base/status.h"
#include "absl/strings/match.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/common/case.h"
#include "backend/datamodel/value.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/hint_rewriter.h"
#include "backend/query/hint_validator.h"
#include "common/constants.h"
#include "common/errors.h"
#include "frontend/converters/values.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

bool IsDMLQuery(const std::string& query) {
  zetasql::ResolvedNodeKind query_kind = zetasql::GetStatementKind(query);
  return query_kind == zetasql::RESOLVED_INSERT_STMT ||
         query_kind == zetasql::RESOLVED_UPDATE_STMT ||
         query_kind == zetasql::RESOLVED_DELETE_STMT;
}

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

  zetasql_base::Status Status() const override { return zetasql_base::OkStatus(); }

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
    zetasql::TypeFactory* type_factory) {
  zetasql::EvaluatorOptions options;
  options.type_factory = type_factory;
  options.scramble_undefined_orderings = true;
  return options;
}

// Uses googlesql/public/analyzer to build an AnalyzerOutput for an query.
zetasql_base::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>> Analyze(
    const std::string& sql, const zetasql::ParameterValueMap& params,
    zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory) {
  zetasql::AnalyzerOptions options = MakeGoogleSqlAnalyzerOptions();
  for (const auto& [name, value] : params) {
    ZETASQL_RETURN_IF_ERROR(options.AddQueryParameter(name, value.type()));
  }
  std::unique_ptr<const zetasql::AnalyzerOutput> output;
  ZETASQL_RETURN_IF_ERROR(zetasql::AnalyzeStatement(sql, options, catalog,
                                              type_factory, &output));
  return output;
}

// TODO : Replace with a better error transforming mechanism,
// ideally hooking into ZetaSQL to control error messages.
zetasql_base::Status MaybeTransformZetaSQLDMLError(zetasql_base::Status error) {
  // For inserts to a primary key columm.
  if (absl::StartsWith(error.message(),
                       "Failed to insert row with primary key")) {
    return zetasql_base::Status(zetasql_base::StatusCode::kAlreadyExists, error.message());
  }

  // For updates to a primary key column.
  if (absl::StartsWith(error.message(), "Cannot modify a primary key column")) {
    return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument, error.message());
  }

  return error;
}

// Builds a INSERT mutation and returns it along with a count of inserted rows.
std::pair<Mutation, int64_t> BuildInsert(
    std::unique_ptr<zetasql::EvaluatorTableModifyIterator> iterator,
    MutationOpType op_type,
    const CaseInsensitiveStringSet& pending_ts_columns) {
  const zetasql::Table* table = iterator->table();
  std::vector<std::string> column_names;
  column_names.reserve(table->NumColumns());
  for (int i = 0; i < table->NumColumns(); ++i) {
    column_names.push_back(table->GetColumn(i)->Name());
  }

  std::vector<ValueList> values;
  while (iterator->NextRow()) {
    values.emplace_back();
    for (int i = 0; i < table->NumColumns(); ++i) {
      if (pending_ts_columns.find(column_names[i]) !=
          pending_ts_columns.end()) {
        values.back().push_back(
            zetasql::Value::StringValue(kCommitTimestampIdentifier));
      } else {
        values.back().push_back(iterator->GetColumnValue(i));
      }
    }
  }

  Mutation mutation;
  mutation.AddWriteOp(op_type, table->Name(), column_names, values);
  return std::make_pair(mutation, values.size());
}

// Builds a UPDATE mutation and returns it along with a count of updated rows.
std::pair<Mutation, int64_t> BuildUpdate(
    std::unique_ptr<zetasql::EvaluatorTableModifyIterator> iterator,
    MutationOpType op_type,
    const CaseInsensitiveStringSet& pending_ts_columns) {
  const zetasql::Table* table = iterator->table();
  std::vector<std::string> column_names;
  column_names.reserve(table->NumColumns());
  for (int i = 0; i < table->NumColumns(); ++i) {
    column_names.push_back(table->GetColumn(i)->Name());
  }

  std::vector<ValueList> values;
  while (iterator->NextRow()) {
    values.emplace_back();
    for (int i = 0; i < table->NumColumns(); ++i) {
      if (pending_ts_columns.find(column_names[i]) !=
          pending_ts_columns.end()) {
        values.back().push_back(
            zetasql::Value::StringValue(kCommitTimestampIdentifier));
      } else {
        values.back().push_back(iterator->GetColumnValue(i));
      }
    }
  }

  Mutation mutation;
  mutation.AddWriteOp(op_type, table->Name(), column_names, values);
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
  mutation.AddDeleteOp(table->Name(), key_set);
  return std::make_pair(mutation, key_set.keys().size());
}

// Returns true if the ResolvedDMLValue is a call to PENDING_COMMIT_TIMESTAMP()
bool IsPendingCommitTimestamp(const zetasql::ResolvedDMLValue& dml_value) {
  if (dml_value.value()->node_kind() == zetasql::RESOLVED_FUNCTION_CALL) {
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

zetasql_base::StatusOr<CaseInsensitiveStringSet> PendingCommitTimestampColumnsInInsert(
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

zetasql_base::StatusOr<CaseInsensitiveStringSet> PendingCommitTimestampColumnsInUpdate(
    const std::vector<std::unique_ptr<const zetasql::ResolvedUpdateItem>>&
        update_item_list) {
  CaseInsensitiveStringSet pending_ts_columns;
  for (const auto& update_item : update_item_list) {
    if (IsPendingCommitTimestamp(*update_item->set_value())) {
      std::string column_name = update_item->target()
                                    ->GetAs<zetasql::ResolvedColumnRef>()
                                    ->column()
                                    .name();
      pending_ts_columns.insert(column_name);
    }
  }
  return pending_ts_columns;
}

zetasql_base::StatusOr<std::pair<Mutation, int64_t>> EvaluateResolvedInsert(
    const zetasql::ResolvedInsertStmt* insert_statement,
    const zetasql::ParameterValueMap& parameters,
    zetasql::TypeFactory* type_factory) {
  ZETASQL_ASSIGN_OR_RETURN(auto pending_ts_columns,
                   PendingCommitTimestampColumnsInInsert(
                       insert_statement->insert_column_list(),
                       insert_statement->row_list()));

  auto prepared_insert = absl::make_unique<zetasql::PreparedModify>(
      insert_statement, CommonEvaluatorOptions(type_factory));
  auto status_or = prepared_insert->Execute(parameters);
  if (!status_or.ok()) {
    return MaybeTransformZetaSQLDMLError(status_or.status());
  }
  auto iterator = std::move(status_or).ValueOrDie();
  return BuildInsert(std::move(iterator), MutationOpType::kInsert,
                     pending_ts_columns);
}

zetasql_base::StatusOr<std::pair<Mutation, int64_t>> EvaluateResolvedUpdate(
    const zetasql::ResolvedUpdateStmt* update_statement,
    const zetasql::ParameterValueMap& parameters,
    zetasql::TypeFactory* type_factory) {
  ZETASQL_ASSIGN_OR_RETURN(auto pending_ts_columns,
                   PendingCommitTimestampColumnsInUpdate(
                       update_statement->update_item_list()));
  auto prepared_update = absl::make_unique<zetasql::PreparedModify>(
      update_statement, CommonEvaluatorOptions(type_factory));
  auto status_or = prepared_update->Execute(parameters);
  if (!status_or.ok()) {
    return MaybeTransformZetaSQLDMLError(status_or.status());
  }
  auto iterator = std::move(status_or).ValueOrDie();
  return BuildUpdate(std::move(iterator), MutationOpType::kUpdate,
                     pending_ts_columns);
}

zetasql_base::StatusOr<std::pair<Mutation, int64_t>> EvaluateResolvedDelete(
    const zetasql::ResolvedDeleteStmt* delete_statement,
    const zetasql::ParameterValueMap& parameters,
    zetasql::TypeFactory* type_factory) {
  auto prepared_delete = absl::make_unique<zetasql::PreparedModify>(
      delete_statement, CommonEvaluatorOptions(type_factory));
  ZETASQL_ASSIGN_OR_RETURN(auto iterator, prepared_delete->Execute(parameters));

  return BuildDelete(std::move(iterator));
}

// Uses googlesql/public/evaluator to evaluate a DML statement represented by a
// resolved AST and returns a pair of mutation and count of modified rows.
zetasql_base::StatusOr<std::pair<Mutation, int64_t>> EvaluateUpdate(
    const zetasql::ResolvedStatement* resolved_statement,
    const zetasql::ParameterValueMap& parameters,
    zetasql::TypeFactory* type_factory) {
  switch (resolved_statement->node_kind()) {
    case zetasql::RESOLVED_INSERT_STMT:
      return EvaluateResolvedInsert(
          resolved_statement->GetAs<zetasql::ResolvedInsertStmt>(),
          parameters, type_factory);
    case zetasql::RESOLVED_UPDATE_STMT:
      return EvaluateResolvedUpdate(
          resolved_statement->GetAs<zetasql::ResolvedUpdateStmt>(),
          parameters, type_factory);
    case zetasql::RESOLVED_DELETE_STMT:
      return EvaluateResolvedDelete(
          resolved_statement->GetAs<zetasql::ResolvedDeleteStmt>(),
          parameters, type_factory);
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unsupported support node kind "
                       << ResolvedNodeKind_Name(
                              resolved_statement->node_kind());
      break;
  }
}

// Uses googlesql/public/evaluator to evaluate a query statement represented by
// a resolved AST and returns a row cursor.
zetasql_base::StatusOr<std::unique_ptr<RowCursor>> EvaluateQuery(
    const zetasql::ResolvedStatement* resolved_statement,
    const zetasql::ParameterValueMap& params,
    zetasql::TypeFactory* type_factory, int64_t* num_output_rows) {
  ZETASQL_RET_CHECK_EQ(resolved_statement->node_kind(), zetasql::RESOLVED_QUERY_STMT)
      << "input is not a query statement";
  auto prepared_query = absl::make_unique<zetasql::PreparedQuery>(
      resolved_statement->GetAs<zetasql::ResolvedQueryStmt>(),
      CommonEvaluatorOptions(type_factory));
  ZETASQL_ASSIGN_OR_RETURN(auto iterator, prepared_query->Execute(params));

  std::vector<std::vector<zetasql::Value>> values;
  while (iterator->NextRow()) {
    values.emplace_back();
    values.back().reserve(iterator->NumColumns());
    for (int i = 0; i < iterator->NumColumns(); ++i) {
      values.back().push_back(iterator->GetValue(i));
    }
  }
  ZETASQL_RETURN_IF_ERROR(iterator->Status());
  *num_output_rows = values.size();
  std::vector<std::string> names;
  std::vector<const zetasql::Type*> types;
  for (int i = 0; i < iterator->NumColumns(); ++i) {
    names.push_back(iterator->GetColumnName(i));
    types.push_back(iterator->GetColumnType(i));
  }
  return absl::make_unique<VectorsRowCursor>(names, types, values);
}

zetasql_base::StatusOr<std::unique_ptr<zetasql::ResolvedStatement>>
ExtractValidatdResolvedStatement(
    const zetasql::AnalyzerOutput* analyzer_output, const Schema* schema) {
  ZETASQL_RET_CHECK_NE(analyzer_output->resolved_statement(), nullptr);
  HintRewriter rewriter;
  ZETASQL_RETURN_IF_ERROR(analyzer_output->resolved_statement()->Accept(&rewriter));
  ZETASQL_ASSIGN_OR_RETURN(auto statement,
                   rewriter.ConsumeRootNode<zetasql::ResolvedStatement>());

  HintValidator validator{schema};
  ZETASQL_RETURN_IF_ERROR(statement->Accept(&validator));
  return statement;
}

}  // namespace

zetasql_base::StatusOr<QueryResult> QueryEngine::ExecuteSql(
    const Query& query, const QueryContext& context) const {
  absl::Time start_time = absl::Now();
  Catalog catalog{context.schema, &function_catalog_, context.reader};
  ZETASQL_ASSIGN_OR_RETURN(
      auto analyzer_output,
      Analyze(query.sql, query.declared_params, &catalog, type_factory_));

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
    if (type->IsTimestamp() || type->IsDate()) {
      return error::UnableToInferUndeclaredParameter(name);
    }

    auto it = undeclared_params.find(name);

    // ZetaSQL will return an undeclared parameter error for any parameters that
    // do not have values specified (ie, when it == end()).
    if (it != undeclared_params.end()) {
      auto parsed_value = frontend::ValueFromProto(*it->second, type);

      // If the value does not parse as the given type, the error code is
      // kInvalidArgument, not kFailedPrecondition, for example.
      if (!parsed_value.ok()) {
        return zetasql_base::Status(zetasql_base::StatusCode::kInvalidArgument,
                            parsed_value.status().message());
      }

      params[name] = parsed_value.value();
    }
  }

  ZETASQL_ASSIGN_OR_RETURN(
      auto resolved_statement,
      ExtractValidatdResolvedStatement(analyzer_output.get(), context.schema));

  QueryResult result;
  if (analyzer_output->resolved_statement()->node_kind() ==
      zetasql::RESOLVED_QUERY_STMT) {
    ZETASQL_ASSIGN_OR_RETURN(auto cursor,
                     EvaluateQuery(resolved_statement.get(), params,
                                   type_factory_, &result.num_output_rows));
    result.rows = std::move(cursor);
  } else {
    ZETASQL_RET_CHECK_NE(context.writer, nullptr);
    ZETASQL_ASSIGN_OR_RETURN(
        const auto& mutation_and_count,
        EvaluateUpdate(resolved_statement.get(), params, type_factory_));
    ZETASQL_RETURN_IF_ERROR(context.writer->Write(mutation_and_count.first));
    result.modified_row_count = static_cast<int64_t>(mutation_and_count.second);
  }

  result.elapsed_time = absl::Now() - start_time;
  return result;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
