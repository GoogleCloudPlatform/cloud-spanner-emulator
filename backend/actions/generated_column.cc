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

#include "backend/actions/generated_column.h"

#include <string>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/substitute.h"
#include "backend/actions/ops.h"
#include "backend/common/graph_dependency_helper.h"
#include "backend/query/analyzer_options.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {
absl::string_view GetColumnName(const Column* const& column) {
  return column->Name();
}

absl::Status GetGeneratedColumnsInTopologicalOrder(
    const Table* table, std::vector<const Column*>* generated_columns) {
  GraphDependencyHelper<const Column*, GetColumnName> sorter(
      /*object_type=*/"generated column");
  for (const Column* column : table->columns()) {
    if (column->is_generated()) {
      ZETASQL_RETURN_IF_ERROR(sorter.AddNodeIfNotExists(column));
    }
  }
  for (const Column* column : table->columns()) {
    if (column->is_generated()) {
      for (const Column* dep : column->dependent_columns()) {
        if (dep->is_generated()) {
          ZETASQL_RETURN_IF_ERROR(
              sorter.AddEdgeIfNotExists(column->Name(), dep->Name()));
        }
      }
    }
  }
  return sorter.TopologicalOrder(generated_columns);
}

zetasql_base::StatusOr<std::unique_ptr<zetasql::PreparedExpression>>
PrepareExpression(const Column* generated_column,
                  zetasql::Catalog* function_catalog) {
  constexpr char kExpression[] = "CAST (($0) AS $1)";
  std::string sql = absl::Substitute(
      kExpression, generated_column->expression().value(),
      generated_column->GetType()->TypeName(zetasql::PRODUCT_EXTERNAL));
  auto expr = absl::make_unique<zetasql::PreparedExpression>(sql);
  zetasql::AnalyzerOptions options = MakeGoogleSqlAnalyzerOptions();
  for (const Column* dep : generated_column->dependent_columns()) {
    ZETASQL_RETURN_IF_ERROR(options.AddExpressionColumn(dep->Name(), dep->GetType()));
  }
  ZETASQL_RETURN_IF_ERROR(expr->Prepare(options, function_catalog));
  ZETASQL_RET_CHECK(generated_column->GetType()->Equals(expr->output_type()));
  return std::move(expr);
}

}  // namespace

GeneratedColumnEffector::GeneratedColumnEffector(
    const Table* table, zetasql::Catalog* function_catalog)
    : table_(table) {
  absl::Status s = Initialize(function_catalog);
  ZETASQL_DCHECK(s.ok()) << "Failed to initialize GeneratedColumnEffector: " << s;
}

absl::Status GeneratedColumnEffector::Initialize(
    zetasql::Catalog* function_catalog) {
  ZETASQL_RETURN_IF_ERROR(
      GetGeneratedColumnsInTopologicalOrder(table_, &generated_columns_));
  expressions_.reserve(generated_columns_.size());
  for (const Column* generated_column : generated_columns_) {
    ZETASQL_ASSIGN_OR_RETURN(auto expr,
                     PrepareExpression(generated_column, function_catalog));
    expressions_[generated_column] = std::move(expr);
  }
  return absl::OkStatus();
}

zetasql_base::StatusOr<zetasql::Value>
GeneratedColumnEffector::ComputeGeneratedColumnValue(
    const Column* generated_column,
    const zetasql::ParameterValueMap& row_column_values) const {
  ZETASQL_RET_CHECK(generated_column != nullptr && generated_column->is_generated());
  ZETASQL_ASSIGN_OR_RETURN(
      zetasql::Value value,
      expressions_.at(generated_column)->Execute(row_column_values));
  if (value.is_null() && !generated_column->is_nullable()) {
    return error::NullValueForNotNullColumn(table_->Name(),
                                            generated_column->FullName());
  }

  return value;
}

absl::Status GeneratedColumnEffector::Effect(const ActionContext* ctx,
                                             const InsertOp& op) const {
  zetasql::ParameterValueMap column_values;
  ZETASQL_RET_CHECK_EQ(op.columns.size(), op.values.size());
  for (const Column* column : table_->columns()) {
    column_values[column->Name()] = zetasql::Value::Null(column->GetType());
  }
  for (int i = 0; i < op.columns.size(); ++i) {
    column_values[op.columns[i]->Name()] = op.values[i];
  }
  return Effect(ctx, op.key, &column_values);
}

absl::Status GeneratedColumnEffector::Effect(const ActionContext* ctx,
                                             const UpdateOp& op) const {
  if (!op.columns.empty() && op.columns.at(0)->is_generated()) {
    // This is a generated column effect. Don't process it again.
    return absl::OkStatus();
  }

  zetasql::ParameterValueMap column_values;
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<StorageIterator> itr,
      ctx->store()->Read(table_, KeyRange::Point(op.key), table_->columns()));
  ZETASQL_RET_CHECK(itr->Next());
  ZETASQL_RETURN_IF_ERROR(itr->Status());
  ZETASQL_RET_CHECK_EQ(op.columns.size(), op.values.size());
  for (int i = 0; i < itr->NumColumns(); ++i) {
    column_values[table_->columns().at(i)->Name()] = itr->ColumnValue(i);
  }
  for (int i = 0; i < op.columns.size(); ++i) {
    column_values[op.columns[i]->Name()] = op.values[i];
  }
  return Effect(ctx, op.key, &column_values);
}

absl::Status GeneratedColumnEffector::Effect(
    const ActionContext* ctx, const Key& key,
    zetasql::ParameterValueMap* column_values) const {
  std::vector<zetasql::Value> generated_values;
  generated_values.reserve(generated_columns_.size());

  // Evaluate generated columns in topological order.
  for (int i = 0; i < generated_columns_.size(); ++i) {
    const Column* generated_column = generated_columns_[i];
    ZETASQL_ASSIGN_OR_RETURN(
        zetasql::Value value,
        ComputeGeneratedColumnValue(generated_column, *column_values));

    // Update the column value so that it can be used to evaluate other
    // generated columns that depend on it.
    (*column_values)[generated_column->Name()] = value;
    generated_values.push_back(value);
  }

  ctx->effects()->Update(table_, key, generated_columns_, generated_values);
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
