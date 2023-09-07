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

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/substitute.h"
#include "backend/access/write.h"
#include "backend/actions/ops.h"
#include "backend/common/graph_dependency_helper.h"
#include "backend/query/analyzer_options.h"
#include "common/errors.h"
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
    if (column->is_generated() || column->has_default_value()) {
      ZETASQL_RETURN_IF_ERROR(sorter.AddNodeIfNotExists(column));
    }
  }
  for (const Column* column : table->columns()) {
    if (column->is_generated()) {
      for (const Column* dep : column->dependent_columns()) {
        if (dep->is_generated() || dep->has_default_value()) {
          ZETASQL_RETURN_IF_ERROR(
              sorter.AddEdgeIfNotExists(column->Name(), dep->Name()));
        }
      }
    }
  }
  return sorter.TopologicalOrder(generated_columns);
}

absl::StatusOr<std::unique_ptr<zetasql::PreparedExpression>>
PrepareExpression(const Column* generated_column,
                  zetasql::Catalog* function_catalog) {
  constexpr char kExpression[] = "CAST (($0) AS $1)";
  std::string sql = absl::Substitute(
      kExpression, generated_column->expression().value(),
      generated_column->GetType()->TypeName(zetasql::PRODUCT_EXTERNAL));
  auto expr = std::make_unique<zetasql::PreparedExpression>(sql);
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
    const Table* table, zetasql::Catalog* function_catalog, bool for_keys)
    : table_(table), for_keys_(for_keys) {
  absl::Status s = Initialize(function_catalog);
  ABSL_DCHECK(s.ok()) << "Failed to initialize GeneratedColumnEffector: " << s;
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

absl::StatusOr<zetasql::Value>
GeneratedColumnEffector::ComputeGeneratedColumnValue(
    const Column* generated_column,
    const zetasql::ParameterValueMap& row_column_values) const {
  ZETASQL_RET_CHECK(generated_column != nullptr &&
            (generated_column->is_generated() ||
             generated_column->has_default_value()));
  ZETASQL_ASSIGN_OR_RETURN(
      zetasql::Value value,
      expressions_.at(generated_column)->Execute(row_column_values));
  if (value.is_null() && !generated_column->is_nullable()) {
    return error::NullValueForNotNullColumn(table_->Name(),
                                            generated_column->FullName());
  }

  return value;
}

absl::Status GeneratedColumnEffector::Effect(
    const MutationOp& op,
    std::vector<std::vector<zetasql::Value>>* generated_values,
    std::vector<const Column*>* columns_with_generated_values) const {
  ZETASQL_RET_CHECK(for_keys_ == true);

  columns_with_generated_values->reserve(generated_columns_.size());

  // This vector stores column values for each row that can be used to evaluate
  // generated columns.
  std::vector<zetasql::ParameterValueMap> row_column_values(
      op.rows.size(), zetasql::ParameterValueMap());
  // Evaluate generated columns in topological order.
  for (int i = 0; i < generated_columns_.size(); ++i) {
    const Column* generated_column = generated_columns_[i];
    if (table_->FindKeyColumn(generated_column->Name()) == nullptr) {
      // skip non-key columns.
      continue;
    }
    auto column_supplied_itr = std::find(op.columns.begin(), op.columns.end(),
                                         generated_column->Name());
    bool is_user_supplied_value = column_supplied_itr != op.columns.end();

    if (generated_column->has_default_value()) {
      // If this column has a default value and the user is supplying a value
      // for it, then we don't need to compute its default value.
      if (is_user_supplied_value) {
        continue;
      }
      if (op.type == MutationOpType::kUpdate ||
          op.type == MutationOpType::kDelete) {
        return error::DefaultPKNeedsExplicitValue(generated_column->FullName(),
                                                  "Update/Delete");
      }
    }

    for (int i = 0; i < op.rows.size(); ++i) {
      // Calculate values of generated columns for each row.
      for (int j = 0; j < op.columns.size(); ++j) {
        row_column_values[i][op.columns[j]] = op.rows[i][j];
      }
      ZETASQL_ASSIGN_OR_RETURN(
          zetasql::Value value,
          ComputeGeneratedColumnValue(generated_column, row_column_values[i]));
      // Update row_column_values so that other dependent columns on this
      // generated column can use the value.
      row_column_values[i][generated_column->Name()] = value;
      generated_values->at(i).push_back(value);
    }
    columns_with_generated_values->push_back(generated_column);
  }
  return absl::OkStatus();
}

absl::Status GeneratedColumnEffector::Effect(const ActionContext* ctx,
                                             const InsertOp& op) const {
  zetasql::ParameterValueMap column_values;
  ZETASQL_RET_CHECK_EQ(op.columns.size(), op.values.size());

  for (int i = 0; i < op.columns.size(); ++i) {
    column_values[op.columns[i]->Name()] = op.values[i];
  }

  for (const Column* column : table_->columns()) {
    // If the column doesn't appear in the list and doesn't have a default
    // value, we fill in Null value for it, so it can be used to compute
    // generated column values.
    // Columns with default values will be computed the same way as generated
    // columns.
    if (column_values.find(column->Name()) == column_values.end() &&
        !column->has_default_value()) {
      column_values[column->Name()] = zetasql::Value::Null(column->GetType());
    }
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

  std::vector<const Column*> columns_with_generated_values;
  columns_with_generated_values.reserve(generated_columns_.size());

  // Evaluate generated columns in topological order.
  for (int i = 0; i < generated_columns_.size(); ++i) {
    const Column* generated_column = generated_columns_[i];
    // If this column has a default value and the user is supplying a value
    // for it, then we don't need to compute its default value.
    if (generated_column->has_default_value() &&
        column_values->find(generated_column->Name()) != column_values->end()) {
      continue;
    }

    ZETASQL_ASSIGN_OR_RETURN(
        zetasql::Value value,
        ComputeGeneratedColumnValue(generated_column, *column_values));

    // Update the column value so that it can be used to evaluate other
    // generated columns that depend on it.
    (*column_values)[generated_column->Name()] = value;
    generated_values.push_back(value);
    columns_with_generated_values.push_back(generated_column);
  }

  if (!columns_with_generated_values.empty()) {
    ctx->effects()->Update(table_, key, columns_with_generated_values,
                           generated_values);
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
