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

#include "backend/actions/evaluated_column.h"

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "backend/access/write.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/common/graph_dependency_helper.h"
#include "backend/common/ids.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/query/analyzer_options.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/storage/iterator.h"
#include "common/constants.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {
absl::string_view GetColumnName(const Column* const& column) {
  return column->Name();
}

bool IsKeyColumn(const Column* column) {
  return column->table()->FindKeyColumn(column->Name()) != nullptr;
}

absl::Status GetEvaluatedColumnsInTopologicalOrder(
    const Table* table, std::vector<const Column*>* evaluated_columns) {
  GraphDependencyHelper<const Column*, GetColumnName> sorter(
      /*object_type=*/"evaluated column");
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
  return sorter.TopologicalOrder(evaluated_columns);
}

// Check if any dependent column is present in the user supplied columns for the
// generated key column.
bool IsAnyDependentColumnPresent(
    const Column* generated_column,
    std::vector<std::string> user_supplied_columns) {
  ABSL_DCHECK(generated_column->is_generated());
  for (const auto& dep_col : generated_column->dependent_columns()) {
    if (dep_col->is_generated() && IsKeyColumn(dep_col)) {
      return IsAnyDependentColumnPresent(dep_col, user_supplied_columns);
    }
    if (std::find(user_supplied_columns.begin(), user_supplied_columns.end(),
                  dep_col->Name()) != user_supplied_columns.end()) {
      return true;
    }
  }
  return false;
}

absl::StatusOr<std::unique_ptr<zetasql::PreparedExpression>>
PrepareExpression(const Column* evaluated_column,
                  const zetasql::AnalyzerOptions& analyzer_options,
                  zetasql::Catalog* function_catalog) {
  constexpr char kExpression[] = "CAST (($0) AS $1)";
  std::string sql = absl::Substitute(
      kExpression, evaluated_column->expression().value(),
      evaluated_column->GetType()->TypeName(zetasql::PRODUCT_EXTERNAL,
                                            /*use_external_float32=*/true));
  auto expr = std::make_unique<zetasql::PreparedExpression>(sql);
  zetasql::AnalyzerOptions options = analyzer_options;
  for (const Column* dep : evaluated_column->dependent_columns()) {
    ZETASQL_RETURN_IF_ERROR(options.AddExpressionColumn(dep->Name(), dep->GetType()));
  }
  ZETASQL_RETURN_IF_ERROR(expr->Prepare(options, function_catalog));
  ZETASQL_RET_CHECK(evaluated_column->GetType()->Equals(expr->output_type()));
  return std::move(expr);
}

}  // namespace

EvaluatedColumnEffector::EvaluatedColumnEffector(
    const Table* table, const zetasql::AnalyzerOptions& analyzer_options,
    zetasql::Catalog* function_catalog, bool for_keys)
    : table_(table), for_keys_(for_keys) {
  absl::Status s = Initialize(analyzer_options, function_catalog);
  ABSL_DCHECK(s.ok()) << "Failed to initialize EvaluatedColumnEffector: " << s;
}

absl::Status EvaluatedColumnEffector::Initialize(
    const zetasql::AnalyzerOptions& analyzer_options,
    zetasql::Catalog* function_catalog) {
  ZETASQL_RETURN_IF_ERROR(
      GetEvaluatedColumnsInTopologicalOrder(table_, &evaluated_columns_));
  absl::flat_hash_set<ColumnID> unique_dependent_column;
  expressions_.reserve(evaluated_columns_.size());
  for (const Column* evaluated_column : evaluated_columns_) {
    ZETASQL_ASSIGN_OR_RETURN(auto expr,
                     PrepareExpression(evaluated_column, analyzer_options,
                                       function_catalog));
    expressions_[evaluated_column] = std::move(expr);
    for (const Column* dep : evaluated_column->dependent_columns()) {
      if (unique_dependent_column.insert(dep->id()).second) {
        dependent_columns_.push_back(dep);
      }
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<zetasql::Value>
EvaluatedColumnEffector::ComputeEvaluatedColumnValue(
    const Column* evaluated_column,
    const zetasql::ParameterValueMap& row_column_values) const {
  ZETASQL_RET_CHECK(evaluated_column != nullptr &&
            (evaluated_column->is_generated() ||
             evaluated_column->has_default_value()));
  ZETASQL_ASSIGN_OR_RETURN(
      zetasql::Value value,
      expressions_.at(evaluated_column)->Execute(row_column_values));
  if (value.is_null() && !evaluated_column->is_nullable()) {
    return error::NullValueForNotNullColumn(table_->Name(),
                                            evaluated_column->FullName());
  }

  return value;
}

absl::Status EvaluatedColumnEffector::Effect(
    const MutationOp& op,
    std::vector<std::vector<zetasql::Value>>* evaluated_values,
    std::vector<const Column*>* columns_with_evaluated_values) const {
  ZETASQL_RET_CHECK(for_keys_ == true);

  columns_with_evaluated_values->reserve(evaluated_columns_.size());

  // This vector stores column values for each row that can be used to evaluate
  // evaluated columns.
  std::vector<zetasql::ParameterValueMap> row_column_values(
      op.rows.size(), zetasql::ParameterValueMap());
  // Evaluate evaluated columns in topological order.
  for (int i = 0; i < evaluated_columns_.size(); ++i) {
    const Column* evaluated_column = evaluated_columns_[i];
    if (!evaluated_column->has_default_value() &&
        !IsKeyColumn(evaluated_column)) {
      // skip non-key columns except default columns since generated key columns
      // may be depended by default columns values of which would need to be
      // evaluated.
      continue;
    }
    auto column_supplied_itr = std::find(op.columns.begin(), op.columns.end(),
                                         evaluated_column->Name());
    bool is_user_supplied_value = column_supplied_itr != op.columns.end();

    if (evaluated_column->has_default_value()) {
      // If this column has a default value and the user is supplying a value
      // for it, then we don't need to compute its default value.
      if (is_user_supplied_value) {
        continue;
      }
      if (IsKeyColumn(evaluated_column) &&
          (op.type == MutationOpType::kUpdate ||
           op.type == MutationOpType::kDelete)) {
        return error::DefaultPKNeedsExplicitValue(evaluated_column->FullName(),
                                                  "Update/Delete");
      }
    } else if (evaluated_column->is_generated() && is_user_supplied_value) {
      // If this column is generated column and user is supplying a value for it
      // and the user is not supplying values for dependent column values to
      // evaluate generated column value, we don't need to compute its generated
      // value.
      if (!IsAnyDependentColumnPresent(evaluated_column, op.columns)) {
        continue;
      }
      // Users should supply values for generated columns only in update
      // operations.
      if (op.type != MutationOpType::kUpdate) {
        return error::UserSuppliedValueInNonUpdateGpk(
            evaluated_column->FullName());
      }
    }

    for (int i = 0; i < op.rows.size(); ++i) {
      // Calculate values of evaluated columns for each row.
      for (int j = 0; j < op.columns.size(); ++j) {
        row_column_values[i][op.columns[j]] = op.rows[i][j];
      }
      ZETASQL_ASSIGN_OR_RETURN(
          zetasql::Value value,
          ComputeEvaluatedColumnValue(evaluated_column, row_column_values[i]));
      if (evaluated_column->is_generated() && is_user_supplied_value) {
        size_t index = column_supplied_itr - op.columns.begin();
        zetasql::Value provided_value = op.rows[i][index];
        if (provided_value != value) {
          return error::GeneratedPkModified(evaluated_column->FullName());
        }
      }
      // Update row_column_values so that other dependent columns on this
      // evaluated column can use the value.
      row_column_values[i][evaluated_column->Name()] = value;
      evaluated_values->at(i).push_back(value);
    }
    columns_with_evaluated_values->push_back(evaluated_column);
  }
  return absl::OkStatus();
}

absl::Status EvaluatedColumnEffector::Effect(const ActionContext* ctx,
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

  return Effect(ctx, op.key, &column_values, /*is_update_op=*/false,
                /*apply_on_update=*/false, /*origin_is_dml=*/op.origin_is_dml);
}

absl::Status EvaluatedColumnEffector::Effect(const ActionContext* ctx,
                                             const UpdateOp& op) const {
  bool apply_on_update = false;
  for (const Column* column : op.columns) {
    // If any non-key generated columns appear then this is a generated column
    // effect and we do not need to process it again. The non-key requirement is
    // needed because user-generated updates are expected to include key values,
    // including generated ones.
    if (!IsKeyColumn(column)) {
      if (column->is_generated()) {
        return absl::OkStatus();
      }
    }
  }

  zetasql::ParameterValueMap column_values;
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<StorageIterator> itr,
      ctx->store()->Read(table_, KeyRange::Point(op.key), dependent_columns_));
  ZETASQL_RET_CHECK(itr->Next());
  ZETASQL_RETURN_IF_ERROR(itr->Status());
  ZETASQL_RET_CHECK_EQ(op.columns.size(), op.values.size());
  ZETASQL_RET_CHECK_EQ(itr->NumColumns(), dependent_columns_.size());
  for (int i = 0; i < dependent_columns_.size(); ++i) {
    column_values[dependent_columns_[i]->Name()] = itr->ColumnValue(i);
  }
  for (int i = 0; i < op.columns.size(); ++i) {
    column_values[op.columns[i]->Name()] = op.values[i];
  }
  return Effect(ctx, op.key, &column_values, /*is_update_op=*/true,
                apply_on_update, /*origin_is_dml=*/op.origin_is_dml);
}

absl::Status EvaluatedColumnEffector::Effect(
    const ActionContext* ctx, const Key& key,
    zetasql::ParameterValueMap* column_values, bool is_update_op,
    bool apply_on_update, bool origin_is_dml) const {
  ZETASQL_RET_CHECK(for_keys_ == false);
  std::vector<zetasql::Value> evaluated_values;
  evaluated_values.reserve(evaluated_columns_.size());

  std::vector<const Column*> columns_with_evaluated_values;
  columns_with_evaluated_values.reserve(evaluated_columns_.size());

  // Evaluate evaluated columns in topological order.
  for (int i = 0; i < evaluated_columns_.size(); ++i) {
    const Column* evaluated_column = evaluated_columns_[i];
    // Keys are handled by a separate effector initialized with for_keys_=true.
    // Skipping these columns here is not simply an optimization; if we include
    // them then we may find ourselves in an infinite loop repeatedly generating
    // effects for generated PKs.
    if (IsKeyColumn(evaluated_column)) {
      continue;
    }

    // User provided values override computed values for DEFAULT.
    if (evaluated_column->has_default_value() &&
        column_values->find(evaluated_column->Name()) != column_values->end()) {
      continue;
    }

    // DEFAULT values only apply to inserts.
    if (evaluated_column->has_default_value() && is_update_op) {
      continue;
    }

    // For DML-originated mutations, default values have already been evaluated
    // by ZetaSQL and included in the mutation. Skip re-evaluation to avoid
    // timestamp differences between RETURNING clause and stored values.
    if (evaluated_column->has_default_value() && origin_is_dml) {
      continue;
    }

    ZETASQL_ASSIGN_OR_RETURN(
        zetasql::Value value,
        ComputeEvaluatedColumnValue(evaluated_column, *column_values));

    // Update the column value so that it can be used to evaluate other
    // evaluated columns that depend on it.
    (*column_values)[evaluated_column->Name()] = value;
    evaluated_values.push_back(value);
    columns_with_evaluated_values.push_back(evaluated_column);
  }

  if (!columns_with_evaluated_values.empty()) {
    ctx->effects()->Update(table_, key, columns_with_evaluated_values,
                           evaluated_values);
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
